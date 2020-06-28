//
// Eventum
//   Copyright (C) 2020 Toshiaki Takada
//
// Event machine
//  File Descriptor events
//  Timer events
//  Channel events
//

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use log::debug;
use mio::event::*;
use mio::*;
use quick_error::*;

pub const EVENT_MANAGER_TICK: u64 = 10;

quick_error! {
    #[derive(Debug)]
    pub enum EventError {
        RegistryError {
            description("Registry error")
            display(r#"Registry error"#)
        }
        ChannelQueueEmpty {
            description("Channel queue is empty")
            display(r#"Channel queue is empty"#)
        }
        SystemShutdown {
            description("System shutdown")
            display(r#"System shutdown"#)
        }
    }
}

/// Event types.
pub enum EventType {
    SimpleEvent,
    ReadEvent,
    WriteEvent,
    TimerEvent,
    ChannelEvent,
    ErrorEvent,
}

impl EventType {
    pub fn to_string(&self) -> &str {
        match *self {
            EventType::SimpleEvent => "Simple Event",
            EventType::ReadEvent => "Read Event",
            EventType::WriteEvent => "Write Event",
            EventType::TimerEvent => "Timer Event",
            EventType::ChannelEvent => "Channel Event",
            EventType::ErrorEvent => "Error Event",
        }
    }
}

impl fmt::Debug for EventType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// Event Handler trait.
/// Token is associated with EventHandler and certain event expected.
pub trait EventHandler
where
    Self: Send,
    Self: Sync,
{
    /// Handle event.
    fn handle(&self, event_type: EventType) -> Result<(), EventError>;

    /// Set token to event handler.
    fn set_token(&self, _token: Token) {
        // Placeholder
    }

    /// Get token from event handler.
    fn get_token(&self) -> Token {
        // Placeholder
        Token(0)
    }
}

/// Event Manager.
pub struct EventManager {
    /// Simple events poller.
    simple: RefCell<SimplePoller>,

    /// File descriptor (read/write) events.
    fdesc: RefCell<FdescPoller>,

    /// Timer events.
    timer: RefCell<TimerPoller>,

    /// Channel events.
    channel: RefCell<ChannelPoller>,
}

impl EventManager {
    /// Constructor.
    pub fn new() -> EventManager {
        EventManager {
            simple: RefCell::new(SimplePoller::new()),
            fdesc: RefCell::new(FdescPoller::new()),
            timer: RefCell::new(TimerPoller::new()),
            channel: RefCell::new(ChannelPoller::new()),
        }
    }

    /// Register a low priority event.
    pub fn register_low(&self, handler: Arc<dyn EventHandler>) {
        self.simple.borrow_mut().register_low(handler);
    }

    /// Register a high priority event.
    pub fn register_high(&self, handler: Arc<dyn EventHandler>) {
        self.simple.borrow_mut().register_high(handler);
    }

    /// Poll low priority events.
    pub fn poll_low(&self) -> Vec<(EventType, Arc<dyn EventHandler>)> {
        let mut simple = self.simple.borrow_mut();
        simple
            .low
            .drain(..)
            .map(|h| (EventType::SimpleEvent, h))
            .collect()
    }

    /// Poll high priority events.
    pub fn poll_high(&self) -> Vec<(EventType, Arc<dyn EventHandler>)> {
        let mut simple = self.simple.borrow_mut();
        simple
            .high
            .drain(..)
            .map(|h| (EventType::SimpleEvent, h))
            .collect()
    }

    /// Register listen socket.
    pub fn register_listen(
        &self,
        fd: &mut dyn Source,
        handler: Arc<dyn EventHandler>,
    ) -> Result<(), EventError> {
        let mut fdesc = self.fdesc.borrow_mut();
        let index = fdesc.index;
        let token = Token(index);

        fdesc.handlers.insert(token, handler);

        // TODO: consider index wrap around?
        fdesc.index += 1;

        if let Err(_) = fdesc
            .poll
            .registry()
            .register(fd, token, Interest::READABLE)
        {
            Err(EventError::RegistryError)
        } else {
            Ok(())
        }
    }

    /// Register read socket.
    pub fn register_read(
        &self,
        fd: &mut dyn Source,
        handler: Arc<dyn EventHandler>,
    ) -> Result<(), EventError> {
        let mut fdesc = self.fdesc.borrow_mut();
        let index = fdesc.index;
        let token = Token(index);

        handler.set_token(token);

        fdesc.handlers.insert(token, handler);

        // TODO: consider index wrap around?
        fdesc.index += 1;

        if let Err(_) = fdesc
            .poll
            .registry()
            .register(fd, token, Interest::READABLE)
        {
            Err(EventError::RegistryError)
        } else {
            Ok(())
        }
    }

    /// Register write socket.
    pub fn register_write(
        &self,
        fd: &mut dyn Source,
        handler: Arc<dyn EventHandler>,
    ) -> Result<(), EventError> {
        let mut fdesc = self.fdesc.borrow_mut();
        let index = fdesc.index;
        let token = Token(index);

        handler.set_token(token);

        fdesc.handlers.insert(token, handler);

        // TODO: consider index wrap around?
        fdesc.index += 1;

        if let Err(_) = fdesc
            .poll
            .registry()
            .register(fd, token, Interest::WRITABLE)
        {
            Err(EventError::RegistryError)
        } else {
            Ok(())
        }
    }

    /// Unregister read socket.
    pub fn unregister_read(&self, fd: &mut dyn Source, token: Token) {
        let mut fdesc = self.fdesc.borrow_mut();

        let _e = fdesc.handlers.remove(&token);
        fdesc.poll.registry().deregister(fd).unwrap();
    }

    /// Poll and return events ready to read or write.
    pub fn poll_get_events(&self) -> Events {
        let mut fdesc = self.fdesc.borrow_mut();
        let mut events = Events::with_capacity(1024);
        let timeout = fdesc.timeout;

        match fdesc.poll.poll(&mut events, Some(timeout)) {
            Ok(_) => {}
            Err(_) => {}
        }

        events
    }

    /// Return handler associated with token.
    pub fn poll_get_handler(&self, event: &Event) -> Option<Arc<dyn EventHandler>> {
        let fdesc = self.fdesc.borrow_mut();
        match fdesc.handlers.get(&event.token()) {
            Some(handler) => Some(handler.clone()),
            None => None,
        }
    }

    /// Poll FDs and handle events.
    pub fn poll_fd(&self) -> Vec<(EventType, Arc<dyn EventHandler>)> {
        let events = self.poll_get_events();
        let mut vec = Vec::new();

        for event in events.iter() {
            if let Some(handler) = self.poll_get_handler(event) {
                if event.is_readable() {
                    vec.push((EventType::ReadEvent, handler));
                } else if event.is_writable() {
                    vec.push((EventType::WriteEvent, handler));
                } else {
                    vec.push((EventType::ErrorEvent, handler));
                };
            }
        }

        vec
    }

    /// Register timer.
    pub fn register_timer(&self, d: Duration, handler: Arc<dyn EventHandler>) {
        let timers = self.timer.borrow();
        timers.register(d, handler);
    }

    /// Poll timers and handle events.
    pub fn poll_timer(&self) -> Vec<(EventType, Arc<dyn EventHandler>)> {
        let mut vec = Vec::new();

        let timer = self.timer.borrow_mut();
        while let Some(handler) = timer.poll() {
            vec.push((EventType::TimerEvent, handler));
        }

        vec
    }

    /// Register channel handler.
    pub fn register_channel(&self, handler: Box<dyn ChannelHandler>) {
        self.channel.borrow_mut().register_handler(handler);
    }

    /// Poll channel handlers.
    pub fn poll_channel(&self) -> Vec<(EventType, Arc<dyn EventHandler>)> {
        self.channel.borrow_mut().poll_channel()
    }

    /// Poll and collect all runnable events.
    /// To dispatch and do handling depends on a runner.
    pub fn poll(&self) -> Vec<(EventType, Arc<dyn EventHandler>)> {
        let mut vec = Vec::new();

        // High priority events.
        vec.append(&mut self.poll_high());

        // Process FD events.
        vec.append(&mut self.poll_fd());

        // Process channels.
        vec.append(&mut self.poll_channel());

        // Process timer.
        vec.append(&mut self.poll_timer());

        // Low priority events.
        vec.append(&mut self.poll_low());

        // Return events vector.
        vec
    }
}

/// Utility to blocking until fd gets readable.
pub fn wait_until_readable(fd: &mut dyn Source) -> Result<(), EventError> {
    let mut poll = Poll::new().unwrap();

    if let Err(_) = poll.registry().register(fd, Token(0), Interest::READABLE) {
        Err(EventError::RegistryError)
    } else {
        let mut events = Events::with_capacity(1024);
        let _ = poll.poll(&mut events, None);
        Ok(())
    }
}

/// Utility to blocking until fd gets writable.
pub fn wait_until_writable(fd: &mut dyn Source) -> Result<(), EventError> {
    let mut poll = Poll::new().unwrap();

    if let Err(_) = poll.registry().register(fd, Token(0), Interest::WRITABLE) {
        Err(EventError::RegistryError)
    } else {
        let mut events = Events::with_capacity(1024);
        let _ = poll.poll(&mut events, None);
        Ok(())
    }
}

/// Trait EventRunner.
pub trait EventRunner {
    /// Run events.
    fn run(&self, events: Vec<(EventType, Arc<dyn EventHandler>)>) -> Result<(), EventError>;
}

/// Default event runner
pub struct SimpleRunner {}

impl SimpleRunner {
    /// Constructor.
    pub fn new() -> SimpleRunner {
        SimpleRunner {}
    }

    /// Sleep certain period to have other events to occur.
    pub fn sleep(&self) {
        // TBD: we should sleep MIN(earlist timer, Tick).
        thread::sleep(Duration::from_millis(EVENT_MANAGER_TICK));
    }
}

impl EventRunner for SimpleRunner {
    /// Run events, return last event error.
    fn run(&self, events: Vec<(EventType, Arc<dyn EventHandler>)>) -> Result<(), EventError> {
        let mut result = Ok(());

        for (event_type, handler) in events {
            debug!("Event {:?}", event_type);
            result = handler.handle(event_type);
        }

        result
    }
}

/// Helper.
pub fn poll_and_run(manager: &mut EventManager, runner: Box<dyn EventRunner>) {
    loop {
        let events = manager.poll();
        let result = runner.run(events);

        match result {
            Err(EventError::SystemShutdown) => break,
            _ => {}
        }
    }
}

/// Simple poller.
struct SimplePoller {
    /// High priority events.
    high: Vec<Arc<dyn EventHandler>>,

    /// Low priority events.
    low: Vec<Arc<dyn EventHandler>>,
}

impl SimplePoller {
    pub fn new() -> SimplePoller {
        SimplePoller {
            high: Vec::new(),
            low: Vec::new(),
        }
    }

    pub fn register_low(&mut self, handler: Arc<dyn EventHandler>) {
        self.low.push(handler);
    }

    pub fn register_high(&mut self, handler: Arc<dyn EventHandler>) {
        self.high.push(handler);
    }
}

/// File Descriptor poller.
struct FdescPoller {
    /// Token index.
    index: usize,

    /// Token to handler map.
    handlers: HashMap<Token, Arc<dyn EventHandler>>,

    /// mio::Poll
    poll: Poll,

    /// poll timeout in msecs
    timeout: Duration,
}

impl FdescPoller {
    pub fn new() -> FdescPoller {
        FdescPoller {
            index: 1, // Reserve 0
            handlers: HashMap::new(),
            poll: Poll::new().unwrap(),
            timeout: Duration::from_millis(EVENT_MANAGER_TICK),
        }
    }
}

/// TimerHandler trait.
pub struct TimerHandler {
    /// Expiration time.
    exp: Instant,

    /// EventHandler
    handler: Arc<dyn EventHandler>,
}

impl TimerHandler {
    /// Constructor.
    pub fn new(d: Duration, handler: Arc<dyn EventHandler>) -> TimerHandler {
        TimerHandler {
            exp: Instant::now() + d,
            handler,
        }
    }

    /// Get expiration time.
    pub fn expiration(&self) -> Instant {
        self.exp
    }
}

impl Ord for TimerHandler {
    fn cmp(&self, other: &Self) -> Ordering {
        other.expiration().cmp(&self.expiration())
    }
}

impl PartialOrd for TimerHandler {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for TimerHandler {}

impl PartialEq for TimerHandler {
    fn eq(&self, other: &Self) -> bool {
        other.expiration() == self.expiration()
    }
}

/// Timer poller.
struct TimerPoller {
    /// Ordering handler by expiration time.
    heap: RefCell<BinaryHeap<TimerHandler>>,
}

impl TimerPoller {
    /// Constructor.
    pub fn new() -> TimerPoller {
        TimerPoller {
            heap: RefCell::new(BinaryHeap::new()),
        }
    }

    /// Register timer handler.
    pub fn register(&self, d: Duration, handler: Arc<dyn EventHandler>) {
        let timer_handler = TimerHandler::new(d, handler);

        self.heap.borrow_mut().push(timer_handler);
    }

    /// Pop a timer handler if it is expired.
    fn pop_if_expired(&self) -> Option<TimerHandler> {
        if match self.heap.borrow_mut().peek() {
            Some(handler) if handler.expiration() < Instant::now() => true,
            _ => false,
        } {
            self.heap.borrow_mut().pop()
        } else {
            None
        }
    }

    /// Run all expired event handler.
    pub fn poll(&self) -> Option<Arc<dyn EventHandler>> {
        match self.pop_if_expired() {
            Some(timer_handler) => Some(timer_handler.handler),
            None => None,
        }
    }
}

/// Channel poller.
struct ChannelPoller {
    /// Channel Message Handlers.
    handlers: RefCell<Vec<Box<dyn ChannelHandler>>>,
}

impl ChannelPoller {
    /// Constructor.
    pub fn new() -> ChannelPoller {
        ChannelPoller {
            handlers: RefCell::new(Vec::new()),
        }
    }

    /// Register handler.
    pub fn register_handler(&self, handler: Box<dyn ChannelHandler>) {
        self.handlers.borrow_mut().push(handler);
    }

    /// Poll all channels and handle all messages.
    pub fn poll_channel(&self) -> Vec<(EventType, Arc<dyn EventHandler>)> {
        let mut vec = Vec::new();

        for handler in self.handlers.borrow().iter() {
            let mut tmp = (*handler).poll_channel();

            vec.append(&mut tmp);
        }

        vec
    }
}

/// Channel Handler trait.
pub trait ChannelHandler {
    /// Poll channel.
    fn poll_channel(&self) -> Vec<(EventType, Arc<dyn EventHandler>)>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use mio::net::UnixListener;
    use mio::net::UnixStream;
    use std::fs::*;
    use std::path::Path;
    use std::sync::mpsc;
    use std::sync::Mutex;
    use std::thread;

    /// Simple event test.
    struct TestState {
        vec: Vec<u32>,
    }

    impl TestState {
        pub fn new() -> TestState {
            TestState { vec: Vec::new() }
        }
    }

    struct TestHandler {
        priority: u32,
        state: Arc<Mutex<TestState>>,
    }

    impl EventHandler for TestHandler {
        fn handle(&self, event_type: EventType) -> Result<(), EventError> {
            match event_type {
                EventType::SimpleEvent => {
                    let state = self.state.clone();
                    let mut state = state.lock().unwrap();

                    state.vec.push(self.priority);
                }
                _ => {
                    assert!(false);
                }
            }

            Ok(())
        }
    }

    #[test]
    pub fn test_simple_event() {
        let em = EventManager::new();
        let state = Arc::new(Mutex::new(TestState::new()));

        let handler1 = TestHandler {
            state: state.clone(),
            priority: 100,
        };
        let handler2 = TestHandler {
            state: state.clone(),
            priority: 200,
        };

        em.register_low(Arc::new(handler1));
        em.register_high(Arc::new(handler2));

        let runner = SimpleRunner::new();
        let events = em.poll();
        runner.run(events).unwrap();

        let state = state.lock().unwrap();

        assert_eq!(state.vec[0], 200);
        assert_eq!(state.vec[1], 100);
    }

    /// File Descriptor event test.
    ///  Create a UNIX domain socket server and listen, and connect to the socket
    ///  from another thread, and the server accept it.

    struct TestListener {
        accept: Mutex<bool>,
    }

    impl EventHandler for TestListener {
        fn handle(&self, event_type: EventType) -> Result<(), EventError> {
            match event_type {
                EventType::ReadEvent => {
                    *self.accept.lock().unwrap() = true;
                    println!("Listener got a connection");
                }
                _ => {
                    assert!(false);
                }
            }

            Ok(())
        }
    }

    #[test]
    pub fn test_fd_event() {
        let path = Path::new("/tmp/test_uds.sock");
        remove_file(&path).unwrap();

        let em = EventManager::new();
        let mut listener = UnixListener::bind(path).unwrap();
        let eh = Arc::new(TestListener {
            accept: Mutex::new(false),
        });

        em.register_listen(&mut listener, eh.clone()).unwrap();

        let _ = thread::spawn(move || {
            match UnixStream::connect(&path) {
                Ok(_stream) => {
                    println!("Stream");
                }
                Err(_) => {
                    println!("Connect error");
                }
            }

            ()
        });

        let runner = SimpleRunner::new();
        let events = em.poll();
        runner.run(events).unwrap();

        assert_eq!(*eh.accept.lock().unwrap(), true);
    }

    /// Timer event test.
    ///  Set 1 second timer and check the value before and after the timer
    ///  is expired.

    struct TimerEntry {
        done: Mutex<bool>,
    }

    impl TimerEntry {
        pub fn new() -> TimerEntry {
            TimerEntry {
                done: Mutex::new(false),
            }
        }

        pub fn done(&self) -> bool {
            *self.done.lock().unwrap()
        }
    }

    impl EventHandler for TimerEntry {
        fn handle(&self, event_type: EventType) -> Result<(), EventError> {
            match event_type {
                EventType::TimerEvent => {
                    *self.done.lock().unwrap() = true;
                }
                _ => {
                    assert!(false);
                }
            }

            Ok(())
        }
    }

    #[test]
    pub fn test_timer_event() {
        let em = EventManager::new();
        let d = Duration::from_secs(1);
        let tc = Arc::new(TimerEntry::new());
        let runner = SimpleRunner::new();

        em.register_timer(d, tc.clone());
        let events = em.poll();
        runner.run(events).unwrap();

        assert_eq!(tc.done(), false);

        thread::sleep(Duration::from_millis(1100));

        let events = em.poll();
        runner.run(events).unwrap();

        assert_eq!(tc.done(), true);
    }

    /// Channel event test.
    ///  Create a channel handler and a message handler, sending a message
    ///  from another thread and check if it is handled by message handler.

    pub enum TestMessage {
        Number(i32),
        Desc(String),
    }

    pub struct TestMessageState {
        number: Option<i32>,
        desc: Option<String>,
    }

    impl TestMessageState {
        pub fn new() -> TestMessageState {
            TestMessageState {
                number: None,
                desc: None,
            }
        }
    }

    pub struct TestChannelHandler {
        receiver: mpsc::Receiver<TestMessage>,
        state: Arc<Mutex<TestMessageState>>,
    }

    impl TestChannelHandler {
        pub fn new(
            receiver: mpsc::Receiver<TestMessage>,
            state: Arc<Mutex<TestMessageState>>,
        ) -> TestChannelHandler {
            TestChannelHandler {
                receiver: receiver,
                state: state,
            }
        }
    }

    impl ChannelHandler for TestChannelHandler {
        /// Poll channel.
        fn poll_channel(&self) -> Vec<(EventType, Arc<dyn EventHandler>)> {
            let mut vec = Vec::new();

            while let Ok(d) = self.receiver.try_recv() {
                let handler = TestMessageHandler::new(d, self.state.clone());

                vec.push((EventType::ChannelEvent, handler));
            }

            vec
        }
    }

    pub struct TestMessageHandler {
        message: TestMessage,
        state: Arc<Mutex<TestMessageState>>,
    }

    impl TestMessageHandler {
        pub fn new(
            message: TestMessage,
            state: Arc<Mutex<TestMessageState>>,
        ) -> Arc<dyn EventHandler> {
            Arc::new(TestMessageHandler {
                message: message,
                state: state,
            })
        }
    }

    impl EventHandler for TestMessageHandler {
        /// Handle event.
        fn handle(&self, event_type: EventType) -> Result<(), EventError> {
            match event_type {
                EventType::ChannelEvent => match &self.message {
                    TestMessage::Number(i) => {
                        let state = self.state.clone();
                        let mut state = state.lock().unwrap();

                        (*state).number.replace(*i);
                    }
                    TestMessage::Desc(s) => {
                        let state = self.state.clone();
                        let mut state = state.lock().unwrap();

                        (*state).desc.replace(s.clone());
                    }
                },
                _ => assert!(false),
            }

            Ok(())
        }
    }

    #[test]
    pub fn test_channel_event() {
        let em = EventManager::new();
        let runner = SimpleRunner::new();

        let (sender, receiver) = mpsc::channel::<TestMessage>();
        let state = Arc::new(Mutex::new(TestMessageState::new()));

        let channel_handler = TestChannelHandler::new(receiver, state.clone());
        em.register_channel(Box::new(channel_handler));

        thread::spawn(move || {
            sender.send(TestMessage::Number(100)).unwrap();
        });

        let events = em.poll();
        runner.run(events).unwrap();

        let state = state.lock().unwrap();
        assert_eq!(state.number, Some(100));
        assert_eq!(state.desc, None);
    }
}
