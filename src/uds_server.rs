//
// Eventum
//   Copyright (C) 2020 Toshiaki Takada
//
// Unix Domain Socket Server.
//

use std::io::Read;
use std::io::Write;
use std::sync::Arc;
use std::cell::Cell;
use std::cell::RefCell;
use std::path::PathBuf;
use std::net::Shutdown;
use std::collections::HashMap;
use std::sync::Mutex;

use log::{debug, error};
use mio::Token;
use mio::net::UnixListener;
use mio::net::UnixStream;

use super::core::*;

/// Trait UdsServer handler.
pub trait UdsServerHandler {

    /// callback when server accepts client connection.
    fn handle_connect(&self, server: Arc<UdsServer>, entry: &UdsServerEntry) -> Result<(), EventError>;

    /// callback when server detects client disconnected.
    fn handle_disconnect(&self, server: Arc<UdsServer>, entry: &UdsServerEntry) -> Result<(), EventError>;

    /// callback when server entry received message.
    fn handle_message(&self, server: Arc<UdsServer>, entry: &UdsServerEntry) -> Result<(), EventError>;
}

unsafe impl Send for UdsServerEntry {}
unsafe impl Sync for UdsServerEntry {}

/// Unix Domain Socket server entry, created per connect.
pub struct UdsServerEntry {

    /// Index.
    index: u32,

    /// EventHandler token.
    token: Cell<Token>,

    /// Pointer to UdsServer.
    server: RefCell<Arc<UdsServer>>,

    /// mio UnixStream.
    stream: RefCell<Option<UnixStream>>,
}

/// UdsServerEntry implementation.
impl UdsServerEntry {

    /// Constructor.
    pub fn new(server: Arc<UdsServer>, index: u32) -> UdsServerEntry {
        UdsServerEntry {
            index: index,
            token: Cell::new(Token(0)),
            server: RefCell::new(server),
            stream: RefCell::new(None),
        }
    }

    /// Return index.
    pub fn index(&self) -> u32 {
        self.index
    }

    /// Send a message through UnixStream.
    pub fn stream_send(&self, message: &str) -> Result<(), EventError> {
        match *self.stream.borrow_mut() {
            Some(ref mut stream) => {
                if let Err(_err) = stream.write_all(message.as_bytes()) {
                    return Err(EventError::WriteError("UDS".to_string()))
                }
            },
            None => {
                return Err(EventError::WriteError("UDS".to_string()))
            }
        }

        Ok(())
    }

    /// Receive a message through UnixStream.
    pub fn stream_read(&self) -> Result<String, EventError> {
        match *self.stream.borrow_mut() {
            Some(ref mut stream) => {
                let mut buffer = Vec::new();

                if let Err(err) = stream.read_to_end(&mut buffer) {
                    if err.kind() != std::io::ErrorKind::WouldBlock {
                        return Err(EventError::ReadError(err.to_string()))
                    }
                }
                
                let str = std::str::from_utf8(&buffer).unwrap();
                if str.len() > 0 {
                    let message = String::from(str);
                    Ok(message)
                } else {
                    Err(EventError::ReadError("Empty string from stream".to_string()))
                }
            },
            None => {
                Err(EventError::NoStream)
            }
        }
    }
}

/// Drop implementation.
impl Drop for UdsServerEntry {
    fn drop(&mut self) {
        debug!("Drop UdsServerEntry");
    }
}

/// EventHandler implementation for UdsServerEntry.
impl EventHandler for UdsServerEntry {

    /// Handle event.
    fn handle(&self, e: EventType) -> Result<(), EventError> {
        match e {
            EventType::ReadEvent => {
                let server = self.server.borrow_mut();
                let inner = server.get_inner();
                let handler = inner.handler.borrow_mut();

                // Dispatch message to Server message handler.
                return handler.handle_message(server.clone(), self);
            },
            EventType::ErrorEvent => {
                let server = self.server.borrow_mut();
                let inner = server.get_inner();
                let handler = inner.handler.borrow_mut();

                inner.entries.borrow_mut().remove(&self.index);

                // Dispatch message to Server message handler.
                return handler.handle_disconnect(server.clone(), self);
            },
            _ => {
                debug!("Unknown event");
            }
        }

        Ok(())
    }

    /// Set token to entry.
    fn set_token(&self, token: Token) {
        self.token.replace(token);
    }

    /// Get token from entry.
    fn get_token(&self) -> Token {
        self.token.get()
    }
}

/// UdsServer Inner.
pub struct UdsServerInner {

    /// UdsServer
    server: RefCell<Arc<UdsServer>>,

    /// EventManager.
    event_manager: Arc<Mutex<EventManager>>,

    /// Message Server Handler.
    handler: RefCell<Arc<dyn UdsServerHandler>>,

    /// mio UnixListener.
    listener: RefCell<UnixListener>,

    /// Index for UdsServerEntry.
    index: Cell<u32>,

    /// Index to UdsServerEntry map.
    entries: RefCell<HashMap<u32, Arc<UdsServerEntry>>>,
}

/// UdsServerInner implementation.
impl UdsServerInner {

    /// Constructor.
    pub fn new(server: Arc<UdsServer>, event_manager: Arc<Mutex<EventManager>>,
               handler: Arc<dyn UdsServerHandler>, path: &PathBuf) -> UdsServerInner {
        let listener = match UnixListener::bind(path) {
            Ok(listener) => listener,
            Err(_) => panic!("UnixListener::bind() error"),
        };

        UdsServerInner {
            server: RefCell::new(server),
            event_manager: event_manager,
            handler: RefCell::new(handler),
            listener: RefCell::new(listener),
            index: Cell::new(0),
            entries: RefCell::new(HashMap::new()),
        }
    }

    /// Return UdsServerEntry by index.
    pub fn lookup_entry(&self, index: u32) -> Option<Arc<UdsServerEntry>> {
        match self.entries.borrow_mut().get(&index) {
            Some(entry) => Some(entry.clone()),
            None => None
        }
    }
}

unsafe impl Send for UdsServerInner {}
unsafe impl Sync for UdsServerInner {}

/// UdsServer Message Server.
pub struct UdsServer {

    /// UdsServer Inner.
    inner: RefCell<Option<Arc<UdsServerInner>>>,
}
  
/// UdsServer implementation.
impl UdsServer {

    /// Constructor.
    fn new() -> UdsServer {
        UdsServer {
            inner: RefCell::new(None),
        }
    }

    /// Return UdsServerInner.
    pub fn get_inner(&self) -> Arc<UdsServerInner> {
        if let Some(ref mut inner) = *self.inner.borrow_mut() {
            return inner.clone()
        }

        // should not happen.
        panic!("No inner exists");
    }

    /// Start UdsServer.
    pub fn start(event_manager: Arc<Mutex<EventManager>>,
                 handler: Arc<dyn UdsServerHandler>, path: &PathBuf) -> Arc<UdsServer> {
        let server = Arc::new(UdsServer::new());
        let inner = Arc::new(UdsServerInner::new(server.clone(), event_manager.clone(), handler.clone(), path));

        event_manager.lock().unwrap().register_listen(&mut *inner.listener.borrow_mut(), inner.clone());

        server.inner.borrow_mut().replace(inner);
        server
    }

    /// Shutdown UdsServerEntry.
    pub fn shutdown_entry(&self, entry: &UdsServerEntry) {
        if let Some(ref mut stream) = *entry.stream.borrow_mut() {
            self.get_inner().event_manager.lock().unwrap().unregister_read(stream, entry.get_token());

            if let Err(err) = stream.shutdown(Shutdown::Both) {
                error!("Entry shutdown error {}", err.to_string());
            }
        }

        entry.stream.replace(None);
    }
}

/// EventHandler implementation for UdsServerInner.
impl EventHandler for UdsServerInner {

    /// Event handler.
    fn handle(&self, e: EventType) -> Result<(), EventError> {
        let server = self.server.borrow_mut();

        match e {
            EventType::ReadEvent => {
                let listener = self.listener.borrow_mut();

                match listener.accept() {
                    Ok((mut stream, _addr)) => {
                        debug!("Accept a UDS client");

                        let index = self.index.get();
                        self.index.set(index + 1);

                        let entry = Arc::new(UdsServerEntry::new(server.clone(), index));
                        let event_manager = self.event_manager.lock().unwrap();

                        if let Err(_) = self.handler.borrow_mut().handle_connect(server.clone(), &entry) {
                            error!("UDS Server handler error");
                        }

                        event_manager.register_read(&mut stream, entry.clone());
                        entry.stream.borrow_mut().replace(stream);

                        self.entries.borrow_mut().insert(index, entry);
                    },
                    Err(err) => debug!("Accept failed: {:?}", err),
                }
            },
            _ => {
                debug!("Unknown event");
            }
        }

        Ok(())
    }
}
