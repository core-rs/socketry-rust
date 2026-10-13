// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! In-memory files and duplex byte streams. No OS resources or native runtime.
//! This is an interface probe, not a model of TCP scheduling or durability.
use super::{File, Open, Socket};
use event_listener::Event;
use socketry_executor::BufferResult;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

type Bytes = Arc<Mutex<Vec<u8>>>;

pub struct Scheduler {
    files: Mutex<HashMap<PathBuf, Bytes>>,
    listeners: Mutex<HashMap<SocketAddr, Weak<Incoming>>>,
    read_limit: usize,
    write_limit: usize,
}

pub struct FileHandle {
    bytes: Bytes,
    writable: bool,
}

#[derive(Default)]
struct Incoming {
    sockets: Mutex<VecDeque<Connection>>,
    ready: Event,
}

pub struct Listener {
    address: SocketAddr,
    incoming: Arc<Incoming>,
}

#[derive(Default)]
struct PipeState {
    bytes: VecDeque<u8>,
    reader_closed: bool,
    writer_closed: bool,
}

#[derive(Default)]
struct Pipe {
    state: Mutex<PipeState>,
    ready: Event,
}

pub struct Connection {
    input: Arc<Pipe>,
    output: Arc<Pipe>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.input.state.lock().unwrap().reader_closed = true;
        self.output.state.lock().unwrap().writer_closed = true;
        self.input.ready.notify(usize::MAX);
        self.output.ready.notify(usize::MAX);
    }
}

impl Scheduler {
    pub fn new(read_limit: usize, write_limit: usize) -> Self {
        assert!(read_limit > 0 && write_limit > 0);
        Self {
            files: Mutex::new(HashMap::new()),
            listeners: Mutex::new(HashMap::new()),
            read_limit,
            write_limit,
        }
    }

    pub fn insert(&self, path: &Path, bytes: &[u8]) {
        self.files
            .lock()
            .unwrap()
            .insert(path.to_owned(), Arc::new(Mutex::new(bytes.to_vec())));
    }

    pub fn contents(&self, path: &Path) -> Vec<u8> {
        self.files.lock().unwrap()[path].lock().unwrap().clone()
    }

    #[cfg(test)]
    pub fn open_files(&self) -> usize {
        self.files
            .lock()
            .unwrap()
            .values()
            .map(|bytes| Arc::strong_count(bytes) - 1)
            .sum()
    }
}

impl File for Scheduler {
    type File = FileHandle;

    async fn file_open(&self, path: &Path, mode: Open) -> io::Result<FileHandle> {
        let mut files = self.files.lock().unwrap();
        let writable = matches!(mode, Open::Create);
        let bytes = match mode {
            Open::Read => files.get(path).ok_or(io::ErrorKind::NotFound)?.clone(),
            Open::Create => {
                let bytes = files.entry(path.to_owned()).or_default();
                bytes.lock().unwrap().clear();
                bytes.clone()
            }
        };
        Ok(FileHandle { bytes, writable })
    }

    async fn file_read_at(
        &self,
        file: &FileHandle,
        mut buffer: Vec<u8>,
        offset: u64,
    ) -> BufferResult {
        let bytes = file.bytes.lock().unwrap();
        let count = if offset >= bytes.len() as u64 {
            0
        } else {
            let offset = offset as usize;
            let count = buffer.len().min(self.read_limit).min(bytes.len() - offset);
            buffer[..count].copy_from_slice(&bytes[offset..offset + count]);
            count
        };
        (Ok(count), buffer)
    }

    async fn file_write_at(&self, file: &FileHandle, buffer: Vec<u8>, offset: u64) -> BufferResult {
        if !file.writable {
            return (Err(io::ErrorKind::PermissionDenied.into()), buffer);
        }
        let count = buffer.len().min(self.write_limit);
        // Keep the model's filesystem bounded so malformed offsets cannot
        // turn a test error into an unbounded allocation.
        let end = offset
            .checked_add(count as u64)
            .filter(|end| *end <= 1024 * 1024);
        let Some(end) = end else {
            return (Err(io::ErrorKind::InvalidInput.into()), buffer);
        };
        if count > 0 {
            let mut bytes = file.bytes.lock().unwrap();
            let length = bytes.len().max(end as usize);
            bytes.resize(length, 0);
            bytes[offset as usize..end as usize].copy_from_slice(&buffer[..count]);
        }
        (Ok(count), buffer)
    }
}

impl Socket for Scheduler {
    type Socket = Connection;
    type Listener = Listener;

    async fn socket_listen(&self, mut address: SocketAddr) -> io::Result<Listener> {
        let mut listeners = self.listeners.lock().unwrap();
        listeners.retain(|_, incoming| incoming.strong_count() > 0);
        if address.port() == 0 {
            let port = (10000..=u16::MAX)
                .find(|port| {
                    let mut candidate = address;
                    candidate.set_port(*port);
                    !listeners.contains_key(&candidate)
                })
                .ok_or(io::ErrorKind::AddrNotAvailable)?;
            address.set_port(port);
        }
        if listeners.contains_key(&address) {
            return Err(io::ErrorKind::AddrInUse.into());
        }
        let incoming = Arc::new(Incoming::default());
        listeners.insert(address, Arc::downgrade(&incoming));
        Ok(Listener { address, incoming })
    }

    fn listener_addr(&self, listener: &Listener) -> io::Result<SocketAddr> {
        Ok(listener.address)
    }

    async fn socket_accept(&self, listener: &Listener) -> io::Result<Connection> {
        loop {
            // Register before checking state; await and notify outside locks.
            let ready = listener.incoming.ready.listen();
            let socket = listener.incoming.sockets.lock().unwrap().pop_front();
            if let Some(socket) = socket {
                return Ok(socket);
            }
            ready.await;
        }
    }

    async fn socket_connect(&self, address: SocketAddr) -> io::Result<Connection> {
        let incoming = self
            .listeners
            .lock()
            .unwrap()
            .get(&address)
            .and_then(Weak::upgrade)
            .ok_or(io::ErrorKind::ConnectionRefused)?;
        let input = Arc::new(Pipe::default());
        let output = Arc::new(Pipe::default());
        let server = Connection {
            input: output.clone(),
            output: input.clone(),
        };
        incoming.sockets.lock().unwrap().push_back(server);
        incoming.ready.notify(usize::MAX);
        Ok(Connection { input, output })
    }

    async fn socket_recv(&self, socket: &Connection, mut buffer: Vec<u8>) -> BufferResult {
        loop {
            let ready = socket.input.ready.listen();
            {
                let mut state = socket.input.state.lock().unwrap();
                let count = buffer.len().min(self.read_limit).min(state.bytes.len());
                if count > 0 || buffer.is_empty() || state.writer_closed {
                    for byte in &mut buffer[..count] {
                        *byte = state.bytes.pop_front().unwrap();
                    }
                    return (Ok(count), buffer);
                }
            }
            ready.await;
        }
    }

    async fn socket_send(&self, socket: &Connection, buffer: Vec<u8>) -> BufferResult {
        let count = buffer.len().min(self.write_limit);
        {
            let mut state = socket.output.state.lock().unwrap();
            if state.reader_closed {
                return (Err(io::ErrorKind::BrokenPipe.into()), buffer);
            }
            state.bytes.extend(&buffer[..count]);
        }
        socket.output.ready.notify(usize::MAX);
        (Ok(count), buffer)
    }
}
