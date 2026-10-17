// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! In-memory files and duplex byte streams. No OS resources or native runtime.
//! This is an interface probe, not a model of TCP scheduling or durability.
use super::{Cancellation, File, FileOptions, OperationResult, Socket};
use event_listener::Event;
use socketry_executor::{AllocationMode, BufferResult, FileAllocate, SyncMode};
use socketry_executor::{Interest, IoBuf, IoBufMut};
use std::collections::{HashMap, VecDeque};
use std::io::{self, SeekFrom};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::{
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};

type Bytes = Arc<Mutex<Vec<u8>>>;

pub struct Scheduler {
    files: Mutex<HashMap<PathBuf, Bytes>>,
    listeners: Mutex<HashMap<SocketAddr, Weak<Incoming>>>,
    read_limit: usize,
    write_limit: usize,
}

pub struct FileHandle {
    position: Mutex<u64>,
    bytes: Bytes,
    writable: bool,
    readable: bool,
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
    local: SocketAddr,
    peer: SocketAddr,
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

impl FileAllocate for Scheduler {
    async fn file_allocate(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        mode: AllocationMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        let end = offset
            .checked_add(length)
            .filter(|end| length > 0 && *end <= i64::MAX as u64)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        if !file.writable {
            return Err(io::Error::from(io::ErrorKind::PermissionDenied).into());
        }
        if end > 1024 * 1024 {
            return Err(io::Error::from(io::ErrorKind::FileTooLarge).into());
        }
        let mut bytes = file.bytes.lock().unwrap();
        let additional = (end as usize).saturating_sub(bytes.len());
        bytes.reserve(additional);
        if mode == AllocationMode::Extend && end as usize > bytes.len() {
            bytes.resize(end as usize, 0);
        }
        Ok(())
    }
}

impl File for Scheduler {
    type File = FileHandle;

    async fn file_truncate(
        &self,
        file: &FileHandle,
        length: u64,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        if length > i64::MAX as u64 {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        if !file.writable {
            return Err(io::Error::from(io::ErrorKind::PermissionDenied).into());
        }
        // Match the simulator's existing bounded storage policy without allocating a huge sparse file in memory.
        if length > 1024 * 1024 {
            return Err(io::Error::from(io::ErrorKind::FileTooLarge).into());
        }
        file.bytes.lock().unwrap().resize(length as usize, 0);
        Ok(())
    }

    async fn file_sync(
        &self,
        _file: &Self::File,
        _mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        Ok(())
    }

    async fn file_seek(
        &self,
        file: &FileHandle,
        position: SeekFrom,
        cancellation: &Cancellation,
    ) -> OperationResult<u64> {
        cancellation.check()?;
        let mut cursor = file.position.lock().unwrap();
        let position = match position {
            SeekFrom::Start(offset) => i128::from(offset),
            SeekFrom::Current(offset) => i128::from(*cursor) + i128::from(offset),
            SeekFrom::End(offset) => file.bytes.lock().unwrap().len() as i128 + i128::from(offset),
        };
        if !(0..=i128::from(i64::MAX)).contains(&position) {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        *cursor = position as u64;
        Ok(*cursor)
    }

    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<FileHandle> {
        cancellation.check()?;
        if (!options.read && !options.write)
            || ((options.create || options.create_new || options.truncate) && !options.write)
        {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        let mut files = self.files.lock().unwrap();
        if options.create_new && files.contains_key(path) {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists).into());
        }
        if !files.contains_key(path) && !(options.create || options.create_new) {
            return Err(io::Error::from(io::ErrorKind::NotFound).into());
        }
        let bytes = files.entry(path.to_owned()).or_default().clone();
        if options.truncate {
            bytes.lock().unwrap().clear();
        }
        Ok(FileHandle {
            position: Mutex::new(0),
            bytes,
            writable: options.write,
            readable: options.read,
        })
    }

    async fn file_read<B: IoBufMut>(
        &self,
        file: &FileHandle,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let mut position = file.position.lock().unwrap();
        let (result, buffer) = self.read_file_at(file, buffer, *position, cancellation);
        if let Ok(count) = &result {
            *position += *count as u64;
        }
        (result, buffer)
    }

    async fn file_read_at<B: IoBufMut>(
        &self,
        file: &FileHandle,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.read_file_at(file, buffer, offset, cancellation)
    }
    async fn file_write<B: IoBuf>(
        &self,
        file: &FileHandle,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let mut position = file.position.lock().unwrap();
        let (result, buffer) = self.write_file_at(file, buffer, *position, cancellation);
        if let Ok(count) = &result {
            *position += *count as u64;
        }
        (result, buffer)
    }

    async fn file_write_at<B: IoBuf>(
        &self,
        file: &FileHandle,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.write_file_at(file, buffer, offset, cancellation)
    }
}

impl Socket for Scheduler {
    type Socket = Connection;
    type Listener = Listener;

    async fn socket_listen(
        &self,
        mut address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Listener> {
        cancellation.check()?;
        let mut listeners = self.listeners.lock().unwrap();
        listeners.retain(|_, incoming| incoming.strong_count() > 0);
        if address.port() == 0 {
            let port = (10000..=u16::MAX)
                .find(|port| {
                    let mut candidate = address;
                    candidate.set_port(*port);
                    !listeners.contains_key(&candidate)
                })
                .ok_or_else(|| io::Error::from(io::ErrorKind::AddrNotAvailable))?;
            address.set_port(port);
        }
        if listeners.contains_key(&address) {
            return Err(io::Error::from(io::ErrorKind::AddrInUse).into());
        }
        let incoming = Arc::new(Incoming::default());
        listeners.insert(address, Arc::downgrade(&incoming));
        Ok(Listener { address, incoming })
    }

    fn listener_addr(&self, listener: &Listener) -> io::Result<SocketAddr> {
        Ok(listener.address)
    }

    fn socket_local_addr(&self, socket: &Connection) -> io::Result<SocketAddr> {
        Ok(socket.local)
    }
    fn socket_peer_addr(&self, socket: &Connection) -> io::Result<SocketAddr> {
        Ok(socket.peer)
    }
    async fn io_wait(
        &self,
        socket: &Connection,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        if matches!(interest, Interest::Writable) {
            return Ok(());
        }
        loop {
            let ready = socket.input.ready.listen();
            {
                let state = socket.input.state.lock().unwrap();
                if !state.bytes.is_empty() || state.writer_closed {
                    return Ok(());
                }
            }
            wait(ready, cancellation).await?;
        }
    }
    async fn accept(
        &self,
        listener: &Listener,
        cancellation: &Cancellation,
    ) -> OperationResult<(Connection, SocketAddr)> {
        cancellation.check()?;
        loop {
            // Register before checking state; await and notify outside locks.
            let ready = listener.incoming.ready.listen();
            let socket = listener.incoming.sockets.lock().unwrap().pop_front();
            if let Some(socket) = socket {
                let peer = socket.peer;
                return Ok((socket, peer));
            }
            wait(ready, cancellation).await?;
        }
    }

    async fn connect(
        &self,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<Connection> {
        cancellation.check()?;
        let incoming = self
            .listeners
            .lock()
            .unwrap()
            .get(&address)
            .and_then(Weak::upgrade)
            .ok_or_else(|| io::Error::from(io::ErrorKind::ConnectionRefused))?;
        let input = Arc::new(Pipe::default());
        let output = Arc::new(Pipe::default());
        let mut client_address = address;
        client_address.set_port(40000);
        let server = Connection {
            local: address,
            peer: client_address,
            input: output.clone(),
            output: input.clone(),
        };
        incoming.sockets.lock().unwrap().push_back(server);
        incoming.ready.notify(usize::MAX);
        Ok(Connection {
            input,
            output,
            local: client_address,
            peer: address,
        })
    }

    async fn io_read<B: IoBufMut>(
        &self,
        socket: &Connection,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        loop {
            let ready = socket.input.ready.listen();
            {
                let mut state = socket.input.state.lock().unwrap();
                let count = buffer
                    .as_ref()
                    .len()
                    .min(self.read_limit)
                    .min(state.bytes.len());
                if count > 0 || buffer.as_ref().is_empty() || state.writer_closed {
                    for byte in &mut buffer.as_mut()[..count] {
                        *byte = state.bytes.pop_front().unwrap();
                    }
                    return (Ok(count), buffer);
                }
            }
            if let Err(error) = wait(ready, cancellation).await {
                return (Err(error), buffer);
            }
        }
    }

    async fn io_write<B: IoBuf>(
        &self,
        socket: &Connection,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        let count = buffer.as_ref().len().min(self.write_limit);
        {
            let mut state = socket.output.state.lock().unwrap();
            if state.reader_closed {
                return (
                    Err(io::Error::from(io::ErrorKind::BrokenPipe).into()),
                    buffer,
                );
            }
            state.bytes.extend(&buffer.as_ref()[..count]);
        }
        socket.output.ready.notify(usize::MAX);
        (Ok(count), buffer)
    }
}

async fn wait(
    ready: event_listener::EventListener,
    cancellation: &Cancellation,
) -> OperationResult<()> {
    let mut ready = pin!(ready);
    let mut cancelled = pin!(cancellation.cancelled());
    poll_fn(|context| {
        if ready.as_mut().poll(context).is_ready() {
            return Poll::Ready(Ok(()));
        }
        cancelled
            .as_mut()
            .poll(context)
            .map(|()| Err(socketry_executor::Cancelled.into()))
    })
    .await
}

impl Scheduler {
    fn read_file_at<B: IoBufMut>(
        &self,
        file: &FileHandle,
        mut buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        if !file.readable {
            return (
                Err(io::Error::from(io::ErrorKind::PermissionDenied).into()),
                buffer,
            );
        }
        let bytes = file.bytes.lock().unwrap();
        let count = if offset >= bytes.len() as u64 {
            0
        } else {
            let offset = offset as usize;
            let count = buffer
                .as_ref()
                .len()
                .min(self.read_limit)
                .min(bytes.len() - offset);
            buffer.as_mut()[..count].copy_from_slice(&bytes[offset..offset + count]);
            count
        };
        (Ok(count), buffer)
    }

    fn write_file_at<B: IoBuf>(
        &self,
        file: &FileHandle,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        if !file.writable {
            return (
                Err(io::Error::from(io::ErrorKind::PermissionDenied).into()),
                buffer,
            );
        }
        let count = buffer.as_ref().len().min(self.write_limit);
        // Keep the model's filesystem bounded so malformed offsets cannot turn a test error into an unbounded allocation.
        let end = offset
            .checked_add(count as u64)
            .filter(|end| *end <= 1024 * 1024);
        let Some(end) = end else {
            return (
                Err(io::Error::from(io::ErrorKind::InvalidInput).into()),
                buffer,
            );
        };
        if count > 0 {
            let mut bytes = file.bytes.lock().unwrap();
            let length = bytes.len().max(end as usize);
            bytes.resize(length, 0);
            bytes[offset as usize..end as usize].copy_from_slice(&buffer.as_ref()[..count]);
        }
        (Ok(count), buffer)
    }
}
