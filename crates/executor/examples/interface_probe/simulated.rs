// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! In-memory files and duplex byte streams. No OS resources or native runtime.
//! This is an interface probe, not a model of TCP scheduling or durability.
use super::{Cancellation, File, FileOptions, OperationResult, Socket};
use event_listener::Event;
use socketry_executor::{AllocationMode, BufferResult, FileAllocate, SyncMode};
use socketry_executor::{Interest, IoBuf, IoBufMut, SocketDomain, SocketProtocol, SocketType};
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

fn available_port(
    listeners: &HashMap<SocketAddr, Weak<Incoming>>,
    mut address: SocketAddr,
) -> io::Result<u16> {
    (10000..=u16::MAX)
        .find(|port| {
            address.set_port(*port);
            !listeners.contains_key(&address)
        })
        .ok_or_else(|| io::Error::from(io::ErrorKind::AddrNotAvailable))
}

pub struct Scheduler {
    files: Mutex<HashMap<PathBuf, Bytes>>,
    listeners: Mutex<HashMap<SocketAddr, Weak<Incoming>>>,
    read_limit: usize,
    write_limit: usize,
}

pub struct FileDescriptor {
    position: Mutex<u64>,
    bytes: Bytes,
    writable: bool,
    readable: bool,
}

#[derive(Default)]
struct Incoming {
    sockets: Mutex<VecDeque<SocketDescriptor>>,
    ready: Event,
}

pub struct SocketDescriptor {
    state: Mutex<SocketState>,
}

enum SocketState {
    Open {
        domain: SocketDomain,
        kind: SocketType,
        local: SocketAddr,
    },
    Listening {
        address: SocketAddr,
        incoming: Arc<Incoming>,
    },
    Connected(Connection),
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

struct Connection {
    input: Arc<Pipe>,
    output: Arc<Pipe>,
    local: SocketAddr,
    peer: SocketAddr,
}

impl Drop for SocketDescriptor {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap();
        if let SocketState::Connected(socket) = state {
            socket.input.state.lock().unwrap().reader_closed = true;
            socket.output.state.lock().unwrap().writer_closed = true;
            socket.input.ready.notify(usize::MAX);
            socket.output.ready.notify(usize::MAX);
        }
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
    type File = FileDescriptor;

    async fn file_close(&self, file: Self::File) -> io::Result<()> {
        drop(file);
        Ok(())
    }

    async fn file_close_submit(&self, file: Self::File) -> io::Result<()> {
        self.file_close(file).await
    }

    async fn file_truncate(
        &self,
        file: &FileDescriptor,
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
        file: &FileDescriptor,
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
    ) -> OperationResult<FileDescriptor> {
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
        Ok(FileDescriptor {
            position: Mutex::new(0),
            bytes,
            writable: options.write,
            readable: options.read,
        })
    }

    async fn file_read<B: IoBufMut>(
        &self,
        file: &FileDescriptor,
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
        file: &FileDescriptor,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.read_file_at(file, buffer, offset, cancellation)
    }
    async fn file_write<B: IoBuf>(
        &self,
        file: &FileDescriptor,
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
        file: &FileDescriptor,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.write_file_at(file, buffer, offset, cancellation)
    }
}

impl Socket for Scheduler {
    type Socket = SocketDescriptor;

    async fn socket_open(
        &self,
        domain: SocketDomain,
        kind: SocketType,
        protocol: Option<SocketProtocol>,
        cancellation: &Cancellation,
    ) -> OperationResult<SocketDescriptor> {
        cancellation.check()?;
        match (kind, protocol) {
            (SocketType::Stream, None | Some(SocketProtocol::Tcp)) => {}
            (SocketType::Datagram, None | Some(SocketProtocol::Udp)) => {
                return Err(io::Error::from(io::ErrorKind::Unsupported).into());
            }
            _ => return Err(io::Error::from(io::ErrorKind::InvalidInput).into()),
        }
        let local = match domain {
            SocketDomain::Ipv4 => SocketAddr::from(([0, 0, 0, 0], 0)),
            SocketDomain::Ipv6 => SocketAddr::from(([0; 16], 0)),
        };
        Ok(SocketDescriptor {
            state: Mutex::new(SocketState::Open {
                domain,
                kind,
                local,
            }),
        })
    }

    async fn socket_bind(
        &self,
        socket: &SocketDescriptor,
        mut address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        let mut state = socket.state.lock().unwrap();
        let SocketState::Open {
            domain,
            kind: SocketType::Stream,
            local,
        } = &mut *state
        else {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        };
        if SocketDomain::from(address) != *domain {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        if local.port() != 0 {
            return Err(io::Error::from(io::ErrorKind::AddrInUse).into());
        }
        let mut listeners = self.listeners.lock().unwrap();
        // Expired weak registrations do not reserve their former addresses.
        listeners.retain(|_, incoming| incoming.strong_count() > 0);
        if address.port() == 0 {
            address.set_port(available_port(&listeners, address)?);
        } else if listeners.contains_key(&address) {
            return Err(io::Error::from(io::ErrorKind::AddrInUse).into());
        }
        *local = address;
        Ok(())
    }

    async fn socket_listen(
        &self,
        socket: &SocketDescriptor,
        _backlog: i32,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        let mut state = socket.state.lock().unwrap();
        let (kind, mut local) = match &*state {
            SocketState::Open { kind, local, .. } => (*kind, *local),
            _ => return Err(io::Error::from(io::ErrorKind::InvalidInput).into()),
        };
        if kind != SocketType::Stream {
            return Err(io::Error::from(io::ErrorKind::Unsupported).into());
        }
        let mut listeners = self.listeners.lock().unwrap();
        listeners.retain(|_, incoming| incoming.strong_count() > 0);
        if local.port() == 0 {
            local.set_port(available_port(&listeners, local)?);
        }
        if listeners.contains_key(&local) {
            return Err(io::Error::from(io::ErrorKind::AddrInUse).into());
        }
        let incoming = Arc::new(Incoming::default());
        listeners.insert(local, Arc::downgrade(&incoming));
        *state = SocketState::Listening {
            address: local,
            incoming,
        };
        Ok(())
    }

    fn socket_local_addr(&self, socket: &SocketDescriptor) -> io::Result<SocketAddr> {
        match &*socket.state.lock().unwrap() {
            SocketState::Open { local, .. } => Ok(*local),
            SocketState::Listening { address, .. } => Ok(*address),
            SocketState::Connected(connection) => Ok(connection.local),
        }
    }

    fn socket_peer_addr(&self, socket: &SocketDescriptor) -> io::Result<SocketAddr> {
        match &*socket.state.lock().unwrap() {
            SocketState::Connected(connection) => Ok(connection.peer),
            _ => Err(io::Error::from(io::ErrorKind::NotConnected)),
        }
    }

    async fn socket_connect(
        &self,
        socket: &SocketDescriptor,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        let incoming = self
            .listeners
            .lock()
            .unwrap()
            .get(&address)
            .and_then(Weak::upgrade)
            .ok_or_else(|| io::Error::from(io::ErrorKind::ConnectionRefused))?;

        let mut state = socket.state.lock().unwrap();
        let (domain, kind, mut local) = match &*state {
            SocketState::Open {
                domain,
                kind,
                local,
            } => (*domain, *kind, *local),
            _ => return Err(io::Error::from(io::ErrorKind::InvalidInput).into()),
        };
        if kind != SocketType::Stream {
            return Err(io::Error::from(io::ErrorKind::Unsupported).into());
        }
        if SocketDomain::from(address) != domain {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        if local.port() == 0 {
            local = match domain {
                SocketDomain::Ipv4 => SocketAddr::from(([127, 0, 0, 1], 40000)),
                SocketDomain::Ipv6 => {
                    SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], 40000))
                }
            };
        }
        let input = Arc::new(Pipe::default());
        let output = Arc::new(Pipe::default());
        let server = SocketDescriptor {
            state: Mutex::new(SocketState::Connected(Connection {
                local: address,
                peer: local,
                input: output.clone(),
                output: input.clone(),
            })),
        };
        incoming.sockets.lock().unwrap().push_back(server);
        incoming.ready.notify(usize::MAX);
        *state = SocketState::Connected(Connection {
            input,
            output,
            local,
            peer: address,
        });
        Ok(())
    }

    async fn socket_accept(
        &self,
        socket: &SocketDescriptor,
        cancellation: &Cancellation,
    ) -> OperationResult<(SocketDescriptor, SocketAddr)> {
        cancellation.check()?;
        let incoming = match &*socket.state.lock().unwrap() {
            SocketState::Listening { incoming, .. } => Arc::clone(incoming),
            _ => return Err(io::Error::from(io::ErrorKind::InvalidInput).into()),
        };
        loop {
            let ready = incoming.ready.listen();
            let accepted = incoming.sockets.lock().unwrap().pop_front();
            if let Some(socket) = accepted {
                let peer = self.socket_peer_addr(&socket)?;
                return Ok((socket, peer));
            }
            wait(ready, cancellation).await?;
        }
    }

    async fn socket_wait(
        &self,
        socket: &SocketDescriptor,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        match interest {
            Interest::Writable => Ok(()),
            Interest::Readable => {
                let (input, incoming) = match &*socket.state.lock().unwrap() {
                    SocketState::Connected(connection) => {
                        (Some(Arc::clone(&connection.input)), None)
                    }
                    SocketState::Listening { incoming, .. } => (None, Some(Arc::clone(incoming))),
                    SocketState::Open { .. } => {
                        return Err(io::Error::from(io::ErrorKind::NotConnected).into());
                    }
                };
                if let Some(input) = input {
                    loop {
                        let ready = input.ready.listen();
                        {
                            let state = input.state.lock().unwrap();
                            if !state.bytes.is_empty() || state.writer_closed {
                                return Ok(());
                            }
                        }
                        wait(ready, cancellation).await?;
                    }
                }
                let incoming = incoming.unwrap();
                loop {
                    let ready = incoming.ready.listen();
                    if !incoming.sockets.lock().unwrap().is_empty() {
                        return Ok(());
                    }
                    wait(ready, cancellation).await?;
                }
            }
        }
    }

    async fn socket_read<B: IoBufMut>(
        &self,
        socket: &SocketDescriptor,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        let input = match &*socket.state.lock().unwrap() {
            SocketState::Connected(connection) => Arc::clone(&connection.input),
            _ => {
                return (
                    Err(io::Error::from(io::ErrorKind::NotConnected).into()),
                    buffer,
                );
            }
        };
        loop {
            let ready = input.ready.listen();
            {
                let mut state = input.state.lock().unwrap();
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

    async fn socket_write<B: IoBuf>(
        &self,
        socket: &SocketDescriptor,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        let output = match &*socket.state.lock().unwrap() {
            SocketState::Connected(connection) => Arc::clone(&connection.output),
            _ => {
                return (
                    Err(io::Error::from(io::ErrorKind::NotConnected).into()),
                    buffer,
                );
            }
        };
        let count = buffer.as_ref().len().min(self.write_limit);
        {
            let mut state = output.state.lock().unwrap();
            if state.reader_closed {
                return (
                    Err(io::Error::from(io::ErrorKind::BrokenPipe).into()),
                    buffer,
                );
            }
            state.bytes.extend(&buffer.as_ref()[..count]);
        }
        output.ready.notify(usize::MAX);
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
        file: &FileDescriptor,
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
        file: &FileDescriptor,
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
