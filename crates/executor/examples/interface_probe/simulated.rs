// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! In-memory files and duplex byte streams. No OS resources or native runtime.
//! This is an interface probe, not a model of TCP scheduling or durability.
use super::{Cancellation, FileOptions, OperationResult, SocketScheduler};
use event_listener::Event;
use socketry_executor::{AllocationMode, BufferResult, FileAllocate, SyncMode};
use socketry_executor::{
    FileClose, FileOpen, FileOperation, FileRead, FileReadAt, FileSeek, FileStat, FileSync,
    FileTruncate, FileWrite, FileWriteAt,
};
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

#[path = "simulated/address_resolution.rs"]
mod address_resolution;

type Bytes = Arc<Mutex<Vec<u8>>>;

fn available_port(
    reservations: &HashMap<SocketAddr, Weak<Reservation>>,
    mut address: SocketAddr,
) -> io::Result<u16> {
    (10000..=u16::MAX)
        .find(|port| {
            address.set_port(*port);
            !reservations.contains_key(&address)
        })
        .ok_or_else(|| io::Error::from(io::ErrorKind::AddrNotAvailable))
}

fn reserve_address(
    reservations: &mut HashMap<SocketAddr, Weak<Reservation>>,
    mut address: SocketAddr,
) -> io::Result<(SocketAddr, Arc<Reservation>)> {
    reservations.retain(|_, reservation| reservation.strong_count() > 0);
    if address.port() == 0 {
        address.set_port(available_port(reservations, address)?);
    } else if reservations.contains_key(&address) {
        return Err(io::Error::from(io::ErrorKind::AddrInUse));
    }

    let reservation = Arc::new(Reservation);
    reservations.insert(address, Arc::downgrade(&reservation));
    Ok((address, reservation))
}

pub struct Scheduler {
    names: Mutex<HashMap<String, Vec<std::net::IpAddr>>>,
    files: Mutex<HashMap<PathBuf, Bytes>>,
    reservations: Mutex<HashMap<SocketAddr, Weak<Reservation>>>,
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

struct Incoming {
    sockets: Mutex<VecDeque<SocketDescriptor>>,
    ready: Event,
    reservation: Arc<Reservation>,
}

pub struct SocketDescriptor {
    state: Mutex<SocketState>,
    options: Mutex<SocketConfiguration>,
}

// This model retains configured values, but does not model packet timing, probes, address-sharing policies, or accepted-socket inheritance.
#[derive(Default)]
struct SocketConfiguration {
    reuse_address: bool,
    keepalive: bool,
    no_delay: bool,
}

enum SocketState {
    Open {
        domain: SocketDomain,
        kind: SocketType,
        local: SocketAddr,
        reservation: Option<Arc<Reservation>>,
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
    _reservation: Option<Arc<Reservation>>,
}

struct Reservation;

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
            names: Mutex::new(HashMap::new()),
            files: Mutex::new(HashMap::new()),
            reservations: Mutex::new(HashMap::new()),
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

impl socketry_executor::SocketOptions<socketry_executor::SocketReuseAddress> for Scheduler {
    async fn socket_get_option(
        &self,
        socket: &Self::Socket,
        _option: socketry_executor::SocketReuseAddress,
        cancellation: &Cancellation,
    ) -> OperationResult<bool> {
        cancellation.check()?;
        Ok(socket.options.lock().unwrap().reuse_address)
    }
    async fn socket_set_option(
        &self,
        socket: &Self::Socket,
        _option: socketry_executor::SocketReuseAddress,
        value: bool,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        socket.options.lock().unwrap().reuse_address = value;
        Ok(())
    }
}

impl socketry_executor::SocketOptions<socketry_executor::SocketKeepalive> for Scheduler {
    async fn socket_get_option(
        &self,
        socket: &Self::Socket,
        _option: socketry_executor::SocketKeepalive,
        cancellation: &Cancellation,
    ) -> OperationResult<bool> {
        cancellation.check()?;
        Ok(socket.options.lock().unwrap().keepalive)
    }
    async fn socket_set_option(
        &self,
        socket: &Self::Socket,
        _option: socketry_executor::SocketKeepalive,
        value: bool,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        socket.options.lock().unwrap().keepalive = value;
        Ok(())
    }
}

impl socketry_executor::SocketOptions<socketry_executor::TcpNoDelay> for Scheduler {
    async fn socket_get_option(
        &self,
        socket: &Self::Socket,
        _option: socketry_executor::TcpNoDelay,
        cancellation: &Cancellation,
    ) -> OperationResult<bool> {
        cancellation.check()?;
        Ok(socket.options.lock().unwrap().no_delay)
    }
    async fn socket_set_option(
        &self,
        socket: &Self::Socket,
        _option: socketry_executor::TcpNoDelay,
        value: bool,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        socket.options.lock().unwrap().no_delay = value;
        Ok(())
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

/// This simulator models regular-file bytes, not filesystem timestamps or native identity.
pub struct FileMetadata {
    size: u64,
}

impl socketry_executor::FileMetadata for FileMetadata {
    fn file_type(&self) -> Option<socketry_executor::FileType> {
        Some(socketry_executor::FileType::File)
    }
    fn size(&self) -> Option<u64> {
        Some(self.size)
    }
    fn accessed(&self) -> Option<std::time::SystemTime> {
        None
    }
    fn modified(&self) -> Option<std::time::SystemTime> {
        None
    }
    fn created(&self) -> Option<std::time::SystemTime> {
        None
    }
}

impl socketry_executor::FileAdvice for Scheduler {
    async fn file_advise(
        &self,
        _file: &Self::File,
        offset: u64,
        length: u64,
        _advice: socketry_executor::AdviceMode,
    ) -> io::Result<()> {
        if offset
            .checked_add(length)
            .is_none_or(|end| end > i64::MAX as u64)
        {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        Ok(())
    }

    async fn file_advise_submit(
        &self,
        file: &Self::File,
        offset: u64,
        length: u64,
        advice: socketry_executor::AdviceMode,
    ) -> io::Result<()> {
        socketry_executor::FileAdvice::file_advise(self, file, offset, length, advice).await
    }
}

impl FileOperation for Scheduler {
    type File = FileDescriptor;
}

impl FileClose for Scheduler {
    async fn file_close(&self, file: Self::File) -> io::Result<()> {
        drop(file);
        Ok(())
    }

    async fn file_close_submit(&self, file: Self::File) -> io::Result<()> {
        self.file_close(file).await
    }
}

impl FileTruncate for Scheduler {
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
}

impl FileStat for Scheduler {
    type Metadata = FileMetadata;

    async fn file_stat(
        &self,
        file: &Self::File,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Metadata> {
        cancellation.check()?;
        Ok(FileMetadata {
            size: file.bytes.lock().unwrap().len() as u64,
        })
    }
}

impl FileSync for Scheduler {
    async fn file_sync(
        &self,
        _file: &Self::File,
        _mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        Ok(())
    }
}

impl FileSeek for Scheduler {
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
}

impl FileOpen for Scheduler {
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
}

impl FileRead for Scheduler {
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
}

impl FileReadAt for Scheduler {
    async fn file_read_at<B: IoBufMut>(
        &self,
        file: &FileDescriptor,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.read_file_at(file, buffer, offset, cancellation)
    }
}

impl FileWrite for Scheduler {
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
}

impl FileWriteAt for Scheduler {
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

impl SocketScheduler for Scheduler {
    type Socket = SocketDescriptor;
    async fn socket_close(&self, socket: Self::Socket) -> io::Result<()> {
        drop(socket);
        Ok(())
    }

    async fn socket_close_submit(&self, socket: Self::Socket) -> io::Result<()> {
        self.socket_close(socket).await
    }

    async fn socket_shutdown(
        &self,
        socket: &Self::Socket,
        how: std::net::Shutdown,
    ) -> io::Result<()> {
        let (input, output) = match &*socket.state.lock().unwrap() {
            SocketState::Connected(connection) => (
                Arc::clone(&connection.input),
                Arc::clone(&connection.output),
            ),
            _ => return Err(io::Error::from(io::ErrorKind::NotConnected)),
        };
        if matches!(how, std::net::Shutdown::Read | std::net::Shutdown::Both) {
            let mut state = input.state.lock().unwrap();
            state.reader_closed = true;
            state.bytes.clear();
        }
        if matches!(how, std::net::Shutdown::Write | std::net::Shutdown::Both) {
            output.state.lock().unwrap().writer_closed = true;
        }
        input.ready.notify(usize::MAX);
        output.ready.notify(usize::MAX);
        Ok(())
    }

    async fn socket_shutdown_submit(
        &self,
        socket: &Self::Socket,
        how: std::net::Shutdown,
    ) -> io::Result<()> {
        // This backend completes immediately, but submission still reports admission rather than the operation's result.
        let _ = self.socket_shutdown(socket, how).await;
        Ok(())
    }

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
            options: Mutex::new(SocketConfiguration::default()),
            state: Mutex::new(SocketState::Open {
                domain,
                kind,
                local,
                reservation: None,
            }),
        })
    }

    async fn socket_bind(
        &self,
        socket: &SocketDescriptor,
        address: SocketAddr,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        let mut state = socket.state.lock().unwrap();
        let SocketState::Open {
            domain,
            kind: SocketType::Stream,
            local,
            reservation,
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
        let (address, binding) = reserve_address(&mut self.reservations.lock().unwrap(), address)?;
        *local = address;
        *reservation = Some(binding);
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
        let (kind, mut local, mut reservation) = match &*state {
            SocketState::Open {
                kind,
                local,
                reservation,
                ..
            } => (*kind, *local, reservation.clone()),
            _ => return Err(io::Error::from(io::ErrorKind::InvalidInput).into()),
        };
        if kind != SocketType::Stream {
            return Err(io::Error::from(io::ErrorKind::Unsupported).into());
        }
        if reservation.is_none() {
            let (address, binding) =
                reserve_address(&mut self.reservations.lock().unwrap(), local)?;
            local = address;
            reservation = Some(binding);
        }
        let mut listeners = self.listeners.lock().unwrap();
        listeners.retain(|_, incoming| incoming.strong_count() > 0);
        if listeners.contains_key(&local) {
            return Err(io::Error::from(io::ErrorKind::AddrInUse).into());
        }
        let incoming = Arc::new(Incoming {
            sockets: Mutex::new(VecDeque::new()),
            ready: Event::default(),
            reservation: reservation.unwrap(),
        });
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
        let (domain, kind, mut local, mut reservation) = match &*state {
            SocketState::Open {
                domain,
                kind,
                local,
                reservation,
            } => (*domain, *kind, *local, reservation.clone()),
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
                SocketDomain::Ipv4 => SocketAddr::from(([127, 0, 0, 1], 0)),
                SocketDomain::Ipv6 => {
                    SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], 0))
                }
            };
            let (address, binding) =
                reserve_address(&mut self.reservations.lock().unwrap(), local)?;
            local = address;
            reservation = Some(binding);
        }
        let input = Arc::new(Pipe::default());
        let output = Arc::new(Pipe::default());
        let server = SocketDescriptor {
            options: Mutex::new(SocketConfiguration::default()),
            state: Mutex::new(SocketState::Connected(Connection {
                local: address,
                peer: local,
                input: output.clone(),
                output: input.clone(),
                _reservation: Some(Arc::clone(&incoming.reservation)),
            })),
        };
        incoming.sockets.lock().unwrap().push_back(server);
        incoming.ready.notify(usize::MAX);
        *state = SocketState::Connected(Connection {
            input,
            output,
            local,
            peer: address,
            _reservation: reservation,
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
                            if !state.bytes.is_empty() || state.writer_closed || state.reader_closed
                            {
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
                if count > 0
                    || buffer.as_ref().is_empty()
                    || state.writer_closed
                    || state.reader_closed
                {
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
            if state.reader_closed || state.writer_closed {
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
