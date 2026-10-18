// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Foundational concurrency APIs for Socketry.
#![doc = include_str!("../readme.md")]

pub use socketry_executor as executor;
pub use socketry_executor::{
    AddressHints, AddressInfo, AddressResolution, AddressResolveError, AddressResolveErrorKind,
    AddressService, AdviceMode, AllocationMode, Blocking, BlockingError, BlockingOptions,
    BlockingTask, BufferResult, BufferSlice, Cancellation, Cancelled, Clock, FileAdvice,
    FileAllocate, FileClose, FileDescriptor, FileMetadata, FileOpen, FileOperation, FileOptions,
    FileRead, FileReadAt, FileScheduler, FileSeek, FileStat, FileSync, FileTruncate, FileType,
    FileWrite, FileWriteAt, Interest, IoBuf, IoBufMut, Notify, OperationError, OperationResult,
    Scheduler, SchedulerHandle, Signals, SocketAccept, SocketBind, SocketClose, SocketConnect,
    SocketDomain, SocketGetOption, SocketKeepalive, SocketListen, SocketLocalAddr, SocketMessage,
    SocketMessages, SocketOpen, SocketOperation, SocketOption, SocketOptions, SocketPeerAddr,
    SocketProtocol, SocketRead, SocketRecvFlags, SocketRecvMsg, SocketRecvResult,
    SocketRegistration, SocketReuseAddress, SocketScheduler, SocketSendFlags, SocketSendMsg,
    SocketSetOption, SocketShutdown, SocketType, SocketWait, SocketWrite, Spawn, SpawnError,
    SyncMode, Task, TaskError, TaskHandle, TcpNoDelay, defer_cancel, yield_now,
};

#[cfg(all(unix, feature = "signals", any(feature = "native", feature = "tokio")))]
pub use socketry_executor::SignalRegistration;
#[cfg(all(unix, feature = "signals"))]
pub use socketry_executor::signal;

pub use socketry_timers as timers;

pub use socketry_executor::scheduler;

#[cfg(feature = "native")]
pub use socketry_executor::SocketDescriptor;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use socketry_executor::StatxMetadata;
pub use socketry_executor::SystemFileMetadata;
