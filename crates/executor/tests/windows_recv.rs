//! Temporary diagnosis of concurrent receives, independent of socketry's executor.

use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

fn pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();
    client.set_nonblocking(true).unwrap();
    (client, server)
}

#[test]
fn standard_library_concurrent_reads() {
    for iteration in 0..1_000 {
        let (client, mut server) = pair();
        let values = thread::scope(|scope| {
            let mut readers = Vec::new();
            for _ in 0..4 {
                let mut client = &client;
                readers.push(scope.spawn(move || {
                    let mut values = Vec::new();
                    for _ in 0..4 {
                        let mut buffer = [0];
                        loop {
                            match client.read(&mut buffer) {
                                Ok(1) => {
                                    values.push(buffer[0]);
                                    break;
                                }
                                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                                    thread::yield_now()
                                }
                                result => panic!("unexpected recv result: {result:?}"),
                            }
                        }
                    }
                    values
                }));
            }
            server.write_all(&(0..16u8).collect::<Vec<_>>()).unwrap();
            readers
                .into_iter()
                .flat_map(|reader| reader.join().unwrap())
                .collect::<Vec<_>>()
        });
        let mut values = values;
        values.sort_unstable();
        assert_eq!(
            values,
            (0..16u8).collect::<Vec<_>>(),
            "iteration {iteration}"
        );
    }
}

#[test]
fn async_io_concurrent_reads() {
    for iteration in 0..1_000 {
        let (client, mut server) = pair();
        let client = async_io::Async::new(socket2::Socket::from(client)).unwrap();
        let values = thread::scope(|scope| {
            let mut readers = Vec::new();
            for _ in 0..4 {
                let client = &client;
                readers.push(scope.spawn(move || {
                    let mut values = Vec::new();
                    for _ in 0..4 {
                        let mut buffer = [std::mem::MaybeUninit::new(0)];
                        let size = futures_lite::future::block_on(
                            client.read_with(|socket| socket.recv(&mut buffer)),
                        )
                        .unwrap();
                        assert_eq!(size, 1);
                        // SAFETY: The successful receive initialized this byte.
                        values.push(unsafe { buffer[0].assume_init() });
                    }
                    values
                }));
            }
            server.write_all(&(0..16u8).collect::<Vec<_>>()).unwrap();
            readers
                .into_iter()
                .flat_map(|reader| reader.join().unwrap())
                .collect::<Vec<_>>()
        });
        let mut values = values;
        values.sort_unstable();
        assert_eq!(
            values,
            (0..16u8).collect::<Vec<_>>(),
            "iteration {iteration}"
        );
    }
}
