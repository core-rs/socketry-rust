# Echo

These commented examples use the public `socketry` scheduler capabilities.
Run the commands from the `socketry-rust` directory, in separate terminals.

Start the server in one terminal:

```sh
cargo run --example echo-server --features signals
```

It listens on `127.0.0.1:9000`. In another terminal, send a message:

```sh
cargo run --example echo-client -- "Hello, Socketry!"
```

The client prints the echoed message and exits. Omitting the message uses
`Hello, Socketry!`. It handles partial writes and reads, and exchanges large
messages in chunks so both peers can keep making progress.

Press Ctrl-C in the server terminal to request graceful shutdown. The server
stops accepting connections and reading new data, finishes echoing data it has
already read, and awaits its connection tasks before exiting. Signal support
currently requires Unix.
