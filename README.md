# Go Event Loop with Kqueue and Simple Redis

This project is a hands-on learning exercise to understand and implement an event loop from scratch in Go, utilizing the `kqueue` system call for efficient I/O multiplexing. Alongside the custom event loop, it features a basic, in-memory Redis-like server implementation that handles a subset of Redis commands.

## Why this Project?

The primary goal of this project is to demystify how event loops work by building one directly using operating system primitives (`kqueue` on BSD/macOS). It serves as an educational tool to:

- **Understand Event Loops:** Gain an  understanding of non-blocking I/O and event-driven programming.
- **Kqueue Mechanics:** Learn how to interact with the `kqueue` system call for monitoring multiple file descriptors for events (readiness for I/O).
- **Go's `sys/unix` Package:** Explore the `golang.org/x/sys/unix` package for low-level system programming.
- **Single-Threaded Event Model:** Observe how a single goroutine can efficiently handle many concurrent client connections without explicit locking, as all state modifications happen within the event loop's context.

## How it Works (Technical Deep Dive)
- Writing in progress 

## Supported Redis Commands

The simple Redis implementation supports the following commands:
- `SET <key> <value>`: Stores a string value associated with a key.
- `GET <key>`: Retrieves the string value for a given key. Returns a null bulk string if the key does not exist.
- `DEL <key> [<key> ...]`: Deletes one or more keys. Returns the number of keys removed.
- `PING`: Responds with `+PONG`.

## Getting Started

To run this project:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/event-loop.git
   cd event-loop
   ```
2. **Build the application:**
   ```bash
   go build -o redis-eventloop .
   ```
3. **Run the server:**
   ```bash
   ./redis-eventloop
   ```
   The server will start listening on `localhost:6379`.

4. **Connect with `redis-cli`:**
   You can connect using the standard Redis CLI:
   ```bash
   redis-cli
   ```
   Then try the supported commands:
   ```
   127.0.0.1:6379> SET mykey "Hello, Event Loop!"
   OK
   127.0.0.1:6379> GET mykey
   "Hello, Event Loop!"
   127.0.0.1:6379> DEL mykey
   (integer) 1
   127.0.0.1:6379> PING
   PONG
   ```

## Project Structure
- `main.go`: Entry point for the application, initializes and runs the `EventLoop`.
- `eventloop.go`: Contains the core `EventLoop` implementation, `kqueue` interaction, connection handling, and event dispatching.
- `client.go`: Defines the `Client` struct, managing client-specific read/write buffers.
- `parser.go`: Implements the parsing logic for the Redis Serialization Protocol (RESP).
- `commands.go`: Contains the `execute` function, which dispatches and handles the supported Redis commands.
- `db.go`: Provides a simple in-memory key-value store for the Redis data.
- `go.mod`, `go.sum`: Go module files.


## Note:
- This only works with MacOS and UNIX systems that support kqueue. It is not written to support epoll calls `¯\_(ツ)_/¯`.
