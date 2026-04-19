package main

import (
	"log"

	"golang.org/x/sys/unix"
)

// Client holds all state for a single TCP connection.
// rbuf accumulates raw bytes until we have a full RESP command.
// wbuf accumulates response bytes until we can flush them.
type Client struct {
	fd   int
	rbuf []byte
	wbuf []byte
}

func NewClient(fd int) *Client {
	return &Client{
		fd:   fd,
		rbuf: make([]byte, 0, 4096),
		wbuf: make([]byte, 0, 4096),
	}
}

// appendRead drains whatever bytes are currently available on the fd
// into the client's read buffer. Returns false if the connection closed.
func (c *Client) appendRead() bool {
	tmp := make([]byte, 4096)
	n, err := unix.Read(c.fd, tmp)
	if err != nil || n == 0 {
		return false
	}
	c.rbuf = append(c.rbuf, tmp[:n]...)
	return true
}

// flushWrites attempts to drain wbuf to the socket.
// On a non-blocking fd, Write() may only consume part of wbuf if the
// kernel send buffer is full — it never blocks. When that happens we
// keep the unwritten remainder in wbuf and return true to signal that
// the caller should register EVFILT_WRITE with kqueue so we can drain
// the rest once the send buffer has room again.
// Returns true if bytes remain (write is still pending), false when done.
func (c *Client) flushWrites() bool {
	for len(c.wbuf) > 0 {
		n, err := unix.Write(c.fd, c.wbuf)
		if err != nil {
			if err == unix.EAGAIN {
				// Kernel send buffer is full. Stop here; the event loop will
				// register EVFILT_WRITE and call us again when there's room.
				return true
			}
			// Any other error means the connection is broken.
			log.Printf("fd=%d write error: %v", c.fd, err)
			return false
		}
		c.wbuf = c.wbuf[n:]
	}
	return false // wbuf fully drained
}

func (c *Client) writeString(s string) {
	c.wbuf = append(c.wbuf, []byte(s)...)
}
