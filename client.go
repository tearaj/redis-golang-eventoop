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

// flushWrites attempts to send everything in wbuf to the client.
// For now we do a simple Write — partial write handling would register
// EVFILT_WRITE with kqueue (explained in eventloop.go).
func (c *Client) flushWrites() {
	if len(c.wbuf) == 0 {
		return
	}
	_, err := unix.Write(c.fd, c.wbuf)
	if err != nil {
		log.Printf("Error while write to buffer: %v\n", err)
	}
	c.wbuf = c.wbuf[:0]
}

func (c *Client) writeString(s string) {
	c.wbuf = append(c.wbuf, []byte(s)...)
}
