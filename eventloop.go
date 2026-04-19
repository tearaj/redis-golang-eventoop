package main

import (
	"fmt"
	"log"

	"golang.org/x/sys/unix"
)

// EventLoop is the core of the server.
// One goroutine runs Run() — nothing else touches db or clients.
type EventLoop struct {
	kqfd       int             // the kqueue file descriptor itself
	events     []unix.Kevent_t // reusable buffer kqueue writes ready events into
	db         *DB
	clients    map[int]*Client // fd → client state
	listenerFd int
}

func NewEventLoop() (*EventLoop, error) {
	kqfd, err := unix.Kqueue()
	if err != nil {
		return nil, fmt.Errorf("kqueue: %w", err)
	}

	return &EventLoop{
		kqfd:    kqfd,
		events:  make([]unix.Kevent_t, 128),
		db:      NewDB(),
		clients: make(map[int]*Client),
	}, nil
}

// registerRead tells kqueue: "wake me up when this fd has data to read"
func (el *EventLoop) registerRead(fd int) error {
	var kev unix.Kevent_t
	unix.SetKevent(&kev, fd, unix.EVFILT_READ, unix.EV_ADD|unix.EV_ENABLE)
	_, err := unix.Kevent(el.kqfd, []unix.Kevent_t{kev}, nil, nil)
	return err
}

// registerWrite tells kqueue: "wake me up when this fd can accept writes".
// We only register this when flushWrites signals that the kernel send
// buffer is full. We don't want spurious EVFILT_WRITE events firing
// constantly for clients that have nothing pending.
func (el *EventLoop) registerWrite(fd int) error {
	var kev unix.Kevent_t
	unix.SetKevent(&kev, fd, unix.EVFILT_WRITE, unix.EV_ADD|unix.EV_ONESHOT)
	_, err := unix.Kevent(el.kqfd, []unix.Kevent_t{kev}, nil, nil)
	return err
}

// deregisterClient removes a client fd from kqueue and cleans up state.
func (el *EventLoop) deregisterClient(fd int) {
	// Remove both filters in case a write was pending.
	var kev unix.Kevent_t
	unix.SetKevent(&kev, fd, unix.EVFILT_READ, unix.EV_DELETE)
	unix.Kevent(el.kqfd, []unix.Kevent_t{kev}, nil, nil)
	unix.SetKevent(&kev, fd, unix.EVFILT_WRITE, unix.EV_DELETE)
	unix.Kevent(el.kqfd, []unix.Kevent_t{kev}, nil, nil)

	unix.Close(fd)
	delete(el.clients, fd)
	log.Printf("client fd=%d disconnected (total: %d)", fd, len(el.clients))
}

// acceptConnection handles a new client connecting to the listening socket.
func (el *EventLoop) acceptConnection() {
	nfd, _, err := unix.Accept(el.listenerFd)
	if err != nil {
		log.Printf("accept error: %v", err)
		return
	}

	// Non-blocking — we never want a Read() or Write() to stall the loop.
	if err := unix.SetNonblock(nfd, true); err != nil {
		unix.Close(nfd)
		return
	}

	el.clients[nfd] = NewClient(nfd)

	if err := el.registerRead(nfd); err != nil {
		log.Printf("registerRead fd=%d: %v", nfd, err)
		el.deregisterClient(nfd)
		return
	}

	log.Printf("client fd=%d connected (total: %d)", nfd, len(el.clients))
}

// handleClient is called when kqueue tells us a client fd is readable.
//  1. Drain bytes into rbuf.
//  2. Parse as many complete RESP commands as possible.
//  3. Execute each command against db, staging responses into wbuf.
//  4. Flush wbuf to the socket. If the send buffer is full, register
//     EVFILT_WRITE so we resume once there's room.
func (el *EventLoop) handleClient(fd int) {
	client := el.clients[fd]

	if !client.appendRead() {
		el.deregisterClient(fd)
		return
	}

	for {
		cmd, consumed, err := tryParseRESP(client.rbuf)
		if err != nil {
			if err == errIncomplete {
				break
			}
			log.Printf("parse error fd=%d: %v | raw: %q", fd, err, client.rbuf)
			el.deregisterClient(fd)
			return
		}

		client.rbuf = client.rbuf[consumed:]

		if cmd.Name == "" {
			continue
		}

		execute(cmd, el.db, client)
	}

	el.flushOrPark(client)
}

// handleWrite is called when kqueue fires EVFILT_WRITE — the kernel send
// buffer has room again. Resume draining whatever is left in wbuf.
func (el *EventLoop) handleWrite(fd int) {
	client := el.clients[fd]
	el.flushOrPark(client)
}

// flushOrPark tries to drain the client's wbuf. If the write blocks
// (EAGAIN), it registers EVFILT_WRITE with EV_ONESHOT so kqueue wakes
// us up exactly once when space is available, then parks the client.
func (el *EventLoop) flushOrPark(client *Client) {
	pending := client.flushWrites()
	if !pending {
		return // fully drained, nothing more to do
	}

	// wbuf still has bytes — kernel send buffer was full.
	// Register a one-shot write wakeup and mark the client as parked.
	if err := el.registerWrite(client.fd); err != nil {
		log.Printf("registerWrite fd=%d: %v", client.fd, err)
		el.deregisterClient(client.fd)
		return
	}
	log.Printf("fd=%d write parked (%d bytes pending)", client.fd, len(client.wbuf))
}

// Run is the event loop. Blocks forever on the kqueue syscall,
// wakes only when the OS has something ready for us to handle.
// This runs in a single goroutine — no locks needed anywhere.
func (el *EventLoop) Run(address string) error {
	listenerFd, _ := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	unix.SetsockoptInt(listenerFd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	unix.SetNonblock(listenerFd, true)
	unix.Bind(listenerFd, &unix.SockaddrInet4{Port: 6379})
	unix.Listen(listenerFd, unix.SOMAXCONN)
	el.listenerFd = listenerFd

	if err := el.registerRead(listenerFd); err != nil {
		return fmt.Errorf("registerRead listener: %w", err)
	}

	log.Printf("listening on %s (kqueue fd=%d)", address, el.kqfd)

	for {
		n, err := unix.Kevent(el.kqfd, nil, el.events, nil)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			return fmt.Errorf("kevent wait: %w", err)
		}

		for i := range n {
			fd := int(el.events[i].Ident)
			filter := el.events[i].Filter

			if el.events[i].Flags&unix.EV_ERROR != 0 {
				if fd != listenerFd {
					el.deregisterClient(fd)
				}
				continue
			}

			switch {
			case fd == listenerFd:
				el.acceptConnection()
			case filter == unix.EVFILT_READ:
				el.handleClient(fd)
			case filter == unix.EVFILT_WRITE:
				el.handleWrite(fd)
			}
		}
	}
}
