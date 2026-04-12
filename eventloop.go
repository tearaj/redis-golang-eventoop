package main

import (
	"fmt"
	"golang.org/x/sys/unix"
	"log"
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

// deregister removes a client fd from kqueue and cleans up state
func (el *EventLoop) deregisterClient(fd int) {
	var kev unix.Kevent_t
	unix.SetKevent(&kev, fd, unix.EVFILT_READ, unix.EV_DELETE)
	unix.Kevent(el.kqfd, []unix.Kevent_t{kev}, nil, nil)
	unix.Close(fd)
	delete(el.clients, fd)
	log.Printf("client fd=%d disconnected (total: %d)", fd, len(el.clients))
}

// acceptConnection handles a new client connecting to the listening socket.
// We get the raw fd from the net.Listener so we can register it with kqueue.
func (el *EventLoop) acceptConnection() {
	// Accept at the syscall level to get a raw fd
	nfd, _, err := unix.Accept(el.listenerFd)
	if err != nil {
		log.Printf("accept error: %v", err)
		return
	}

	// Set non-blocking — important: we never want a Read() to block the loop
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
// 1. Drain bytes into rbuf
// 2. Parse as many complete RESP commands as possible
// 3. Execute each command against db
// 4. Flush responses back to the client
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
				log.Println("incomplete read", cmd, consumed, err)
				break
			}
			// Log the raw bytes so we can see what broke the parser
			log.Printf("parse error fd=%d: %v | raw: %q", fd, err, client.rbuf)
			el.deregisterClient(fd)
			return
		}
		log.Printf("Parsing successful: %v", cmd)

		client.rbuf = client.rbuf[consumed:]

		// Skip blank lines that inline parsing may emit
		if cmd.Name == "" {
			continue
		}
		execute(cmd, el.db, client)
	}

	client.flushWrites()
}

// Run is the event loop. Blocks forever on the kqueue syscall,
// wakes only when the OS has something ready for us to handle.
// This runs in a single goroutine — no locks needed anywhere.
func (el *EventLoop) Run(address string) error {
	// Set up the TCP listener using the standard library,
	// then extract the raw listenerFd so we can register it with kqueue.
	listenerFd, _ := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	unix.SetsockoptInt(listenerFd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	unix.SetNonblock(listenerFd, true)
	unix.Bind(listenerFd, &unix.SockaddrInet4{Port: 6379})
	unix.Listen(listenerFd, unix.SOMAXCONN)
	el.listenerFd = listenerFd

	// Register the listening socket — EVFILT_READ on a listener means
	// "a new client is waiting to be accepted"
	if err := el.registerRead(listenerFd); err != nil {
		return fmt.Errorf("registerRead listener: %w", err)
	}

	log.Printf("listening on %s (kqueue fd=%d)", address, el.kqfd)

	for {
		// Blocks here. OS wakes us up only when ≥1 fd is ready.
		// el.events is filled with ready events — n tells us how many.
		n, err := unix.Kevent(el.kqfd, nil, el.events, nil)
		if err != nil {
			if err == unix.EINTR {
				continue // signal interrupted the syscall, just retry
			}
			return fmt.Errorf("kevent wait: %w", err)
		}

		for i := range n {
			fd := int(el.events[i].Ident)

			if el.events[i].Flags&unix.EV_ERROR != 0 {
				// kqueue itself reported an error on this fd
				if fd != listenerFd {
					el.deregisterClient(fd)
				}
				continue
			}

			if fd == listenerFd {
				el.acceptConnection()
			} else {
				el.handleClient(fd)
			}
		}
	}
}
