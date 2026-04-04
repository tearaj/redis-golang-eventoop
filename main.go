package main

import (
	"log"
)

func main() {
	el, err := NewEventLoop()
	if err != nil {
		log.Fatalf("failed to create event loop: %v", err)
	}

	if err := el.Run(":6379"); err != nil {
		log.Fatalf("event loop error: %v", err)
	}
}
