package main

import (
	"fmt"
	"strings"
)

// execute runs a command against the db and stages the RESP response
// into the client's write buffer. Everything here is plain memory ops.
func execute(cmd Command, db *DB, client *Client) {
	switch strings.ToUpper(cmd.Name) {
	case "SET":
		if len(cmd.Args) < 2 {
			client.writeString("-ERR wrong number of arguments for SET\r\n")
			return
		}
		db.Set(cmd.Args[0], cmd.Args[1])
		client.writeString("+OK\r\n")

	case "GET":
		if len(cmd.Args) < 1 {
			client.writeString("-ERR wrong number of arguments for GET\r\n")
			return
		}
		val, ok := db.Get(cmd.Args[0])
		if !ok {
			client.writeString("$-1\r\n") // RESP null bulk string
			return
		}
		// Bulk string: $<len>\r\n<value>\r\n
		client.writeString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))

	case "DEL":
		if len(cmd.Args) < 1 {
			client.writeString("-ERR wrong number of arguments for DEL\r\n")
			return
		}
		count := 0
		for _, key := range cmd.Args {
			if db.Del(key) {
				count++
			}
		}
		client.writeString(fmt.Sprintf(":%d\r\n", count)) // RESP integer

	case "PING":
		client.writeString("+PONG\r\n")

	case "COMMAND":
		// redis-cli sends this on startup — just acknowledge it
		client.writeString("*0\r\n")

	case "CONFIG":
		// redis-benchmark sends CONFIG GET save and CONFIG GET appendonly
		// on startup to check persistence settings. Return empty array —
		// we are an in-memory server with no persistence to report.
		client.writeString("*0\r\n")

	default:
		client.writeString(fmt.Sprintf("-ERR unknown command '%s'\r\n", cmd.Name))
	}
}
