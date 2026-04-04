package main

import (
	"bytes"
	"errors"
	"strconv"
)

// Command is a parsed Redis command e.g. {Name: "SET", Args: ["foo", "bar"]}
type Command struct {
	Name string
	Args []string
}

var errIncomplete = errors.New("incomplete")
var errInvalid = errors.New("invalid RESP")

// tryParseRESP attempts to parse one complete RESP command from buf.
//
// RESP format for an array (what Redis clients send):
//   *<count>\r\n
//   $<len>\r\n<value>\r\n
//   $<len>\r\n<value>\r\n
//   ...
//
// Returns (cmd, bytesConsumed, nil) on success.
// Returns (_, 0, errIncomplete) if more bytes are needed — caller should wait.
// Returns (_, 0, errInvalid) on a protocol error.
func tryParseRESP(buf []byte) (Command, int, error) {
	if len(buf) == 0 {
		return Command{}, 0, errIncomplete
	}

	if buf[0] != '*' {
		return Command{}, 0, errInvalid
	}

	pos := 0

	// Read array length: *<n>\r\n
	count, n, err := readInt(buf, pos+1)
	if err != nil {
		return Command{}, 0, err
	}
	pos = n

	parts := make([]string, 0, count)

	for i := 0; i < count; i++ {
		// Each element: $<len>\r\n<value>\r\n
		if pos >= len(buf) || buf[pos] != '$' {
			return Command{}, 0, errIncomplete
		}

		length, n, err := readInt(buf, pos+1)
		if err != nil {
			return Command{}, 0, err
		}
		pos = n

		// Do we have <length> bytes + \r\n available?
		if pos+length+2 > len(buf) {
			return Command{}, 0, errIncomplete
		}

		parts = append(parts, string(buf[pos:pos+length]))
		pos += length + 2 // skip value + \r\n
	}

	if len(parts) == 0 {
		return Command{}, 0, errInvalid
	}

	cmd := Command{
		Name: parts[0],
		Args: parts[1:],
	}
	return cmd, pos, nil
}

// readInt reads an integer followed by \r\n starting at buf[start].
// Returns (value, newPos, err).
func readInt(buf []byte, start int) (int, int, error) {
	end := bytes.Index(buf[start:], []byte("\r\n"))
	if end == -1 {
		return 0, 0, errIncomplete
	}
	n, err := strconv.Atoi(string(buf[start : start+end]))
	if err != nil {
		return 0, 0, errInvalid
	}
	return n, start + end + 2, nil // +2 to skip \r\n
}
