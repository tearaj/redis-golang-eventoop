package main

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
)

type Command struct {
	Name string
	Args []string
}

var errIncomplete = errors.New("incomplete")
var errInvalid = errors.New("invalid RESP")

// tryParseRESP handles two formats redis-cli and redis-benchmark send:
//
//  1. RESP array (standard client format):
//     *2\r\n$4\r\nPING\r\n...
//
//  2. Inline command (redis-benchmark and plain telnet):
//     PING\r\n
//     CONFIG GET save\r\n
//
// Returns (cmd, bytesConsumed, nil) on success.
// Returns (_, 0, errIncomplete) if more bytes needed.
// Returns (_, 0, errInvalid) on unrecoverable protocol error.
func tryParseRESP(buf []byte) (Command, int, error) {
	if len(buf) == 0 {
		return Command{}, 0, errIncomplete
	}

	if buf[0] == '*' {
		return parseArray(buf)
	}

	// Anything else: try inline format
	return parseInline(buf)
}

// parseArray handles *<n>\r\n$<len>\r\n<value>\r\n...
func parseArray(buf []byte) (Command, int, error) {
	pos := 0

	count, n, err := readInt(buf, pos+1)
	if err != nil {
		return Command{}, 0, err
	}
	pos = n

	parts := make([]string, 0, count)
	for range count {
		if pos >= len(buf) {
			return Command{}, 0, errIncomplete
		}
		if buf[pos] != '$' {
			return Command{}, 0, errInvalid
		}

		length, n, err := readInt(buf, pos+1)
		if err != nil {
			return Command{}, 0, err
		}
		pos = n

		if pos+length+2 > len(buf) {
			return Command{}, 0, errIncomplete
		}

		parts = append(parts, string(buf[pos:pos+length]))
		pos += length + 2
	}

	if len(parts) == 0 {
		return Command{}, 0, errInvalid
	}

	return Command{Name: parts[0], Args: parts[1:]}, pos, nil
}

// parseInline handles plain text commands: PING\r\n or CONFIG GET save\r\n
// redis-benchmark uses this format for its startup handshake.
func parseInline(buf []byte) (Command, int, error) {
	idx := bytes.Index(buf, []byte("\r\n"))
	if idx == -1 {
		// No \r\n yet — might also be just \n (some clients)
		idx = bytes.IndexByte(buf, '\n')
		if idx == -1 {
			return Command{}, 0, errIncomplete
		}
	}

	line := strings.TrimSpace(string(buf[:idx]))
	consumed := idx + 2
	if buf[idx] == '\n' {
		consumed = idx + 1
	}

	if line == "" {
		// blank line — skip it, not an error
		return Command{Name: "", Args: nil}, consumed, nil
	}

	parts := strings.Fields(line)
	return Command{Name: parts[0], Args: parts[1:]}, consumed, nil
}

func readInt(buf []byte, start int) (int, int, error) {
	end := bytes.Index(buf[start:], []byte("\r\n"))
	if end == -1 {
		return 0, 0, errIncomplete
	}
	n, err := strconv.Atoi(string(buf[start : start+end]))
	if err != nil {
		return 0, 0, errInvalid
	}
	return n, start + end + 2, nil
}
