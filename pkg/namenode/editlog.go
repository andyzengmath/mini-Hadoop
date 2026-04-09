package namenode

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// EditOp represents the type of namespace mutation.
type EditOp string

const (
	OpCreateFile  EditOp = "CREATE_FILE"
	OpCompleteFile EditOp = "COMPLETE_FILE"
	OpDeleteFile  EditOp = "DELETE"
	OpMkDir       EditOp = "MKDIR"
	OpAddBlock    EditOp = "ADD_BLOCK"
)

// EditEntry is a single mutation record in the write-ahead log.
type EditEntry struct {
	Sequence  int64           `json:"seq"`
	Timestamp time.Time       `json:"ts"`
	Operation EditOp          `json:"op"`
	Path      string          `json:"path"`
	Data      json.RawMessage `json:"data,omitempty"`
}

// EditLog provides a persistent write-ahead log for namespace mutations.
// Every mutation is appended to the edit log before returning success.
// The standby NameNode replays these entries to stay in sync.
type EditLog struct {
	mu       sync.Mutex
	dir      string
	file     *os.File
	writer   *bufio.Writer
	sequence int64
}

// NewEditLog creates or opens an edit log in the given directory.
func NewEditLog(dir string) (*EditLog, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create editlog dir: %w", err)
	}

	path := filepath.Join(dir, "edits.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open editlog: %w", err)
	}

	el := &EditLog{
		dir:    dir,
		file:   f,
		writer: bufio.NewWriter(f),
	}

	// Determine next sequence number by scanning existing log
	seq, err := el.scanLastSequence()
	if err != nil {
		slog.Warn("could not scan edit log for last sequence", "error", err)
	}
	el.sequence = seq

	slog.Info("edit log opened", "path", path, "next_sequence", el.sequence)
	return el, nil
}

// Append writes a new entry to the edit log. Thread-safe.
func (el *EditLog) Append(op EditOp, path string, data interface{}) (int64, error) {
	el.mu.Lock()
	defer el.mu.Unlock()

	seq := el.sequence + 1 // tentative — only committed after successful flush

	var rawData json.RawMessage
	if data != nil {
		d, err := json.Marshal(data)
		if err != nil {
			return 0, fmt.Errorf("marshal edit data: %w", err)
		}
		rawData = d
	}

	entry := EditEntry{
		Sequence:  seq,
		Timestamp: time.Now(),
		Operation: op,
		Path:      path,
		Data:      rawData,
	}

	line, err := json.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("marshal edit entry: %w", err)
	}

	if _, err := el.writer.Write(line); err != nil {
		return 0, fmt.Errorf("write edit entry: %w", err)
	}
	if err := el.writer.WriteByte('\n'); err != nil {
		return 0, fmt.Errorf("write newline: %w", err)
	}
	if err := el.writer.Flush(); err != nil {
		return 0, fmt.Errorf("flush edit log: %w", err)
	}

	el.sequence = seq // commit only after successful flush
	return el.sequence, nil
}

// Replay reads all entries from the edit log and calls the handler for each.
// Used by the standby NameNode to catch up on missed mutations.
func (el *EditLog) Replay(handler func(entry EditEntry) error) (int64, error) {
	path := filepath.Join(el.dir, "edits.log")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("open editlog for replay: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	var count int64
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry EditEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			slog.Warn("skip malformed edit entry", "error", err)
			continue
		}

		if err := handler(entry); err != nil {
			return count, fmt.Errorf("replay entry %d: %w", entry.Sequence, err)
		}
		count++
	}

	return count, scanner.Err()
}

// ReplayFrom reads entries starting from a given sequence number.
// Returns the count of entries actually passed to handler (not skipped ones).
func (el *EditLog) ReplayFrom(fromSeq int64, handler func(entry EditEntry) error) (int64, error) {
	var count int64
	_, err := el.Replay(func(entry EditEntry) error {
		if entry.Sequence > fromSeq {
			count++
			return handler(entry)
		}
		return nil
	})
	return count, err
}

// GetSequence returns the current sequence number.
func (el *EditLog) GetSequence() int64 {
	el.mu.Lock()
	defer el.mu.Unlock()
	return el.sequence
}

// Close flushes and closes the edit log.
func (el *EditLog) Close() error {
	el.mu.Lock()
	defer el.mu.Unlock()

	if el.writer != nil {
		el.writer.Flush()
	}
	if el.file != nil {
		return el.file.Close()
	}
	return nil
}

// Truncate clears the edit log (called after a full checkpoint).
func (el *EditLog) Truncate() error {
	el.mu.Lock()
	defer el.mu.Unlock()

	if el.writer != nil {
		if err := el.writer.Flush(); err != nil {
			return fmt.Errorf("flush before truncate: %w", err)
		}
	}
	if el.file != nil {
		if err := el.file.Close(); err != nil {
			return fmt.Errorf("close before truncate: %w", err)
		}
	}

	path := filepath.Join(el.dir, "edits.log")
	f, err := os.Create(path) // truncate
	if err != nil {
		return fmt.Errorf("truncate editlog: %w", err)
	}
	el.file = f
	el.writer = bufio.NewWriter(f)
	// Keep sequence number — it only goes forward
	slog.Info("edit log truncated", "sequence", el.sequence)
	return nil
}

// scanLastSequence reads the log to find the highest sequence number.
func (el *EditLog) scanLastSequence() (int64, error) {
	path := filepath.Join(el.dir, "edits.log")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	var maxSeq int64
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry EditEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		if entry.Sequence > maxSeq {
			maxSeq = entry.Sequence
		}
	}
	return maxSeq, scanner.Err()
}
