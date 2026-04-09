package mapreduce

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// TextInputFormat reads lines from a file, emitting (line_offset, line_text) pairs.
type TextInputFormat struct{}

// ReadSplit reads a file and calls the mapper for each line.
func (f *TextInputFormat) ReadSplit(path string, mapper Mapper, emit func(key, value string)) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer file.Close()

	return f.ReadFromReader(file, mapper, emit)
}

// ReadFromReader reads lines from a reader and calls the mapper for each line.
func (f *TextInputFormat) ReadFromReader(r io.Reader, mapper Mapper, emit func(key, value string)) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	offset := 0
	for scanner.Scan() {
		line := scanner.Text()
		mapper.Map(fmt.Sprintf("%d", offset), line, emit)
		offset += len(line) + 1 // +1 for newline
	}
	return scanner.Err()
}

// TextOutputFormat writes key-value pairs as tab-separated lines.
type TextOutputFormat struct{}

// WriteOutput writes key-value pairs to a file.
func (f *TextOutputFormat) WriteOutput(path string, kvs []KeyValue) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, kv := range kvs {
		fmt.Fprintf(w, "%s\t%s\n", kv.Key, kv.Value)
	}
	return w.Flush()
}

// WriteToWriter writes key-value pairs to a writer.
func (f *TextOutputFormat) WriteToWriter(w io.Writer, key, value string) error {
	_, err := fmt.Fprintf(w, "%s\t%s\n", key, value)
	return err
}

// ParseLine splits a tab-separated line into key and value.
func ParseLine(line string) (string, string) {
	parts := strings.SplitN(line, "\t", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return line, ""
}
