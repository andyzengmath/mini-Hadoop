package block

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// CompressionCodec provides transparent block compression.
type CompressionCodec interface {
	Name() string
	Compress(src []byte) ([]byte, error)
	Decompress(src []byte) ([]byte, error)
}

// GetCodec returns a compression codec by name.
// Returns NoneCodec for empty name.
func GetCodec(name string) (CompressionCodec, error) {
	switch name {
	case "", "none":
		return &NoneCodec{}, nil
	case "gzip":
		return &GzipCodec{}, nil
	default:
		return nil, fmt.Errorf("unknown compression codec: %s", name)
	}
}

// NoneCodec is a pass-through codec (no compression).
type NoneCodec struct{}

func (c *NoneCodec) Name() string                          { return "none" }
func (c *NoneCodec) Compress(src []byte) ([]byte, error)   { return src, nil }
func (c *NoneCodec) Decompress(src []byte) ([]byte, error) { return src, nil }

// GzipCodec uses gzip compression (available in Go stdlib, no external deps).
// Good compression ratio (~3-5x on text), moderate speed.
type GzipCodec struct{}

func (c *GzipCodec) Name() string { return "gzip" }

func (c *GzipCodec) Compress(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return nil, fmt.Errorf("gzip writer: %w", err)
	}
	if _, err := w.Write(src); err != nil {
		return nil, fmt.Errorf("gzip compress: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *GzipCodec) Decompress(src []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("gzip decompress: %w", err)
	}
	return data, nil
}

// CompressedBlock wraps compressed data with metadata.
type CompressedBlock struct {
	Codec            string `json:"codec"`
	OriginalSize     int64  `json:"original_size"`
	CompressedSize   int64  `json:"compressed_size"`
	Data             []byte `json:"-"`
}

// CompressBlock compresses a block's data using the given codec.
func CompressBlock(data []byte, codecName string) (*CompressedBlock, error) {
	codec, err := GetCodec(codecName)
	if err != nil {
		return nil, err
	}

	compressed, err := codec.Compress(data)
	if err != nil {
		return nil, err
	}

	return &CompressedBlock{
		Codec:          codec.Name(),
		OriginalSize:   int64(len(data)),
		CompressedSize: int64(len(compressed)),
		Data:           compressed,
	}, nil
}

// DecompressBlock decompresses a compressed block.
func DecompressBlock(cb *CompressedBlock) ([]byte, error) {
	codec, err := GetCodec(cb.Codec)
	if err != nil {
		return nil, err
	}
	return codec.Decompress(cb.Data)
}
