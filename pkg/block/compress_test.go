package block

import (
	"bytes"
	"strings"
	"testing"
)

func TestNoneCodec_RoundTrip(t *testing.T) {
	codec := &NoneCodec{}
	data := []byte("hello world")
	compressed, err := codec.Compress(data)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if !bytes.Equal(compressed, data) {
		t.Error("NoneCodec should return data unchanged")
	}
	decompressed, err := codec.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(decompressed, data) {
		t.Error("round-trip failed")
	}
}

func TestGzipCodec_RoundTrip(t *testing.T) {
	codec := &GzipCodec{}
	data := []byte(strings.Repeat("hadoop distributed computing framework ", 1000))

	compressed, err := codec.Compress(data)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}

	// Compressed should be significantly smaller for repetitive text
	ratio := float64(len(compressed)) / float64(len(data))
	t.Logf("compression: %d -> %d bytes (%.1f%% of original)", len(data), len(compressed), ratio*100)
	if ratio > 0.5 {
		t.Errorf("expected >50%% compression on repetitive text, got %.1f%%", ratio*100)
	}

	decompressed, err := codec.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(decompressed, data) {
		t.Error("round-trip failed: decompressed != original")
	}
}

func TestGetCodec(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"", false},
		{"none", false},
		{"gzip", false},
		{"unknown", true},
	}
	for _, tt := range tests {
		codec, err := GetCodec(tt.name)
		if tt.wantErr && err == nil {
			t.Errorf("GetCodec(%q): expected error", tt.name)
		}
		if !tt.wantErr && err != nil {
			t.Errorf("GetCodec(%q): unexpected error: %v", tt.name, err)
		}
		if !tt.wantErr && codec == nil {
			t.Errorf("GetCodec(%q): expected non-nil codec", tt.name)
		}
	}
}

func TestCompressBlock(t *testing.T) {
	data := []byte(strings.Repeat("test data for compression ", 500))

	cb, err := CompressBlock(data, "gzip")
	if err != nil {
		t.Fatalf("CompressBlock: %v", err)
	}
	if cb.Codec != "gzip" {
		t.Errorf("codec = %q, want gzip", cb.Codec)
	}
	if cb.OriginalSize != int64(len(data)) {
		t.Errorf("original size = %d, want %d", cb.OriginalSize, len(data))
	}
	if cb.CompressedSize >= cb.OriginalSize {
		t.Error("compressed should be smaller than original for repetitive data")
	}

	decompressed, err := DecompressBlock(cb)
	if err != nil {
		t.Fatalf("DecompressBlock: %v", err)
	}
	if !bytes.Equal(decompressed, data) {
		t.Error("CompressBlock/DecompressBlock round-trip failed")
	}
}

func TestCompressBlock_None(t *testing.T) {
	data := []byte("small data")
	cb, err := CompressBlock(data, "none")
	if err != nil {
		t.Fatalf("CompressBlock none: %v", err)
	}
	if cb.CompressedSize != cb.OriginalSize {
		t.Error("none codec should not change size")
	}
}
