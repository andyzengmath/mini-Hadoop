package block

import (
	"bytes"
	"testing"
)

func TestErasureCodec_EncodeDecodeAllPresent(t *testing.T) {
	ec := NewErasureCodec(DefaultErasurePolicy()) // RS-6-3
	data := []byte("hello erasure coding world! this is test data for RS-6-3.")

	shards, err := ec.Encode(data)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(shards) != 9 {
		t.Fatalf("expected 9 shards, got %d", len(shards))
	}

	// All shards present — should decode successfully
	result, err := ec.Decode(shards, len(data))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Error("decoded data doesn't match original")
	}
}

func TestErasureCodec_DecodeWithOneMissing(t *testing.T) {
	ec := NewErasureCodec(DefaultErasurePolicy())
	data := []byte("test data for erasure coding reconstruction with missing shards!!")

	shards, _ := ec.Encode(data)

	// Remove one data shard
	shards[2] = nil

	result, err := ec.Decode(shards, len(data))
	if err != nil {
		t.Fatalf("Decode with 1 missing: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Error("decoded data doesn't match after reconstruction")
	}
}

func TestErasureCodec_TooManyMissing(t *testing.T) {
	ec := NewErasureCodec(DefaultErasurePolicy())
	data := []byte("short test data for failure case")

	shards, _ := ec.Encode(data)

	// Remove 4 shards (more than ParityShards=3)
	shards[0] = nil
	shards[1] = nil
	shards[2] = nil
	shards[3] = nil

	_, err := ec.Decode(shards, len(data))
	if err == nil {
		t.Fatal("expected error with 4 missing shards (only 3 parity)")
	}
}

func TestErasureCodec_StorageOverhead(t *testing.T) {
	ec := NewErasureCodec(DefaultErasurePolicy())
	overhead := ec.StorageOverhead()
	if overhead != 1.5 {
		t.Errorf("expected 1.5x overhead for RS-6-3, got %f", overhead)
	}
}

func TestErasureCodec_TotalShards(t *testing.T) {
	ec := NewErasureCodec(DefaultErasurePolicy())
	if ec.TotalShards() != 9 {
		t.Errorf("expected 9 total shards, got %d", ec.TotalShards())
	}
}

func TestErasureCodec_EmptyData(t *testing.T) {
	ec := NewErasureCodec(DefaultErasurePolicy())
	_, err := ec.Encode([]byte{})
	if err == nil {
		t.Error("expected error encoding empty data")
	}
}

func TestErasureCodec_SmallData(t *testing.T) {
	ec := NewErasureCodec(DefaultErasurePolicy())
	data := []byte("hi") // Very small, less than DataShards bytes

	shards, err := ec.Encode(data)
	if err != nil {
		t.Fatalf("Encode small data: %v", err)
	}

	result, err := ec.Decode(shards, len(data))
	if err != nil {
		t.Fatalf("Decode small data: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Errorf("small data round-trip failed: got %q, want %q", result, data)
	}
}

func TestErasurePolicy_Default(t *testing.T) {
	p := DefaultErasurePolicy()
	if p.Name != "RS-6-3" {
		t.Errorf("expected RS-6-3, got %s", p.Name)
	}
	if p.DataShards != 6 || p.ParityShards != 3 {
		t.Errorf("expected 6+3, got %d+%d", p.DataShards, p.ParityShards)
	}
}

func TestBlockGroup_Structure(t *testing.T) {
	bg := BlockGroup{
		GroupID:      "bg_001",
		Policy:       DefaultErasurePolicy(),
		OriginalSize: 128 * 1024 * 1024,
		Shards: []BlockGroupShard{
			{Index: 0, BlockID: "blk_d0", NodeAddr: "node1:9001", IsData: true},
			{Index: 6, BlockID: "blk_p0", NodeAddr: "node7:9001", IsData: false},
		},
	}
	if bg.GroupID != "bg_001" {
		t.Error("wrong group ID")
	}
	if len(bg.Shards) != 2 {
		t.Errorf("expected 2 shards, got %d", len(bg.Shards))
	}
}
