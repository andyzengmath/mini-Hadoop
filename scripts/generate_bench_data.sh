#!/bin/bash
# Generate deterministic benchmark data files
# Usage: ./scripts/generate_bench_data.sh [output_dir]

OUTPUT_DIR="${1:-/tmp/mini-hadoop-bench}"
mkdir -p "$OUTPUT_DIR"

echo "Generating benchmark data in $OUTPUT_DIR..."

# Seed words for WordCount
WORDS="the quick brown fox jumps over lazy dog hadoop distributed computing system block replication mapreduce namenode datanode resource manager container shuffle"

# --- Small files for metadata benchmark ---
echo "  Small files (1000 × 1KB)..."
mkdir -p "$OUTPUT_DIR/small-files"
for i in $(seq 1 1000); do
  echo "file $i: $WORDS" > "$OUTPUT_DIR/small-files/file-$(printf '%04d' $i).txt"
done

# --- WordCount inputs of various sizes ---
generate_text() {
  local target_mb=$1
  local output=$2
  local target_bytes=$((target_mb * 1024 * 1024))
  local bytes=0

  echo "  WordCount input: ${target_mb}MB → $output"
  > "$output"

  while [ $bytes -lt $target_bytes ]; do
    # Deterministic but varied text
    echo "$WORDS $WORDS $WORDS" >> "$output"
    echo "the fox and the dog play in the hadoop cluster" >> "$output"
    echo "quick brown distributed computing over lazy namenode" >> "$output"
    echo "block replication shuffle reduce map datanode manager" >> "$output"
    bytes=$(wc -c < "$output")
  done
}

generate_text 10 "$OUTPUT_DIR/wordcount-10mb.txt"
generate_text 100 "$OUTPUT_DIR/wordcount-100mb.txt"
generate_text 500 "$OUTPUT_DIR/wordcount-500mb.txt"

# --- SumByKey input ---
echo "  SumByKey input (10M records, 100K keys)..."
{
  i=0
  while [ $i -lt 10000000 ]; do
    key="key-$(printf '%05d' $((i % 100000)))"
    val=$((i % 1000 + 1))
    echo -e "${key}\t${val}"
    i=$((i + 1))
  done
} > "$OUTPUT_DIR/sumbykey-10m.txt" 2>/dev/null

# If the loop above is too slow in sh, use a faster approach
if [ ! -s "$OUTPUT_DIR/sumbykey-10m.txt" ]; then
  echo "  (Using faster awk generator...)"
  awk 'BEGIN {
    for (i = 0; i < 10000000; i++) {
      printf "key-%05d\t%d\n", i % 100000, i % 1000 + 1
    }
  }' > "$OUTPUT_DIR/sumbykey-10m.txt"
fi

# --- Binary test files for SHA-256 verification ---
echo "  Binary files for integrity tests..."
dd if=/dev/urandom bs=1M count=10 of="$OUTPUT_DIR/random-10mb.dat" 2>/dev/null
dd if=/dev/urandom bs=1M count=100 of="$OUTPUT_DIR/random-100mb.dat" 2>/dev/null

# --- Summary ---
echo ""
echo "Generated files:"
ls -lh "$OUTPUT_DIR"/*.txt "$OUTPUT_DIR"/*.dat 2>/dev/null
echo ""
echo "Small files: $(ls "$OUTPUT_DIR/small-files/" | wc -l) files"
echo "Done."
