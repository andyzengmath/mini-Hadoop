#!/bin/bash
# Validate the 3 P0 fixes end-to-end (F5 + F6 scenarios).
# Before the fixes: F5 and F6 both produced 0/5 SHA match.
# After the fixes: both should produce 5/5 SHA match.

set -u
cd "$(dirname "$0")/.."

COMPOSE="docker/docker-compose.yml"
pass=0; fail=0

banner() { echo ""; echo "=== $1 ==="; }

wait_ready() {
  for i in $(seq 1 30); do
    if docker compose -f "$COMPOSE" exec -T client sh -c "curl -s http://namenode:9100/metrics 2>/dev/null | grep -q 'alive_datanodes\":3'"; then
      return 0
    fi
    sleep 2
  done
  return 1
}

check() {
  local label="$1"; local actual="$2"; local expected="$3"
  if [ "$actual" = "$expected" ]; then
    echo "  ✓ PASS: $label"
    pass=$((pass+1))
  else
    echo "  ✗ FAIL: $label (got '$actual', expected '$expected')"
    fail=$((fail+1))
  fi
}

banner "Setup: fresh cluster"
docker compose -f "$COMPOSE" down -v 2>&1 | tail -2 >&2 || true
docker compose -f "$COMPOSE" up -d 2>&1 | tail -5 >&2
wait_ready || { echo "Cluster never became ready"; exit 1; }
echo "Cluster ready."

banner "Test F5: write file, restart NN, read back"
sha_orig=$(docker compose -f "$COMPOSE" exec -T client sh -c "dd if=/dev/urandom bs=1M count=10 of=/tmp/t.dat 2>/dev/null; sha256sum /tmp/t.dat | awk '{print \$1}'" | tr -d '\r\n')
docker compose -f "$COMPOSE" exec -T client sh -c "hdfs put /tmp/t.dat /t.dat" >/dev/null 2>&1
echo "Wrote /t.dat with SHA $sha_orig"
get_sha() { docker compose -f "$COMPOSE" exec -T client sh -c "rm -f $1 2>/dev/null; hdfs get /t.dat $1 >/dev/null 2>&1; sha256sum $1 2>/dev/null | awk '{print \$1}'" | tr -d '\r\n'; }

echo "Stopping + starting namenode..."
docker compose -f "$COMPOSE" stop namenode >/dev/null 2>&1
sleep 3
docker compose -f "$COMPOSE" start namenode >/dev/null 2>&1
sleep 10
# After NN restart, wait for DNs to auto-re-register
wait_ready || echo "Warning: DNs did not re-register"
echo "Cluster resumed."

sha_after_nn=$(get_sha /tmp/r.dat)
check "F5 read SHA matches after NN restart" "$sha_after_nn" "$sha_orig"

banner "Test F6: kill 2 workers, restart them, read back"
docker compose -f "$COMPOSE" stop worker-2 worker-3 >/dev/null 2>&1
sleep 5
docker compose -f "$COMPOSE" start worker-2 worker-3 >/dev/null 2>&1
sleep 20
sha_after_w=$(get_sha /tmp/r2.dat)
check "F6 read SHA matches after worker kill+restart" "$sha_after_w" "$sha_orig"

banner "Test: new writes succeed after NN restart (bug #3 verification)"
docker compose -f "$COMPOSE" exec -T client sh -c "echo hello > /tmp/new.txt"
put_rc=$(docker compose -f "$COMPOSE" exec -T client sh -c "hdfs put /tmp/new.txt /new.txt > /dev/null 2>&1; echo \$?" | tr -d '\r\n')
check "New write after NN restart succeeds" "$put_rc" "0"

banner "Test: data persists across down-v + up (bug #2 verification)"
# Skip if no time — this requires full cluster teardown and restart
docker compose -f "$COMPOSE" down 2>&1 | tail -2 >&2
docker compose -f "$COMPOSE" up -d 2>&1 | tail -3 >&2
wait_ready || echo "Warning: cluster did not re-become ready"
sha_persist=$(get_sha /tmp/p.dat)
check "Data persists across compose down+up" "$sha_persist" "$sha_orig"

banner "Summary"
echo "Passed: $pass"
echo "Failed: $fail"

docker compose -f "$COMPOSE" down -v >/dev/null 2>&1 || true

[ "$fail" -eq 0 ] && exit 0 || exit 1
