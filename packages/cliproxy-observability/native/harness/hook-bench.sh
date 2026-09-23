#!/bin/sh
set -eu
mkdir -m 700 -p /tmp/hookbench /capture
cp /build/cliproxy-capture.so /capture/
cat > /tmp/hookbench/config.json <<'JSON'
{"enabled":true,"instanceId":"hook-bench","revision":"proof-v1","socket":"/tmp/hookbench/capture.sock","bindings":[{"key":"harness-Dev-key-0123456789","destinationId":"harness-dev","deployment":"https://harness-dev.convex.site","environment":"dev"}]}
JSON
chmod 600 /tmp/hookbench/config.json
export CLIPROXY_CAPTURE_CONFIG=/tmp/hookbench/config.json
export CAPTURE_BENCH_REPORT=/report/hook-cost.json
/build/cliproxy-capture-exporter --socket /tmp/hookbench/capture.sock --db /tmp/hookbench/outbox.db --reserve-bytes 1048576 &
exporter_pid=$!
trap 'kill "$exporter_pid" 2>/dev/null || true' EXIT
sh /harness/measure-hooks.sh
