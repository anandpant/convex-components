#!/bin/sh
# Run in the harness build image with pinned source mounted at /upstream and the
# built plugin/exporter at /capture. Never rebuild the serving CLIProxy binary.
set -eu
cd /upstream
[ "$(git rev-parse HEAD)" = b681a1e0f7b89d26814f788b60bf84feaf72e912 ]
cp /harness/stock_hook_cost_test.go.txt internal/pluginhost/capture_measurement_test.go
trap 'rm -f /upstream/internal/pluginhost/capture_measurement_test.go' EXIT
go test ./internal/pluginhost -run '^TestCaptureStockHookCost$' -count=1 -v
