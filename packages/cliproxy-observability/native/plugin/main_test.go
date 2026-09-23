package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestStockSchema6Registration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scope.json")
	os.WriteFile(path, []byte(`{"enabled":false,"instanceId":"test","revision":"r1","socket":"/tmp/test-capture/capture.sock"}`), 0600)
	t.Setenv("CLIPROXY_CAPTURE_CONFIG", path)
	defer cliproxyPluginShutdown()
	for _, schema := range []string{`{"schema_version":5}`, `{"schema_version":7}`} {
		if _, ok := dispatch("plugin.register", []byte(schema), false); ok {
			t.Fatal("unsupported schema accepted")
		}
	}
	v, ok := dispatch("plugin.register", []byte(`{"schema_version":6}`), false)
	if !ok {
		t.Fatal("registration failed")
	}
	raw, _ := json.Marshal(v)
	// Match the stock rpcRegistration wire names, including its non-obvious stream capability.
	var r struct {
		Schema       int `json:"schema_version"`
		Metadata     struct{ Name, Version, Author, GitHubRepository string }
		Capabilities struct {
			Request    bool `json:"request_interceptor"`
			Response   bool `json:"response_interceptor"`
			Stream     bool `json:"response_stream_interceptor"`
			Completion bool `json:"request_lifecycle_plugin"`
		}
	}
	json.Unmarshal(raw, &r)
	if r.Schema != 6 || r.Metadata.Name == "" || r.Metadata.Version == "" || r.Metadata.Author == "" || r.Metadata.GitHubRepository == "" || !r.Capabilities.Request || !r.Capabilities.Response || !r.Capabilities.Stream || !r.Capabilities.Completion {
		t.Fatalf("invalid registration: %s", raw)
	}
}
func TestCallbacksNeverRewriteInference(t *testing.T) {
	for _, method := range []string{"request.intercept_before", "request.intercept_after", "response.intercept_after", "response.intercept_stream_chunk", "request.complete"} {
		v, ok := dispatch(method, nil, false)
		if !ok {
			t.Fatal(method)
		}
		raw, _ := json.Marshal(v)
		var m map[string]json.RawMessage
		json.Unmarshal(raw, &m)
		for k := range m {
			if k != "ClearHeaders" {
				t.Fatalf("%s rewrites %s", method, k)
			}
		}
	}
}
