package main

import (
	"encoding/json"
	"fmt"
	"github.com/anandpant/convex-components/cliproxy-capture/internal/capture"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func registrationConfig(t *testing.T) {
	t.Helper()
	cliproxyPluginShutdown()
	t.Cleanup(cliproxyPluginShutdown)
	path := filepath.Join(t.TempDir(), "scope.json")
	if err := os.WriteFile(path, []byte(`{"enabled":false,"instanceId":"test","revision":"r1","socket":"/tmp/test-capture/capture.sock"}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLIPROXY_CAPTURE_CONFIG", path)
}

func assertRegistration(t *testing.T, v any) {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	// Match stock rpcRegistration wire names, including its stream capability.
	var r struct {
		Schema       int `json:"schema_version"`
		Metadata     struct{ Name, Version, Author, GitHubRepository string }
		Capabilities map[string]bool
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	// Never opt out of request bodies or into raw body patching.
	want := map[string]bool{"request_interceptor": true, "response_interceptor": true, "response_stream_interceptor": true, "request_lifecycle_plugin": true}
	if r.Schema != 6 || r.Metadata.Name == "" || r.Metadata.Version != capture.Version || r.Metadata.Author == "" || r.Metadata.GitHubRepository == "" || !reflect.DeepEqual(r.Capabilities, want) {
		t.Fatalf("invalid registration: %s", raw)
	}
}

func TestHostSchemaNegotiation(t *testing.T) {
	for _, method := range []string{"plugin.register", "plugin.reconfigure"} {
		for _, schema := range []uint32{6, 7, 8, 9, 10, 11, 42, 4294967295} {
			t.Run(fmt.Sprintf("%s/%d", method, schema), func(t *testing.T) {
				registrationConfig(t)
				// Stock sends its maximum schema, not the plugin's selected schema.
				raw := []byte(fmt.Sprintf(`{"schema_version":%d,"config_yaml":"","multi_auth_providers":true}`, schema))
				v, ok := dispatch(method, raw)
				if !ok || engine == nil {
					t.Fatal("registration failed")
				}
				assertRegistration(t, v)
				original := engine
				v, ok = dispatch("plugin.reconfigure", raw)
				if !ok || engine != original {
					t.Fatal("reconfigure replaced the existing capture engine")
				}
				assertRegistration(t, v)
			})
		}
	}
}

func TestInvalidRegistration(t *testing.T) {
	for _, method := range []string{"plugin.register", "plugin.reconfigure"} {
		for _, raw := range []string{"", "{", "null", "[]", "{}", `{"schema_version":null}`, `{"schema_version":"10"}`, `{"schema_version":true}`, `{"schema_version":6.5}`, `{"schema_version":-1}`, `{"schema_version":0}`, `{"schema_version":5}`, `{"schema_version":4294967296}`, `{"schema_version":10} {}`} {
			t.Run(method+"/"+raw, func(t *testing.T) {
				registrationConfig(t)
				if _, ok := dispatch(method, []byte(raw)); ok || engine != nil {
					t.Fatal("invalid registration initialized capture")
				}
				if _, ok := dispatch("plugin.register", []byte(`{"schema_version":6}`)); !ok {
					t.Fatal("valid registration failed")
				}
				original := engine
				if _, ok := dispatch(method, []byte(raw)); ok || engine != original {
					t.Fatal("invalid registration changed existing capture")
				}
			})
		}
	}
}

func TestRegistrationRequiresPrivateConfig(t *testing.T) {
	for _, method := range []string{"plugin.register", "plugin.reconfigure"} {
		for _, config := range []string{"missing", "malformed", "public"} {
			t.Run(method+"/"+config, func(t *testing.T) {
				registrationConfig(t)
				path := os.Getenv("CLIPROXY_CAPTURE_CONFIG")
				var err error
				switch config {
				case "missing":
					err = os.Remove(path)
				case "malformed":
					err = os.WriteFile(path, []byte("{"), 0600)
				case "public":
					err = os.Chmod(path, 0644)
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := dispatch(method, []byte(`{"schema_version":10}`)); ok || engine != nil {
					t.Fatal("invalid private configuration initialized capture")
				}
			})
		}
	}
}

func TestCallbacksNeverRewriteInference(t *testing.T) {
	for _, method := range []string{"request.intercept_before", "request.intercept_after", "response.intercept_after", "response.intercept_stream_chunk", "request.complete"} {
		v, ok := dispatch(method, nil)
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
