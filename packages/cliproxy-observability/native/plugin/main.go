// Native ABI definitions adapted from CLIProxyAPI v7.3.5 (MIT).
package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	void* call;
	void* free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);
*/
import "C"

import (
	"encoding/json"
	"github.com/anandpant/convex-components/cliproxy-capture/internal/capture"
	"os"
	"sync"
	"unsafe"
)

var mu sync.RWMutex
var engine *capture.Engine

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if host == nil || host.abi_version != 1 || plugin == nil {
		return 1
	}
	plugin.abi_version = 1
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, n C.size_t, response *C.cliproxy_buffer) C.int {
	if response == nil {
		return 1
	}
	response.ptr = nil
	response.len = 0
	if method == nil {
		return 1
	}
	name := C.GoString(method)
	var raw []byte
	// Larger payloads pass unchanged; the observation gap is counted without copying the RPC buffer.
	if n <= capture.MaxFrame && request != nil {
		raw = C.GoBytes(unsafe.Pointer(request), C.int(n))
	}
	result, ok := dispatch(name, raw, n > capture.MaxFrame)
	envelope := map[string]any{"ok": ok, "result": result}
	if !ok {
		envelope = map[string]any{"ok": false, "error": map[string]string{"code": "capture_configuration", "message": "capture disabled: invalid private configuration or incompatible schema"}}
	}
	b, _ := json.Marshal(envelope)
	response.ptr = C.CBytes(b)
	response.len = C.size_t(len(b))
	if !ok {
		return 1
	}
	return 0
}
func dispatch(method string, raw []byte, oversized bool) (any, bool) {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		var req struct {
			SchemaVersion int `json:"schema_version"`
		}
		if json.Unmarshal(raw, &req) != nil || req.SchemaVersion != 6 {
			return nil, false
		}
		mu.Lock()
		defer mu.Unlock()
		if engine == nil {
			cfg, err := capture.LoadConfig(os.Getenv("CLIPROXY_CAPTURE_CONFIG"))
			if err != nil {
				return nil, false
			}
			engine = capture.NewEngine(cfg)
		}
		return map[string]any{"schema_version": 6, "metadata": map[string]string{"name": "cliproxy-capture", "version": capture.Version, "author": "shpitdev", "GitHubRepository": "https://github.com/anandpant/convex-components"}, "capabilities": map[string]bool{"request_interceptor": true, "response_interceptor": true, "response_stream_interceptor": true, "request_lifecycle_plugin": true}}, true
	case "plugin.quiesce", "plugin.shutdown":
		return map[string]any{}, true
	}
	mu.RLock()
	defer mu.RUnlock()
	if engine != nil {
		if oversized {
			engine.RecordGap()
		} else {
			engine.Observe(method, raw)
		}
	}
	// Every request clears authority, including excluded requests; all body/status/credential fields pass through.
	if method == "request.intercept_before" || method == "request.intercept_after" {
		return map[string]any{"ClearHeaders": capture.AuthorityHeaders}, true
	}
	return map[string]any{}, true
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, n C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {
	mu.Lock()
	defer mu.Unlock()
	if engine != nil {
		engine.Close()
		engine = nil
	}
}
