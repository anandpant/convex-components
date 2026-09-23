package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestReservedGapAndBootHealthRecoverAfterOutage(t *testing.T) {
	c := testConfig()
	dir, err := os.MkdirTemp("/tmp", "capture-gap-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	os.MkdirAll(dir, 0700)
	c.Socket = filepath.Join(dir, "capture.sock")
	c.MaxActive = 1024
	e := NewEngine(c)
	defer e.Close()
	large, _ := json.Marshal([]byte(`{"text":"` + strings.Repeat("x", 600<<10) + `"}`))
	small, _ := json.Marshal([]byte(`{"text":"hello"}`))
	for i := 0; i < 230; i++ {
		body := small
		if i < 4 {
			body = large
		}
		h := Hook{RequestID: "call-" + strconv.Itoa(i), Headers: testHeaders(), Body: body}
		raw, _ := json.Marshal(h)
		e.Observe("request.intercept_before", raw)
	}
	if e.bytes.Load() > int64(c.QueueBytes) || e.dropped.Load() == 0 || e.controlLost.Load() == 0 {
		t.Fatal("queue/reserved control limits not exercised")
	}
	outbox, err := OpenOutbox(filepath.Join(dir, "events.db"), 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	listener, err := net.Listen("unix", c.Socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: outbox}
	defer server.Close()
	go server.Serve(listener)
	deadline := time.Now().Add(5 * time.Second)
	var persistedHealth Health
	var gaps int
	for time.Now().Before(deadline) {
		var raw []byte
		outbox.db.QueryRow("SELECT payload FROM health LIMIT 1").Scan(&raw)
		json.Unmarshal(raw, &persistedHealth)
		outbox.db.QueryRow("SELECT count(*) FROM events WHERE CAST(payload AS TEXT) LIKE '%capture_queue_%'").Scan(&gaps)
		if persistedHealth.ControlLost > 0 && gaps > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if persistedHealth.ControlLost == 0 || gaps == 0 {
		t.Fatal("loss coverage/gaps not persisted after recovery")
	}
	if persistedHealth.PrecommitCoverage != "unknown_before_local_commit" {
		t.Fatal("invented zero-loss guarantee")
	}
}
func TestDiskAdmissionReservesControlCapacity(t *testing.T) {
	o, err := OpenOutbox(filepath.Join(t.TempDir(), "private", "events.db"), 4<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	full := false
	for i := 1; i <= 40; i++ {
		body := []byte(`{"text":"` + strings.Repeat("x", 200<<10) + `"}`)
		event := Observation{SchemaVersion: 1, Destination: "dev", Instance: "instance", Boot: "boot", RequestID: "call", Sequence: uint64(i), Kind: "stream_chunk", Route: "POST /v1/messages", Body: body, ContentBytes: len(body), ContentSHA256: Digest(body)}
		raw, _ := json.Marshal(event)
		res := httptest.NewRecorder()
		o.ServeHTTP(res, httptest.NewRequest("POST", "/events", bytes.NewReader(raw)))
		if res.Code == 507 {
			full = true
			break
		}
		if res.Code != 200 {
			t.Fatal(res.Code)
		}
	}
	if !full {
		t.Fatal("did not saturate disk budget")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	h := Health{SchemaVersion: 1, Operation: "health_record", Destination: "dev", Instance: "instance", Boot: "boot", StartedAt: now, ObservedAt: now, Dropped: 1, PrecommitCoverage: "unknown_before_local_commit"}
	raw, _ := json.Marshal(h)
	res := httptest.NewRecorder()
	o.ServeHTTP(res, httptest.NewRequest("POST", "/health", bytes.NewReader(raw)))
	if res.Code != 200 {
		t.Fatalf("control reserve unavailable: %d", res.Code)
	}
}
func TestPluginDestinationQueuesRemainIndependent(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "capture-fair-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	c := testConfig()
	c.Socket = filepath.Join(dir, "capture.sock")
	c.MaxActive = 100
	c.Bindings = append(c.Bindings, Binding{Key: "test-Prod-key-123456789", Destination: "prod", Deployment: "https://prod.convex.site", Environment: "prod"})
	delivered := make(chan Observation, 8)
	listener, err := net.Listen("unix", c.Socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events" {
			w.WriteHeader(200)
			return
		}
		var event Observation
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		json.Unmarshal(raw, &event)
		if event.Destination == "dev" {
			w.WriteHeader(507)
			return
		}
		delivered <- event
		json.NewEncoder(w).Encode(map[string]string{"identity": event.Identity(), "digest": Digest(raw)})
	})}
	defer server.Close()
	go server.Serve(listener)
	e := NewEngine(c)
	defer e.Close()
	body, _ := json.Marshal([]byte(`{"text":"` + strings.Repeat("x", 600<<10) + `"}`))
	for i := 0; i < 12; i++ {
		raw, _ := json.Marshal(Hook{RequestID: "dev-" + strconv.Itoa(i), Headers: testHeaders(), Body: body})
		e.Observe("request.intercept_before", raw)
	}
	headers := testHeaders()
	headers.Set("Authorization", "Bearer test-Prod-key-123456789")
	headers.Set("X-Meshix-Capture-Destination", "prod")
	headers.Set("X-Meshix-Deployment", "https://prod.convex.site")
	small, _ := json.Marshal([]byte(`{"text":"safe"}`))
	raw, _ := json.Marshal(Hook{RequestID: "prod-healthy", Headers: headers, Body: small})
	e.Observe("request.intercept_before", raw)
	select {
	case event := <-delivered:
		if event.RequestID != "prod-healthy" || event.Gap != "" {
			t.Fatal("healthy destination lost")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked dev queue stalled prod capture")
	}
	if e.dropped.Load() == 0 {
		t.Fatal("did not exercise blocked destination pressure")
	}
}

func TestHealthACKMustMatchPinnedRecipient(t *testing.T) {
	for _, wrong := range []bool{true, false} {
		t.Run(strconv.FormatBool(wrong), func(t *testing.T) {
			o, err := OpenOutbox(filepath.Join(t.TempDir(), "private", "events.db"), 4<<20, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer o.Close()
			now := time.Now().UTC().Format(time.RFC3339Nano)
			h := Health{SchemaVersion: 1, Operation: "health_record", Destination: "dev", Instance: "instance", Boot: "boot", StartedAt: now, ObservedAt: now, PrecommitCoverage: "unknown_before_local_commit"}
			raw, _ := json.Marshal(h)
			res := httptest.NewRecorder()
			o.ServeHTTP(res, httptest.NewRequest("POST", "/health", bytes.NewReader(raw)))
			if res.Code != 200 {
				t.Fatal(res.Code)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				deployment := "dev-deployment"
				if wrong {
					deployment = "other-deployment"
				}
				json.NewEncoder(w).Encode(map[string]any{"committed": true, "digest": Digest(raw), "destinationId": "dev", "deploymentId": deployment})
			}))
			defer server.Close()
			o.sendHealth(context.Background(), Destination{ID: "dev", Deployment: "dev-deployment", Instance: "instance", URL: server.URL}, server.Client())
			var dirty int
			if err := o.db.QueryRow("SELECT dirty FROM health").Scan(&dirty); err != nil {
				t.Fatal(err)
			}
			if wrong {
				var state string
				o.db.QueryRow("SELECT state FROM delivery_status WHERE destination='dev'").Scan(&state)
				if dirty != 1 || state != "quarantined" {
					t.Fatal("wrong recipient ACK discarded health")
				}
			} else if dirty != 0 {
				t.Fatal("exact recipient ACK did not commit")
			}
		})
	}
}
