package capture

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"
)

func TestContentTestServerBodyReadCancellationAndValidation(t *testing.T) {
	readError := errors.New("fixture body read failed")
	for _, test := range []struct {
		name      string
		canceled  bool
		body      io.Reader
		wantError bool
		wantCode  int
		wantACK   bool
	}{
		{"canceled body read", true, iotest.ErrReader(readError), false, http.StatusOK, false},
		{"uncanceled body read", false, iotest.ErrReader(readError), true, http.StatusOK, false},
		{"malformed JSON", false, strings.NewReader("{"), true, http.StatusBadRequest, false},
		{"canceled malformed JSON", true, strings.NewReader("{"), true, http.StatusBadRequest, false},
		{"invalid observation", false, strings.NewReader(`{"schemaVersion":99}`), true, http.StatusBadRequest, false},
		{"valid observation", false, strings.NewReader(string(drainEvent(1, 8))), false, http.StatusOK, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var reported []string
			acknowledged := false
			handler := contentTestEventsHandler(func(args ...any) {
				reported = append(reported, fmt.Sprint(args...))
			}, func(Observation) bool { acknowledged = true; return true })
			request := httptest.NewRequest("POST", "http://capture/events", test.body)
			if test.canceled {
				ctx, cancel := context.WithCancel(request.Context())
				cancel()
				request = request.WithContext(ctx)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if (len(reported) != 0) != test.wantError || response.Code != test.wantCode || acknowledged != test.wantACK {
				t.Fatalf("reports=%v status=%d acknowledged=%t", reported, response.Code, acknowledged)
			}
		})
	}
}

func TestContentTestServerCanceledUpload(t *testing.T) {
	e := contentTestEngine(t, 64<<20, func(Observation) {
		t.Error("canceled partial upload was acknowledged")
	})
	e.Close()
	conn, err := net.Dial("unix", e.config.Socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err = conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = fmt.Fprint(conn, "POST /events HTTP/1.1\r\nHost: capture\r\nContent-Length: 1024\r\nExpect: 100-continue\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "POST"})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusContinue {
		t.Fatalf("handler did not start reading upload: %s", response.Status)
	}
	// The 100 response proves io.ReadAll has started. Disconnect with the body
	// still incomplete; shared fixture cleanup must join this canceled read.
	conn.Close()
}

func TestContentTestEngineCleanupJoinsActiveHandler(t *testing.T) {
	entered := make(chan struct{})
	finished := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	t.Run("fixture", func(t *testing.T) {
		// Registered first, so this observes the shared fixture after cleanup.
		t.Cleanup(func() {
			select {
			case <-entered:
			default:
				return
			}
			select {
			case <-finished:
			default:
				t.Error("fixture cleanup returned with an active HTTP handler")
			}
		})
		e := contentTestEngine(t, 64<<20, func(Observation) {
			close(entered)
			defer close(finished)
			<-release
		})
		// No engine work should mask the independent server-handler lifetime.
		e.Close()
		t.Cleanup(func() {
			// Hold the handler across the start of cleanup. The timer releases
			// simulated handler work, including assertion-failure paths.
			time.AfterFunc(100*time.Millisecond, unblock)
		})
		conn, err := net.Dial("unix", e.config.Socket)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		raw := drainEvent(1, 8)
		if _, err = fmt.Fprintf(conn, "POST /events HTTP/1.1\r\nHost: capture\r\nContent-Length: %d\r\n\r\n%s", len(raw), raw); err != nil {
			t.Fatal(err)
		}
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("request did not enter fixture handler")
		}
	})
	// Also join on the failing baseline so this regression never leaks work.
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("fixture handler did not finish")
	}
}
