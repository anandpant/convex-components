package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Hook struct {
	RequestID, TraceID, SourceFormat, ToFormat, RequestedModel, Model string
	Headers, RequestHeaders                                           http.Header
	Body                                                              json.RawMessage
	Stream                                                            bool
	ChunkIndex                                                        int
	Outcome                                                           string
	StatusCode                                                        int
	StartedAt, CompletedAt                                            string
	Metadata                                                          struct {
		SelectedAuthID    json.RawMessage `json:"selected_auth_id"`
		SelectedAuthIndex json.RawMessage `json:"selected_auth_index"`
	}
	Error string
}
type scopeState struct {
	block                *Observation
	blockCharge          int
	blockStarted         time.Time
	lostBytes            uint64
	scopeCharge          int
	binding              Binding
	route                string
	start                time.Time
	last                 time.Time
	captureLost          bool
	sequence             uint64
	correlation          map[string]string
	conflicts            []string
	trace, format, model string
}
type queued struct {
	o    Observation
	size int
}
type capturePipe struct {
	items        atomic.Int64
	controlItems atomic.Int64
	queue        chan queued
	control      chan queued
	bytes        atomic.Int64
	budget       int
}
type Engine struct {
	queuedItems  atomic.Int64
	content      contentCounters
	closing      bool
	flushDone    chan struct{}
	closeOnce    sync.Once
	admission    admissionCounters
	config       Config
	boot         string
	mu           sync.Mutex
	scopes       map[string]*scopeState
	pipes        map[string]*capturePipe
	controlLost  atomic.Uint64
	observations atomic.Uint64
	expired      atomic.Uint64
	started      string
	healthDone   chan struct{}
	bytes        atomic.Int64
	dropped      atomic.Uint64
	excluded     atomic.Uint64
	conflicts    atomic.Uint64
	cancel       context.CancelFunc
	done         chan struct{}
}

func NewEngine(c Config) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	destinations := map[string]bool{}
	for _, binding := range c.Bindings {
		destinations[binding.Destination] = true
	}
	count := max(1, len(destinations))
	e := &Engine{config: c, boot: BootID(), scopes: map[string]*scopeState{}, pipes: map[string]*capturePipe{}, started: time.Now().UTC().Format(time.RFC3339Nano), healthDone: make(chan struct{}), flushDone: make(chan struct{}), cancel: cancel, done: make(chan struct{})}
	var workers sync.WaitGroup
	for destination := range destinations {
		pipe := &capturePipe{queue: make(chan queued, contentRecordLimit), control: make(chan queued, max(1, min(128, c.QueueBytes/8/controlRecordCharge)/count)), budget: c.QueueBytes / count}
		e.pipes[destination] = pipe
		workers.Add(2)
		go func() { defer workers.Done(); e.worker(ctx, pipe) }()
		go func() { defer workers.Done(); e.controlWorker(ctx, pipe) }()
	}
	go func() { workers.Wait(); close(e.done) }()
	go e.healthLoop(ctx)
	go e.flushLoop(ctx)
	return e
}
func (e *Engine) Close() { e.closeContent() }
func (e *Engine) Status() map[string]any {
	return map[string]any{"content": e.content.snapshot(e.bytes.Load()), "localAdmission": e.admission.snapshot(), "queuedBytes": e.bytes.Load(), "droppedObservations": e.dropped.Load(), "lostControlObservations": e.controlLost.Load(), "expiredScopes": e.expired.Load(), "excludedCallbacks": e.excluded.Load(), "scopeConflicts": e.conflicts.Load(), "precommitCoverage": "unknown_on_process_loss"}
}
func (e *Engine) Observe(method string, raw []byte) {
	if !e.config.Enabled {
		return
	}
	kind := hookKind(method)
	if kind == "" {
		return
	}
	// Stock host already pays clone/RPC cost. Bound our own retained state and decode.
	if len(raw) > MaxFrame {
		e.RecordGap(method)
		return
	}
	var h Hook
	if json.Unmarshal(raw, &h) != nil || !identifier.MatchString(h.RequestID) {
		e.RecordGap(method)
		return
	}
	if len(h.RequestedModel) > 256 {
		h.RequestedModel = ""
	}
	if len(h.TraceID) > 256 {
		h.TraceID = ""
	}
	if len(h.SourceFormat) > 64 {
		h.SourceFormat = ""
	}
	now := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closing {
		return
	}
	s := e.scopes[h.RequestID]
	if kind == "stream_chunk" && h.ChunkIndex < 0 {
		kind = "stream_init"
	}

	if s == nil {
		// Never enroll a half-observed stream, unknown completion, or arbitrary response.
		if kind != "request" && !(kind == "response" && h.Model == "" && h.RequestedModel == "") {
			return
		}
		headers := h.Headers
		if kind == "response" {
			headers = h.RequestHeaders
		}
		b, reason := Scope(e.config, headers)
		if reason != "" {
			if reason == "scope_conflict" || reason == "credentials_conflict" {
				e.conflicts.Add(1)
			} else {
				e.excluded.Add(1)
			}
			return
		}
		route, _ := ExactlyOne(headers, AuthorityHeaders[1])
		if kind == "response" && route != "GET /v1/models" {
			return
		}
		if len(e.scopes) >= e.config.MaxActive {
			e.content.unknownLoss.Add(1)
			e.dropped.Add(1)
			return
		}
		pipe := e.pipes[b.Destination]
		if !e.reserveBytes(pipe, scopeMetadataCharge, false) {
			e.dropped.Add(1)
			e.content.unknownLoss.Add(1)
			return
		}
		s = &scopeState{scopeCharge: scopeMetadataCharge, binding: b, route: route, start: now, last: now, correlation: map[string]string{}, trace: h.TraceID, format: h.SourceFormat, model: h.RequestedModel}
		for header, field := range map[string]string{"X-Meshix-Request-Id": "requestId", "X-Meshix-Run-Id": "runId", "X-Meshix-Job-Id": "jobId", "X-Meshix-Trace-Id": "traceId", "X-Opencode-Session-Id": "opencodeSessionId", "X-Meshix-Root-Execution-Id": "rootExecutionId", "X-Meshix-Operation-Id": "operationId", "X-Meshix-Step-Id": "stepId", "X-Meshix-Part-Id": "partId", "X-Meshix-Attempt-Id": "attemptId"} {
			v, ok := ExactlyOne(headers, header)
			if ok && len(v) <= 256 {
				s.correlation[field] = v
			} else {
				for k := range headers {
					if strings.EqualFold(k, header) {
						s.conflicts = append(s.conflicts, field)
						break
					}
				}
			}
		}
		e.scopes[h.RequestID] = s
	} else if kind == "request" {
		return
	} // Repeated before hook cannot rebind scope.
	s.last = now
	if kind == "stream_chunk" {
		var body []byte
		if len(h.Body) > 0 && json.Unmarshal(h.Body, &body) != nil {
			e.content.unknownLoss.Add(1)
			e.markIncomplete(s, 0)
			e.emitStreamGap(s, h.RequestID, now, "invalid_body_encoding", 0)
			return
		}
		e.content.streamCallbacks.Add(1)
		e.content.observed.Add(uint64(len(body)))
		if len(body) > MaxBody {
			e.markIncomplete(s, uint64(len(body)))
			e.emitStreamGap(s, h.RequestID, now, "observation_body_limit", len(body))
			return
		}
		e.appendStream(s, h.RequestID, body, h.ChunkIndex, now)
		return
	}
	e.flushStream(s)
	o := e.allocateObservation(s, h.RequestID, kind, now)
	if kind == "request_after_auth" {
		if len(h.Model) <= 256 {
			o.ExecutionModel = h.Model
		} else {
			o.MetadataOmissions = append(o.MetadataOmissions, "executionModel:limit")
		}
		if len(h.ToFormat) <= 64 {
			o.ExecutionProtocol = h.ToFormat
		} else {
			o.MetadataOmissions = append(o.MetadataOmissions, "executionProtocol:limit")
		}
	}
	if kind == "request_after_auth" || kind == "stream_init" {
		o.SelectedAuthID = metadataIdentity(h.Metadata.SelectedAuthID, "selectedAuthId", &o)
		o.SelectedAuthIndex = metadataIdentity(h.Metadata.SelectedAuthIndex, "selectedAuthIndex", &o)
	}
	if kind == "completion" {
		o.Outcome = h.Outcome
		o.StatusCode = h.StatusCode
		o.StartedAt = h.StartedAt
		o.CompletedAt = h.CompletedAt
		if h.Error != "" {
			o.ErrorPresent = true
			o.ObservedErrorBytes = len(h.Error)
			if len(h.Error) <= MaxBody {
				o.Error = h.Error
			} else {
				o.MetadataOmissions = append(o.MetadataOmissions, "error:limit")
				o.Gap = "diagnostic_text_limit"
			}
		}

	}
	if kind == "response" {
		o.StatusCode = h.StatusCode
	}
	if kind == "request" || kind == "request_after_auth" || kind == "response" || kind == "stream_chunk" {
		if len(h.Body) > 0 && json.Unmarshal(h.Body, &o.Body) != nil {
			o.Gap = "invalid_body_encoding"
		}
		o.ObservedBodyBytes = len(o.Body)
		if len(o.Body) > MaxBody {
			o.Body = nil
			o.Gap = "observation_body_limit"
		}
	}
	excludeCredentialMetadata(&o, e.config.Bindings)
	e.content.observed.Add(uint64(o.ObservedBodyBytes + o.ObservedErrorBytes))
	if o.Gap == "invalid_body_encoding" {
		e.content.unknownLoss.Add(1)
		e.markIncomplete(s, 0)
	}
	if o.ObservedBodyBytes > len(o.Body) || o.ObservedErrorBytes > len(o.Error) {
		e.markIncomplete(s, uint64(o.ObservedBodyBytes-len(o.Body)+o.ObservedErrorBytes-len(o.Error)))
	}
	if s.captureLost {
		if o.Gap == "" {
			o.Gap = "prior_capture_gap"
		}
		o.CaptureIncomplete = true
		o.LostContentBytes = s.lostBytes
	}

	e.enqueueObservation(s, o, 0)
	if kind == "completion" {
		delete(e.scopes, h.RequestID)
		e.releaseBytes(e.pipes[s.binding.Destination], s.scopeCharge)
		s.scopeCharge = 0
	}

}
func (e *Engine) worker(ctx context.Context, pipe *capturePipe) {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", e.config.Socket)
	}, MaxConnsPerHost: 1}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for {
		select {
		case <-ctx.Done():
			return
		case q := <-pipe.queue:

			o := q.o
			o.DroppedObservations = e.dropped.Load()
			o.ScopeConflicts = e.conflicts.Load()
			o.ContentBytes = len(o.Body)
			o.ContentSHA256 = Digest(o.Body)
			raw, err := json.Marshal(o)
			if err != nil || len(raw) > MaxFrame {
				e.dropped.Add(1)
				e.releaseBytes(pipe, q.size)
				pipe.items.Add(-1)
				e.queuedItems.Add(-1)

				e.content.lost.Add(uint64(len(o.Body) + len(o.Error)))

				continue
			}
			// Retain the same serialized identity until a durable ACK. No inference goroutine waits here.
			for {
				start := time.Now()
				req, _ := http.NewRequestWithContext(ctx, "POST", "http://capture/events", bytes.NewReader(raw))
				req.Header.Set("Content-Type", "application/json")
				res, err := client.Do(req)
				status := 0
				accepted := false
				if err == nil {
					status = res.StatusCode
					var ack struct{ Identity, Digest string }
					body, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
					res.Body.Close()
					accepted = res.StatusCode == 200 && json.Unmarshal(body, &ack) == nil && ack.Identity == o.Identity() && ack.Digest == Digest(raw)
				}
				e.admission.record(start, status, accepted)
				if accepted {
					break
				}
				select {
				case <-ctx.Done():
					e.content.unconfirmed.Add(uint64(len(o.Body) + len(o.Error)))
					e.releaseBytes(pipe, q.size)
					pipe.items.Add(-1)
					e.queuedItems.Add(-1)

					return
				case <-time.After(250 * time.Millisecond):
				}
			}
			e.content.committed.Add(uint64(len(o.Body) + len(o.Error)))
			e.releaseBytes(pipe, q.size)
			pipe.items.Add(-1)
			e.queuedItems.Add(-1)

		}
	}
}

// An oversized/unreadable RPC cannot be attributed safely. Mark active calls
// incomplete rather than claiming their later terminal proves complete content.
func (e *Engine) RecordGap(method string) {
	if hookKind(method) == "" {
		return
	}
	e.dropped.Add(1)
	e.content.unknownLoss.Add(1)
	e.mu.Lock()
	defer e.mu.Unlock()
	for request, s := range e.scopes {
		e.markIncomplete(s, 0)
		e.emitStreamGap(s, request, time.Now(), "unattributed_rpc_gap", 0)
	}
}

func hookKind(method string) string {
	switch method {
	case "request.intercept_before":
		return "request"
	case "request.intercept_after":
		return "request_after_auth"
	case "response.intercept_after":
		return "response"
	case "response.intercept_stream_chunk":
		return "stream_chunk"
	case "request.complete":
		return "completion"
	}
	return ""
}
