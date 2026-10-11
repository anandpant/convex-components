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
	o     Observation
	parts []Observation
	size  int
}

func (q queued) records() []Observation {
	if q.parts != nil {
		return q.parts
	}
	return []Observation{q.o}
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
	// Stock frames can carry duplicate base64 request copies (OriginalRequest,
	// RequestBody, history) at any size. Hook never decodes them: bound the
	// retained fields, never the frame the host has already allocated.
	var h Hook
	if json.Unmarshal(raw, &h) != nil || !identifier.MatchString(h.RequestID) {
		e.recordUnreadable(kind, h, raw)
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
		if len(e.scopes) >= e.config.MaxActive || !e.reserveBytes(e.pipes[b.Destination], scopeMetadataCharge, false) {
			e.missCall(h)
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
			// One stock callback stays one logical block: exact ordered parts.
			e.flushStream(s)
			o := e.allocateObservation(s, h.RequestID, "stream_chunk", now)
			o.BodyFraming = "stock_hook_block"
			o.ChunkIndex = &h.ChunkIndex
			o.ObservedBodyBytes = len(body)
			e.enqueueParts(s, o, body)
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
	var split []byte
	if kind == "request" || kind == "request_after_auth" || kind == "response" {
		var body []byte
		if len(h.Body) > 0 && json.Unmarshal(h.Body, &body) != nil {
			o.Gap = "invalid_body_encoding"
		}
		o.ObservedBodyBytes = len(body)
		if len(body) > MaxBody {
			split = body
		} else {
			o.Body = body
		}
	}
	excludeCredentialMetadata(&o, e.config.Bindings)
	e.content.observed.Add(uint64(o.ObservedBodyBytes + o.ObservedErrorBytes))
	if o.Gap == "invalid_body_encoding" {
		e.content.unknownLoss.Add(1)
		e.markIncomplete(s, 0)
	}
	if o.ObservedErrorBytes > len(o.Error) {
		e.markIncomplete(s, uint64(o.ObservedErrorBytes-len(o.Error)))
	}
	if s.captureLost {
		if o.Gap == "" {
			o.Gap = "prior_capture_gap"
		}
		o.CaptureIncomplete = true
		o.LostContentBytes = s.lostBytes
	}

	if split != nil {
		e.enqueueParts(s, o, split)
	} else {
		e.enqueueObservation(s, o, 0)
	}
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
			records := q.records()
			for i, o := range records {
				if !e.deliver(ctx, client, o) {
					// Never sent parts of an unfinished split body are lost, not unconfirmed.
					e.content.unconfirmed.Add(contentBytes(records[i : i+1]))
					e.content.lost.Add(contentBytes(records[i+1:]))
					e.releaseBytes(pipe, q.size)
					pipe.items.Add(-int64(len(records) - i))
					e.queuedItems.Add(-int64(len(records) - i))
					return
				}
				pipe.items.Add(-1)
				e.queuedItems.Add(-1)
			}
			// Split parts share one buffer; it stays charged until the last part's ACK.
			e.releaseBytes(pipe, q.size)
		}
	}
}

// deliver retains one serialized identity until its durable ACK. It returns
// false only on shutdown; a record that cannot be serialized is counted lost.
func (e *Engine) deliver(ctx context.Context, client *http.Client, o Observation) bool {
	o.DroppedObservations = e.dropped.Load()
	o.ScopeConflicts = e.conflicts.Load()
	o.ContentBytes = len(o.Body)
	o.ContentSHA256 = Digest(o.Body)
	raw, err := json.Marshal(o)
	if err != nil || len(raw) > MaxFrame {
		e.dropped.Add(1)
		e.content.lost.Add(contentBytes([]Observation{o}))
		return true
	}
	// No inference goroutine waits here.
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
			e.content.committed.Add(contentBytes([]Observation{o}))
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(250 * time.Millisecond):
		}
	}
}

var requestIDKey = []byte(`"RequestID":"`)

// An unreadable callback marks only the call it names. Without a recoverable
// RequestID it is an unknown loss; unrelated active calls are never marked.
func (e *Engine) recordUnreadable(kind string, h Hook, raw []byte) {
	request := h.RequestID
	if !identifier.MatchString(request) {
		// Invalid JSON can still name its call. An unescaped key quote is structural.
		request = ""
		if i := bytes.Index(raw, requestIDKey); i >= 0 {
			rest := raw[i+len(requestIDKey):]
			if end := bytes.IndexByte(rest, '"'); end > 0 && identifier.Match(rest[:end]) {
				request = string(rest[:end])
			}
		}
	}
	// A type error leaves decodable fields set; their body bytes are known loss.
	var body []byte
	if len(h.Body) > 0 && json.Unmarshal(h.Body, &body) != nil {
		body = nil
	}
	e.dropped.Add(1)
	e.content.unknownLoss.Add(1)
	e.content.observed.Add(uint64(len(body)))
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.scopes[request]
	if s == nil || e.closing {
		e.content.lost.Add(uint64(len(body)))
		if kind == "request" && s == nil {
			e.content.missedCalls.Add(1)
		}
		return
	}
	e.markIncomplete(s, uint64(len(body)))
	e.emitStreamGap(s, request, time.Now(), "unreadable_callback", len(body))
}

// missCall records an enrolled call that could not start a scope. Its later
// callbacks have no scope; the request body is the call's known lost content.
func (e *Engine) missCall(h Hook) {
	var body []byte
	if len(h.Body) > 0 && json.Unmarshal(h.Body, &body) != nil {
		body = nil
	}
	e.dropped.Add(1)
	e.content.unknownLoss.Add(1)
	e.content.missedCalls.Add(1)
	e.content.observed.Add(uint64(len(body)))
	e.content.lost.Add(uint64(len(body)))
}

func contentBytes(records []Observation) uint64 {
	n := 0
	for _, o := range records {
		n += len(o.Body) + len(o.Error)
	}
	return uint64(n)
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
