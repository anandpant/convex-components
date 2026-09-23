package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Hook struct {
	RequestID, TraceID, SourceFormat, RequestedModel, Model string
	Headers, RequestHeaders                                 http.Header
	Body                                                    json.RawMessage
	Stream                                                  bool
	ChunkIndex                                              int
	Outcome                                                 string
	StatusCode                                              int
	StartedAt, CompletedAt                                  string
	Error                                                   string
}
type scopeState struct {
	binding              Binding
	route                string
	start                time.Time
	last                 time.Time
	captureLost          bool
	sequence             uint64
	correlation          map[string]string
	conflicts            []string
	trace, format, model string
	stream               bool
}
type queued struct {
	o      Observation
	size   int
	stream bool
}
type capturePipe struct {
	queue       chan queued
	control     chan queued
	bytes       atomic.Int64
	budget      int
	activeLimit int
}
type Engine struct {
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
	e := &Engine{config: c, boot: BootID(), scopes: map[string]*scopeState{}, pipes: map[string]*capturePipe{}, started: time.Now().UTC().Format(time.RFC3339Nano), healthDone: make(chan struct{}), cancel: cancel, done: make(chan struct{})}
	var workers sync.WaitGroup
	for destination := range destinations {
		pipe := &capturePipe{queue: make(chan queued, max(1, 1024/count)), control: make(chan queued, max(1, 128/count)), budget: c.QueueBytes / count, activeLimit: max(1, c.MaxActive/count)}
		e.pipes[destination] = pipe
		workers.Add(2)
		go func() { defer workers.Done(); e.worker(ctx, pipe) }()
		go func() { defer workers.Done(); e.controlWorker(ctx, pipe) }()
	}
	go func() { workers.Wait(); close(e.done) }()
	go e.healthLoop(ctx)
	return e
}
func (e *Engine) Close() { e.cancel(); <-e.done; <-e.healthDone }
func (e *Engine) Status() map[string]any {
	return map[string]any{"queuedBytes": e.bytes.Load(), "droppedObservations": e.dropped.Load(), "lostControlObservations": e.controlLost.Load(), "expiredScopes": e.expired.Load(), "excludedCallbacks": e.excluded.Load(), "scopeConflicts": e.conflicts.Load(), "precommitCoverage": "unknown_on_process_loss"}
}
func (e *Engine) Observe(method string, raw []byte) {
	if !e.config.Enabled {
		return
	}
	// Stock host already pays clone/RPC cost. Bound our own retained state and decode.
	if len(raw) > MaxFrame {
		e.dropped.Add(1)
		return
	}
	var h Hook
	if json.Unmarshal(raw, &h) != nil || !identifier.MatchString(h.RequestID) {
		e.dropped.Add(1)
		return
	}
	if len(h.RequestedModel) > 256 {
		h.RequestedModel = "[oversized model withheld]"
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
	s := e.scopes[h.RequestID]
	kind := ""
	switch method {
	case "request.intercept_before":
		kind = "request"
	case "response.intercept_after":
		kind = "response"
	case "response.intercept_stream_chunk":
		kind = "stream_chunk"
		if h.ChunkIndex < 0 {
			kind = "stream_init"
		}
	case "request.complete":
		kind = "completion"
	default:
		return
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
			e.dropped.Add(1)
			return
		}
		s = &scopeState{binding: b, route: route, start: now, last: now, correlation: map[string]string{}, trace: h.TraceID, format: h.SourceFormat, model: h.RequestedModel, stream: h.Stream}
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
	e.observations.Add(1)
	s.sequence++
	o := Observation{SchemaVersion: 1, PluginVersion: Version, RedactionVersion: "framed-json-v2", Destination: s.binding.Destination, Instance: e.config.Instance, Boot: e.boot, RequestID: h.RequestID, Sequence: s.sequence, Kind: kind, ObservedAt: now.UTC().Format(time.RFC3339Nano), OffsetNS: now.Sub(s.start).Nanoseconds(), Route: s.route, Revision: e.config.Revision, SourceFormat: s.format, Model: s.model, TraceID: s.trace, Correlation: maps.Clone(s.correlation), CorrelationConflicts: s.conflicts}
	if kind == "stream_chunk" {
		index := h.ChunkIndex
		o.ChunkIndex = &index
	}
	if kind == "completion" {
		o.Outcome = h.Outcome
		o.StatusCode = h.StatusCode
		o.StartedAt = h.StartedAt
		o.CompletedAt = h.CompletedAt
		if h.Error != "" {
			o.Error = "[stock error text withheld]"
		}
		delete(e.scopes, h.RequestID)
	}
	if kind == "response" {
		o.StatusCode = h.StatusCode
	}
	if kind == "request" || kind == "response" || kind == "stream_chunk" {
		if len(h.Body) > 0 && json.Unmarshal(h.Body, &o.Body) != nil {
			o.Gap = "invalid_body_encoding"
		}
		o.ObservedBodyBytes = len(o.Body)
		if len(o.Body) > MaxBody {
			o.Body = nil
			o.Gap = "observation_body_limit"
		}
	}
	if s.captureLost {
		o.Body = nil
		o.Gap = "prior_capture_gap"
	}
	size := len(o.Body) + 8192
	pipe := e.pipes[o.Destination]
	if pipe.bytes.Load()+int64(size) > int64(pipe.budget) {
		s.captureLost = true
		e.reserveGap(o, "capture_queue_byte_limit")
		return
	}
	e.bytes.Add(int64(size))
	pipe.bytes.Add(int64(size))
	select {
	case pipe.queue <- queued{o, size, s.stream}:
	default:
		e.bytes.Add(-int64(size))
		pipe.bytes.Add(-int64(size))
		s.captureLost = true
		e.reserveGap(o, "capture_queue_item_limit")
	}
}
func (e *Engine) worker(ctx context.Context, pipe *capturePipe) {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", e.config.Socket)
	}, MaxConnsPerHost: 1}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	redactors := map[string]*FrameRedactor{}
	redactorLast := map[string]time.Time{}
	cleanup := time.NewTicker(time.Minute)
	defer cleanup.Stop()
	redactorBytes := 0
	secrets := append([]string{}, e.config.Redactions...)
	for _, b := range e.config.Bindings {
		secrets = append(secrets, b.Key)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanup.C:
			for id, last := range redactorLast {
				if time.Since(last) > 24*time.Hour {
					redactorBytes -= redactors[id].bufferedBytes()
					delete(redactors, id)
					delete(redactorLast, id)
				}
			}
		case q := <-pipe.queue:
			o := q.o
			if o.Kind == "stream_chunk" || o.Kind == "completion" {
				r := redactors[o.RequestID]
				if r == nil {
					if len(redactors) >= pipe.activeLimit {
						o.Body = nil
						o.Gap = "redaction_state_limit"
					}
					r = &FrameRedactor{allowUndelimited: o.Route == "POST /v1/responses", allowJSON: o.Route == "POST /v1/chat/completions"}
					if len(redactors) < pipe.activeLimit {
						redactors[o.RequestID] = r
						redactorLast[o.RequestID] = time.Now()
					}
				}
				if _, ok := redactors[o.RequestID]; ok {
					redactorLast[o.RequestID] = time.Now()
				}
				redactorBytes -= r.bufferedBytes()
				if o.Gap != "" {
					r.pending = nil
					r.semantic = semanticRedactor{failed: true}
					o.Body = nil
				}
				if redactorBytes+r.bufferedBytes()+len(o.Body) > pipe.budget {
					r.pending = nil
					r.semantic = semanticRedactor{failed: true}
					o.Body = nil
					o.Gap = "redaction_memory_limit"
				}
				if r.allowJSON && len(o.Body) > 0 && o.Body[0] == '{' && json.Valid(o.Body) {
					o.BodyFraming = "stock_json_chunk"
				} else {
					o.BodyFraming = "stock_sse_candidate"
				}
				b, from, gap := r.Feed(o.Body, o.Sequence, true, o.Kind == "completion", secrets)
				redactorBytes += r.bufferedBytes()
				o.Body = b
				o.BodyFromSequence = from
				if gap != "" {
					o.Gap = gap
				}
				if o.Kind == "completion" {
					delete(redactors, o.RequestID)
					delete(redactorLast, o.RequestID)
				}
			} else if len(o.Body) > 0 {
				r := FrameRedactor{}
				b, _, gap := r.Feed(o.Body, o.Sequence, false, true, secrets)
				o.Body = b
				if gap != "" {
					o.Gap = gap
				}
			}
			// Only allowlisted metadata was retained; scrub known values there as well.
			redactMetadata(&o, secrets)
			o.DroppedObservations = e.dropped.Load()
			o.ScopeConflicts = e.conflicts.Load()
			o.ContentBytes = len(o.Body)
			o.ContentSHA256 = Digest(o.Body)
			raw, err := json.Marshal(o)
			if err != nil || len(raw) > MaxFrame {
				e.dropped.Add(1)
				e.bytes.Add(-int64(q.size))
				pipe.bytes.Add(-int64(q.size))
				continue
			}
			// Retain the same serialized identity until a durable ACK. No inference goroutine waits here.
			for {
				req, _ := http.NewRequestWithContext(ctx, "POST", "http://capture/events", bytes.NewReader(raw))
				req.Header.Set("Content-Type", "application/json")
				res, err := client.Do(req)
				accepted := false
				if err == nil {
					var ack struct{ Identity, Digest string }
					body, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
					res.Body.Close()
					accepted = res.StatusCode == 200 && json.Unmarshal(body, &ack) == nil && ack.Identity == o.Identity() && ack.Digest == Digest(raw)
				}
				if accepted {
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(250 * time.Millisecond):
				}
			}
			e.bytes.Add(-int64(q.size))
			pipe.bytes.Add(-int64(q.size))
		}
	}
}
func (e *Engine) RecordGap() { e.dropped.Add(1) }

// Reserved metadata-only gaps preserve the allocated sequence where possible. If this
// bounded reserve also fills, heartbeat counters explicitly report unknown coverage.
func (e *Engine) reserveGap(o Observation, reason string) {
	e.dropped.Add(1)
	o.Body = nil
	o.Gap = reason
	o.ContentBytes = 0
	o.ContentSHA256 = Digest(nil)
	select {
	case e.pipes[o.Destination].control <- queued{o: o, size: 0}:
	default:
		e.controlLost.Add(1)
	}
}
