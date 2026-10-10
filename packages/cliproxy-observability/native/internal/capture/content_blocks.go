package capture

import (
	"context"
	"maps"
	"sync/atomic"
	"time"
)

const streamBlockBytes = 64 << 10
const streamBlockFrames = 1024
const scopeMetadataCharge = 8192
const recordMetadataCharge = 16 << 10
const controlRecordCharge = recordMetadataCharge + 24
const captureDrainLimit = 10 * time.Second

// ContentHealth counts exact bytes across this plugin boot. Committed means
// local durable ACK; unconfirmed bytes may be durable after an ACK was lost.
type ContentHealth struct {
	ObservedBytes        uint64 `json:"observedBytes"`
	CommittedBytes       uint64 `json:"committedBytes"`
	LostBytes            uint64 `json:"lostBytes"`
	UnconfirmedBytes     uint64 `json:"unconfirmedBytes"`
	UnknownLosses        uint64 `json:"unknownLosses"`
	StreamCallbacks      uint64 `json:"streamCallbacks"`
	IncompleteCalls      uint64 `json:"incompleteCalls"`
	CapacityRejections   uint64 `json:"capacityRejections"`
	RetainedChargedBytes int64  `json:"retainedChargedBytes"`
	PeakChargedBytes     int64  `json:"peakChargedBytes"`
}
type contentCounters struct {
	observed, committed, lost, unconfirmed, unknownLoss atomic.Uint64
	streamCallbacks, incomplete, capacityRejections     atomic.Uint64
	peak                                                atomic.Int64
}

func (c *contentCounters) snapshot(retained int64) *ContentHealth {
	return &ContentHealth{c.observed.Load(), c.committed.Load(), c.lost.Load(), c.unconfirmed.Load(), c.unknownLoss.Load(), c.streamCallbacks.Load(), c.incomplete.Load(), c.capacityRejections.Load(), retained, max(retained, c.peak.Load())}
}

// Include all retained representations. Base64/JSON and HTTP serialization
// remain reserved while the immutable body waits for its exact durable ACK.
func observationCharge(o Observation) int {
	return recordMetadataCharge + 4*cap(o.Body) + 24*cap(o.StockHookChunkLengths) + 8*len(o.Error)
}

func (e *Engine) controlReserve() int {
	return min(e.config.QueueBytes/8, 128*controlRecordCharge)
}

// Reservations occur under e.mu; only workers release concurrently.
func (e *Engine) reserveBytes(pipe *capturePipe, n int, control bool) bool {
	limit, destinationLimit := e.config.QueueBytes, pipe.budget
	if !control {
		limit -= e.controlReserve()
		destinationLimit -= e.controlReserve() / max(1, len(e.pipes))
	}
	if n < 0 || e.bytes.Load()+int64(n) > int64(limit) || pipe.bytes.Load()+int64(n) > int64(destinationLimit) {
		return false
	}
	retained := e.bytes.Add(int64(n))
	pipe.bytes.Add(int64(n))
	recordPeak(&e.content.peak, retained)
	return true
}

func (e *Engine) releaseBytes(pipe *capturePipe, n int) {
	e.bytes.Add(-int64(n))
	pipe.bytes.Add(-int64(n))
}

func (e *Engine) allocateObservation(s *scopeState, request, kind string, now time.Time) Observation {
	e.observations.Add(1)
	s.sequence++
	o := Observation{SchemaVersion: 1, PluginVersion: Version, CapturePolicy: CapturePolicy, Destination: s.binding.Destination, Instance: e.config.Instance, Boot: e.boot, RequestID: request, Sequence: s.sequence, Kind: kind, ObservedAt: now.UTC().Format(time.RFC3339Nano), OffsetNS: now.Sub(s.start).Nanoseconds(), Route: s.route, Revision: e.config.Revision, SourceFormat: s.format, Model: s.model, TraceID: s.trace, Correlation: maps.Clone(s.correlation), CorrelationConflicts: append([]string(nil), s.conflicts...)}
	if s.captureLost {
		o.Gap = "prior_capture_gap"
		o.CaptureIncomplete = true
		o.LostContentBytes = s.lostBytes
	}
	excludeCredentialMetadata(&o, e.config.Bindings)
	return o
}

func (e *Engine) markIncomplete(s *scopeState, lost uint64) {
	if !s.captureLost {
		e.content.incomplete.Add(1)
	}
	s.captureLost = true
	s.lostBytes += lost
	e.content.lost.Add(lost)
}

// Never wait on a writer while holding the shared callback mutex. Coalescing
// and the byte pool absorb qualified load; exhaustion is explicit and immediate.
func (e *Engine) tryCapacity(attempt func() bool) bool {
	if attempt() {
		return true
	}
	e.content.capacityRejections.Add(1)
	return false
}

func (e *Engine) enqueueObservation(s *scopeState, o Observation, ownedCharge int) bool {
	pipe := e.pipes[o.Destination]
	charge := observationCharge(o)
	delta := charge - ownedCharge
	accepted := e.tryCapacity(func() bool {
		if pipe.items.Load() >= int64(cap(pipe.queue)) {
			return false
		}
		if delta > 0 && !e.reserveBytes(pipe, delta, false) {
			return false
		}
		if delta < 0 {
			e.releaseBytes(pipe, -delta)
		}
		pipe.items.Add(1)
		pipe.queue <- queued{o: o, size: charge}
		recordPeak(&e.admission.queuePeak, pipe.items.Load())
		return true
	})
	if accepted {
		return true
	}
	if ownedCharge > 0 {
		e.releaseBytes(pipe, ownedCharge)
	}
	e.markIncomplete(s, uint64(len(o.Body)+len(o.Error)))
	o.CaptureIncomplete = true
	o.LostContentBytes = s.lostBytes
	e.reserveContentGap(o, "capture_content_capacity")
	return false
}

func (e *Engine) appendStream(s *scopeState, request string, body []byte, index int, now time.Time) {
	if s.block != nil && (len(s.block.Body)+len(body) > streamBlockBytes || len(s.block.StockHookChunkLengths) >= streamBlockFrames) {
		e.flushStream(s)
	}
	if s.block == nil {
		o := e.allocateObservation(s, request, "stream_chunk", now)
		o.BodyFraming = "stock_hook_block"
		o.ChunkIndex = &index
		o.Body = body
		o.StockHookChunkLengths = []int{len(body)}
		o.ObservedBodyBytes = len(body)
		charge := observationCharge(o)
		pipe := e.pipes[s.binding.Destination]
		if !e.tryCapacity(func() bool { return e.reserveBytes(pipe, charge, false) }) {
			e.markIncomplete(s, uint64(len(body)))
			o.CaptureIncomplete = true
			o.LostContentBytes = s.lostBytes
			e.reserveContentGap(o, "capture_content_capacity")
			return
		}
		s.block = &o
		s.blockCharge = charge
		s.blockStarted = now
	} else {
		o := s.block
		bodyCapacity := cap(o.Body)
		if len(o.Body)+len(body) > bodyCapacity {
			bodyCapacity = min(streamBlockBytes, max(len(o.Body)+len(body), max(256, bodyCapacity*2)))
		}
		frameCapacity := cap(o.StockHookChunkLengths)
		if len(o.StockHookChunkLengths)+1 > frameCapacity {
			frameCapacity = min(streamBlockFrames, max(1, frameCapacity*2))
		}
		charge := recordMetadataCharge + 4*bodyCapacity + 24*frameCapacity
		pipe := e.pipes[s.binding.Destination]
		if !e.tryCapacity(func() bool { return e.reserveBytes(pipe, charge-s.blockCharge, false) }) {
			e.flushStream(s)
			e.appendStream(s, request, body, index, now)
			return
		}
		if bodyCapacity != cap(o.Body) {
			next := make([]byte, len(o.Body), bodyCapacity)
			copy(next, o.Body)
			o.Body = next
		}
		if frameCapacity != cap(o.StockHookChunkLengths) {
			next := make([]int, len(o.StockHookChunkLengths), frameCapacity)
			copy(next, o.StockHookChunkLengths)
			o.StockHookChunkLengths = next
		}
		o.Body = append(o.Body, body...)
		o.StockHookChunkLengths = append(o.StockHookChunkLengths, len(body))
		o.ObservedBodyBytes = len(o.Body)
		s.blockCharge = charge
	}
	if len(s.block.Body) >= streamBlockBytes || len(s.block.StockHookChunkLengths) >= streamBlockFrames {
		e.flushStream(s)
	}
}

func (e *Engine) flushStream(s *scopeState) {
	if s.block == nil {
		return
	}
	o, charge := *s.block, s.blockCharge
	s.block = nil
	s.blockCharge = 0
	e.enqueueObservation(s, o, charge)
}

func (e *Engine) emitStreamGap(s *scopeState, request string, now time.Time, reason string, observedBytes int) {
	e.flushStream(s)
	o := e.allocateObservation(s, request, "stream_chunk", now)
	o.ObservedBodyBytes = observedBytes
	o.BodyFraming = "stock_hook_block"
	o.StockHookChunkLengths = []int{0}
	o.CaptureIncomplete = true
	o.LostContentBytes = s.lostBytes
	e.reserveContentGap(o, reason)
}

func (e *Engine) reserveContentGap(o Observation, reason string) {
	e.dropped.Add(1)
	o.Body = nil
	o.Error = ""
	o.Gap = reason
	o.CaptureIncomplete = true
	o.ContentBytes = 0
	o.ContentSHA256 = Digest(nil)
	if o.BodyFraming == "stock_hook_block" {
		o.StockHookChunkLengths = []int{0}
	}
	pipe := e.pipes[o.Destination]
	charge := observationCharge(o)
	if pipe.controlItems.Load() >= int64(cap(pipe.control)) || !e.reserveBytes(pipe, charge, true) {
		e.controlLost.Add(1)
		return
	}
	pipe.controlItems.Add(1)
	pipe.control <- queued{o: o, size: charge}
	recordPeak(&e.admission.controlPeak, pipe.controlItems.Load())
}

func (e *Engine) flushLoop(ctx context.Context) {
	defer close(e.flushDone)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			e.mu.Lock()
			if !e.closing {
				for _, s := range e.scopes {
					if s.block != nil && now.Sub(s.blockStarted) >= 900*time.Millisecond {
						e.flushStream(s)
					}
				}
			}
			e.mu.Unlock()
		}
	}
}

func (e *Engine) finishIncomplete(s *scopeState, request string, now time.Time, reason string) {
	e.flushStream(s)
	e.markIncomplete(s, 0)
	o := e.allocateObservation(s, request, "completion", now)
	o.Gap = reason
	o.CaptureIncomplete = true
	o.LostContentBytes = s.lostBytes
	// Capture ended before a stock lifecycle result. Do not invent an outcome.
	e.reserveContentGap(o, reason)
	e.releaseBytes(e.pipes[s.binding.Destination], s.scopeCharge)
	s.scopeCharge = 0
}

func (e *Engine) closeContent() {
	e.closeOnce.Do(func() {
		deadline := time.Now().Add(captureDrainLimit)
		e.mu.Lock()
		e.closing = true
		for id, s := range e.scopes {
			e.finishIncomplete(s, id, time.Now(), "capture_shutdown_incomplete")
			delete(e.scopes, id)
		}
		e.mu.Unlock()
		for e.bytes.Load() > 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if e.config.Enabled && time.Now().Before(deadline) {
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			client, transport := healthClient(e.config.Socket)
			e.publishHealth(ctx, client, time.Now(), 0)
			cancel()
			transport.CloseIdleConnections()
		}
		e.cancel()
		<-e.done
		<-e.healthDone
		<-e.flushDone
		for _, pipe := range e.pipes {
			for len(pipe.queue) > 0 {
				q := <-pipe.queue
				e.content.lost.Add(uint64(len(q.o.Body) + len(q.o.Error)))
				e.releaseBytes(pipe, q.size)
				pipe.items.Add(-1)
			}
			for len(pipe.control) > 0 {
				q := <-pipe.control
				e.controlLost.Add(1)
				e.releaseBytes(pipe, q.size)
				pipe.controlItems.Add(-1)
			}
		}
	})
}
