package capture

import (
	"sync/atomic"
	"time"
)

// LocalAdmission describes Unix-socket attempts, not remote receiver delivery.
// Latency includes failed attempts but excludes time waiting in the capture queue.
// Attempts/ACKs count records; Requests counts Unix requests. Batched round-trip
// latency is weighted by record count, since every record waits for that ACK.
// Receiver 0.3.3 accepts/ACKs this optional field but does not project it; the
// private persisted health row is its authoritative source.
// Queue peaks are the largest per-destination depth sampled after enqueue.
type LocalAdmission struct {
	Requests         uint64 `json:"requests"`
	Attempts         uint64 `json:"attempts"`
	Accepted         uint64 `json:"accepted"`
	Rejected507      uint64 `json:"rejected507"`
	OtherFailures    uint64 `json:"otherFailures"`
	LatencyTotalNS   uint64 `json:"latencyTotalNs"`
	LatencyMaxNS     uint64 `json:"latencyMaxNs"`
	QueuePeakItems   int64  `json:"queuePeakItems"`
	ControlPeakItems int64  `json:"controlPeakItems"`
}

type admissionCounters struct {
	requests                                       atomic.Uint64
	attempts, accepted, rejected507, otherFailures atomic.Uint64
	latencyTotal, latencyMax                       atomic.Uint64
	queuePeak, controlPeak                         atomic.Int64
}

func (a *admissionCounters) record(start time.Time, status int, accepted bool) {
	a.recordEvents(start, status, accepted, 1)
}

func (a *admissionCounters) recordEvents(start time.Time, status int, accepted bool, count uint64) {
	ns := uint64(time.Since(start).Nanoseconds())
	a.requests.Add(1)
	a.attempts.Add(count)
	a.latencyTotal.Add(ns * count)
	for old := a.latencyMax.Load(); ns > old; old = a.latencyMax.Load() {
		if a.latencyMax.CompareAndSwap(old, ns) {
			break
		}
	}
	switch {
	case accepted:
		a.accepted.Add(count)
	case status == 507:
		a.rejected507.Add(count)
	default:
		a.otherFailures.Add(count)
	}
}

func recordPeak(peak *atomic.Int64, n int64) {
	for old := peak.Load(); n > old; old = peak.Load() {
		if peak.CompareAndSwap(old, n) {
			return
		}
	}
}

func (a *admissionCounters) snapshot() *LocalAdmission {
	return &LocalAdmission{a.requests.Load(), a.attempts.Load(), a.accepted.Load(), a.rejected507.Load(), a.otherFailures.Load(), a.latencyTotal.Load(), a.latencyMax.Load(), a.queuePeak.Load(), a.controlPeak.Load()}
}
