package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"time"
)

type admissionItem struct {
	raw  []byte
	ack  eventACK
	size int
}

// Group only already queued records. There is no coalescing timer or extra
// producer queue; byte accounting stays charged until exact durable ACKs arrive.
func (e *Engine) admissionWorker(ctx context.Context, pipe *capturePipe, queue <-chan queued, control bool) {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", e.config.Socket)
	}, MaxConnsPerHost: 1}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var carry *admissionItem
	encode := func(q queued) *admissionItem {
		o := q.o
		o.DroppedObservations = e.dropped.Load()
		o.ScopeConflicts = e.conflicts.Load()
		if !control {
			o.ContentBytes = len(o.Body)
			o.ContentSHA256 = Digest(o.Body)
		}
		raw, err := json.Marshal(o)
		if err != nil || len(raw) > MaxFrame {
			e.dropped.Add(1)
			if control {
				e.controlLost.Add(1)
			}
			e.bytes.Add(-int64(q.size))
			pipe.bytes.Add(-int64(q.size))
			return nil
		}
		return &admissionItem{raw, eventACK{o.Identity(), Digest(raw)}, q.size}
	}
	for {
		first := carry
		carry = nil
		if first == nil {
			select {
			case <-ctx.Done():
				return
			case q := <-queue:
				first = encode(q)
			}
		}
		if first == nil {
			continue
		}
		items := []*admissionItem{first}
		frameBytes := len(first.raw) + 2
		// A single existing MaxFrame record uses the original endpoint so array
		// delimiters never reduce its allowed envelope size.
		for len(items) < maxAdmissionEvents && frameBytes < MaxFrame {
			var q queued
			select {
			case q = <-queue:
			default:
				goto ready
			}
			item := encode(q)
			if item == nil {
				continue
			}
			if frameBytes+len(item.raw)+1 > MaxFrame {
				carry = item
				break
			}
			items = append(items, item)
			frameBytes += len(item.raw) + 1
		}
	ready:
		path := "/events"
		raw := first.raw
		if len(items) > 1 {
			path = "/events/batch"
			raw = make([]byte, 0, frameBytes)
			raw = append(raw, '[')
			for i, item := range items {
				if i > 0 {
					raw = append(raw, ',')
				}
				raw = append(raw, item.raw...)
			}
			raw = append(raw, ']')
		}
		for {
			start := time.Now()
			req, _ := http.NewRequestWithContext(ctx, "POST", "http://capture"+path, bytes.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			res, err := client.Do(req)
			status, accepted := 0, false
			if err == nil {
				status = res.StatusCode
				body, readErr := io.ReadAll(io.LimitReader(res.Body, (64<<10)+1))
				res.Body.Close()
				var acks []eventACK
				var decodeErr error
				if len(items) == 1 {
					var ack eventACK
					decodeErr = json.Unmarshal(body, &ack)
					if decodeErr == nil {
						acks = []eventACK{ack}
					}
				} else {
					decodeErr = json.Unmarshal(body, &acks)
				}
				accepted = readErr == nil && decodeErr == nil && len(body) <= 64<<10 && status == 200 && len(acks) == len(items)
				for i := range acks {
					if i >= len(items) || acks[i] != items[i].ack {
						accepted = false
						break
					}
				}
			}
			e.admission.recordEvents(start, status, accepted, uint64(len(items)))
			if accepted {
				break
			}
			if !pause(ctx, 250*time.Millisecond) {
				return
			}
		}
		for _, item := range items {
			e.bytes.Add(-int64(item.size))
			pipe.bytes.Add(-int64(item.size))
		}
	}
}
