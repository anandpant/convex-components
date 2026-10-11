package capture

// A body larger than MaxBody is emitted as exact ordered parts with
// consecutive sequences rather than dropped. Parts share one immutable buffer,
// so the item is charged that buffer once, per-part record metadata, and the
// existing 4x serialization headroom for the single part its worker holds.
func partCharge(o Observation, total, count int) int {
	return count*(recordMetadataCharge+24*len(o.StockHookChunkLengths)) + total + 3*MaxBody
}

// enqueueParts admits all parts or none. A rejected body records its exact
// known loss at the base sequence; later sequences are allocated only on success.
func (e *Engine) enqueueParts(s *scopeState, o Observation, body []byte) bool {
	count := (len(body) + MaxBody - 1) / MaxBody
	if o.BodyFraming == "stock_hook_block" {
		o.StockHookChunkLengths = []int{0}
	}
	pipe := e.pipes[o.Destination]
	charge := partCharge(o, len(body), count)
	if !e.tryCapacity(func() bool {
		return e.queuedItems.Load()+int64(count) <= contentRecordLimit && e.reserveBytes(pipe, charge, false)
	}) {
		e.markIncomplete(s, uint64(len(body)))
		o.CaptureIncomplete = true
		o.LostContentBytes = s.lostBytes
		e.reserveContentGap(o, "capture_content_capacity")
		return false
	}
	digest := Digest(body)
	parts := make([]Observation, count)
	for i := range parts {
		p := o
		if i > 0 {
			e.observations.Add(1)
			s.sequence++
			p.Sequence = s.sequence
		}
		start, end := i*MaxBody, min(len(body), (i+1)*MaxBody)
		p.Body = body[start:end:end]
		p.ObservedBodyBytes = end - start
		p.BodyPart = &BodyPart{Index: i, Count: count, Offset: start, TotalBytes: len(body), TotalSHA256: digest}
		if p.BodyFraming == "stock_hook_block" {
			p.StockHookChunkLengths = []int{end - start}
		}
		parts[i] = p
	}
	pipe.items.Add(int64(count))
	e.queuedItems.Add(int64(count))
	pipe.queue <- queued{o: parts[0], parts: parts, size: charge}
	recordPeak(&e.admission.queuePeak, pipe.items.Load())
	return true
}
