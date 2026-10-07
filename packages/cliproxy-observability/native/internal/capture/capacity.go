package capture

import (
	"time"
)

type Capacity struct {
	BudgetBytes               int64    `json:"budgetBytes"`
	AllocatedBytes            int64    `json:"allocatedBytes"`
	ReusableBytes             int64    `json:"reusableBytes"`
	RemainingBytes            int64    `json:"remainingBytes"`
	PendingBytes              int64    `json:"pendingBytes"`
	AcknowledgedEvents        int64    `json:"acknowledgedEvents"`
	ObservedAt                string   `json:"observedAt"`
	GrowthBytesPerSecond      float64  `json:"growthBytesPerSecond,omitempty"`
	EstimatedSecondsToCeiling *float64 `json:"estimatedSecondsToCeiling,omitempty"`
}

// Forecasts describe measured net growth since the prior sample. A shrinking or
// steady database has no extrapolated exhaustion time; retention is still finite.
func (o *Outbox) capacity() (*Capacity, error) {
	o.capacityMu.Lock()
	defer o.capacityMu.Unlock()
	var pages, free, size, pending, receipts int64
	if err := o.db.QueryRow("SELECT page_count,freelist_count,page_size FROM pragma_page_count(),pragma_freelist_count(),pragma_page_size()").Scan(&pages, &free, &size); err != nil {
		return nil, err
	}
	if err := o.db.QueryRow("SELECT payload_bytes,receipt_rows FROM outbox_counts WHERE id=1").Scan(&pending, &receipts); err != nil {
		return nil, err
	}
	now := time.Now()
	allocated := (pages - free) * size
	c := &Capacity{BudgetBytes: o.budget, AllocatedBytes: allocated, ReusableBytes: free * size, RemainingBytes: max(0, o.budget-min(o.budget/8, 1<<20)-allocated), PendingBytes: pending, AcknowledgedEvents: receipts, ObservedAt: now.UTC().Format(time.RFC3339Nano)}
	if elapsed := now.Sub(o.capacityAt).Seconds(); !o.capacityAt.IsZero() && elapsed >= 1 {
		if growth := float64(allocated-o.capacityBytes) / elapsed; growth > 0 {
			c.GrowthBytesPerSecond = growth
			seconds := float64(c.RemainingBytes) / growth
			c.EstimatedSecondsToCeiling = &seconds
		}
	}
	if o.capacityAt.IsZero() || now.Sub(o.capacityAt) >= time.Second {
		o.capacityAt = now
		o.capacityBytes = allocated
	}
	return c, nil
}
