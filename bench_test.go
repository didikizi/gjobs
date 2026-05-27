package jobs

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// ── Realistic payload types ────────────────────────────────────────────────────
// Each struct reflects the real-world payload for the matching JobDef template.

// quickPayload matches the Quick template: tiny, fire-and-forget.
// Use case: push notification, cache invalidation, event log entry.
type quickPayload struct {
	UserID string `json:"user_id"`
	Event  string `json:"event"`
	Body   string `json:"body"`
}

// standardPayload matches the Standard template: typical async work.
// Use case: transactional email, webhook delivery.
type standardPayload struct {
	To      string            `json:"to"`
	Subject string            `json:"subject"`
	Body    string            `json:"body"`
	Headers map[string]string `json:"headers"`
	Tags    []string          `json:"tags"`
}

// reliablePayload matches the Reliable template: must-complete operations.
// Use case: payment charge, billing event, compliance audit.
type reliablePayload struct {
	OrderID    string            `json:"order_id"`
	UserID     string            `json:"user_id"`
	Amount     float64           `json:"amount"`
	Currency   string            `json:"currency"`
	CardLast4  string            `json:"card_last4"`
	RetryCount int               `json:"retry_count"`
	Metadata   map[string]string `json:"metadata"`
	LineItems  []struct {
		SKU   string  `json:"sku"`
		Qty   int     `json:"qty"`
		Price float64 `json:"price"`
	} `json:"line_items"`
}

// batchPayload matches the Batch template: long-running heavy processing.
// Use case: report generation, data export, ML batch inference.
type batchPayload struct {
	ReportID  string   `json:"report_id"`
	UserIDs   []string `json:"user_ids"` // hundreds of entries
	DateFrom  string   `json:"date_from"`
	DateTo    string   `json:"date_to"`
	Format    string   `json:"format"`
	Options   map[string]any `json:"options"`
}

// ── Sample values ─────────────────────────────────────────────────────────────

var (
	sampleQuick = quickPayload{
		UserID: "u_01HXYZ",
		Event:  "order.shipped",
		Body:   "Your order #1234 has shipped.",
	}

	sampleStandard = standardPayload{
		To:      "alice@example.com",
		Subject: "Your weekly summary",
		Body:    strings.Repeat("Here is your summary. ", 30), // ~660 B
		Headers: map[string]string{
			"X-Mailer":    "jobs/v0.3",
			"X-RequestID": "req_01HXYZ",
		},
		Tags: []string{"transactional", "weekly", "summary"},
	}

	sampleReliable = reliablePayload{
		OrderID:   "ord_01HXYZ",
		UserID:    "u_01HXYZ",
		Amount:    99.99,
		Currency:  "USD",
		CardLast4: "4242",
		Metadata: map[string]string{
			"ip":         "1.2.3.4",
			"country":    "US",
			"session_id": "sess_abc123",
		},
	}

	sampleBatch = func() batchPayload {
		ids := make([]string, 500)
		for i := range ids {
			ids[i] = "u_" + strings.Repeat("0", 8)
		}
		return batchPayload{
			ReportID: "rpt_01HXYZ",
			UserIDs:  ids, // ~500 user IDs → ~5 KB payload
			DateFrom: "2025-01-01",
			DateTo:   "2025-03-31",
			Format:   "csv",
			Options:  map[string]any{"include_deleted": false, "tz": "UTC"},
		}
	}()
)

// payloadBytes returns the JSON-encoded size of a value (for logging).
func payloadBytes(v any) int {
	b, _ := json.Marshal(v)
	return len(b)
}

// ── Storage-level benchmarks ──────────────────────────────────────────────────

func BenchmarkEnqueue_Quick(b *testing.B) {
	benchEnqueue(b, "quick", sampleQuick)
}

func BenchmarkEnqueue_Standard(b *testing.B) {
	benchEnqueue(b, "standard", sampleStandard)
}

func BenchmarkEnqueue_Reliable(b *testing.B) {
	benchEnqueue(b, "reliable", sampleReliable)
}

func BenchmarkEnqueue_Batch(b *testing.B) {
	benchEnqueue(b, "batch", sampleBatch)
}

func benchEnqueue(b *testing.B, name string, payload any) {
	b.Helper()
	s, _ := NewSQLiteStorage(":memory:")
	defer s.Close()
	ctx := context.Background()
	b.ReportMetric(float64(payloadBytes(payload)), "payload_bytes")
	b.ResetTimer()
	for i := range b.N {
		j := newJob(name)
		j.ID = name + "-" + string(rune(i))
		s.Enqueue(ctx, j) //nolint:errcheck
	}
}

// ── End-to-end benchmarks (push → handler → done) ─────────────────────────────

func BenchmarkE2E_Quick(b *testing.B) {
	benchE2E(b, Def("q"), sampleQuick, 10)
}

func BenchmarkE2E_Standard(b *testing.B) {
	benchE2E(b, Def("s"), sampleStandard, 10)
}

func BenchmarkE2E_Reliable(b *testing.B) {
	benchE2E(b, Def("r"), sampleReliable, 20)
}

func BenchmarkE2E_Batch(b *testing.B) {
	benchE2E(b, Def("bt"), sampleBatch, 5)
}

func benchE2E(b *testing.B, def JobDef, payload any, concurrency int) {
	b.Helper()
	storage, _ := NewSQLiteStorage(":memory:")
	q, _ := New(
		WithStorage(storage),
		WithConcurrency(concurrency),
		WithPollInterval(1*time.Millisecond),
	)

	done := make(chan struct{}, b.N+1)
	q.Register(def, func(_ context.Context, _ []byte) error {
		done <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go q.Start(ctx) //nolint:errcheck

	b.ReportMetric(float64(payloadBytes(payload)), "payload_bytes")
	b.ReportMetric(float64(concurrency), "workers")
	b.ResetTimer()

	for range b.N {
		q.Enqueue(def, payload) //nolint:errcheck
	}

	timeout := time.After(60 * time.Second)
	for range b.N {
		select {
		case <-done:
		case <-timeout:
			b.Fatal("benchmark timeout: jobs did not complete in time")
		}
	}
	cancel()
}

// ── Concurrency scaling ────────────────────────────────────────────────────────

func BenchmarkConcurrency_1(b *testing.B)  { benchConcurrency(b, 1) }
func BenchmarkConcurrency_10(b *testing.B) { benchConcurrency(b, 10) }
func BenchmarkConcurrency_50(b *testing.B) { benchConcurrency(b, 50) }

func benchConcurrency(b *testing.B, workers int) {
	b.Helper()
	storage, _ := NewSQLiteStorage(":memory:")
	q, _ := New(
		WithStorage(storage),
		WithConcurrency(workers),
		WithPollInterval(1*time.Millisecond),
	)

	done := make(chan struct{}, b.N+1)
	noop := Def("noop")
	q.Register(noop, func(_ context.Context, _ []byte) error {
		done <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go q.Start(ctx) //nolint:errcheck

	b.ReportMetric(float64(workers), "workers")
	b.ResetTimer()

	for range b.N {
		q.Enqueue(noop, nil) //nolint:errcheck
	}

	timeout := time.After(60 * time.Second)
	for range b.N {
		select {
		case <-done:
		case <-timeout:
			b.Fatal("benchmark timeout")
		}
	}
	cancel()
}

// ── Original benchmarks (kept for historical baseline) ────────────────────────

func BenchmarkEnqueue(b *testing.B) {
	s, _ := NewSQLiteStorage(":memory:")
	defer s.Close()
	ctx := context.Background()
	b.ResetTimer()
	for i := range b.N {
		j := newJob("bench")
		j.ID = "bench-" + string(rune(i))
		s.Enqueue(ctx, j) //nolint:errcheck
	}
}

func BenchmarkClaim(b *testing.B) {
	s, _ := NewSQLiteStorage(":memory:")
	defer s.Close()
	ctx := context.Background()
	for i := range 10_000 {
		j := newJob("bench")
		j.ID = "pre-" + string(rune(i))
		s.Enqueue(ctx, j) //nolint:errcheck
	}
	b.ResetTimer()
	for range b.N {
		jobs, _ := s.Claim(ctx, 10)
		for _, j := range jobs {
			s.MarkDone(ctx, j.ID) //nolint:errcheck
		}
	}
}
