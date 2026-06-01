// Package main demonstrates gjobs deduplication keys.
// Run: go run ./examples/dedup
//
// Three scenarios:
//   1. Ignore mode (default)  — duplicates silently skipped
//   2. Replace mode           — last enqueued payload wins (for pending dups)
//   3. TTL mode               — key stays locked past completion
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/didikizi/gjobs"
)

// ── Job types ─────────────────────────────────────────────────────────────────

type Welcome struct {
	UserID string `json:"user_id"`
}

type CacheRefresh struct {
	Key     string `json:"key"`
	Version int    `json:"version"`
}

type DailyReport struct {
	Date string `json:"date"`
}

// ── JobDef definitions ────────────────────────────────────────────────────────

var (
	SendWelcome  = gjobs.Def("send_welcome")
	RefreshCache = gjobs.Def("refresh_cache")
	GenReport    = gjobs.Def("gen_report")
)

func main() {
	q, err := gjobs.New(
		gjobs.WithStorage(gjobs.NewMemoryStorage()),
		gjobs.WithConcurrency(2),
		gjobs.WithPollInterval(20*time.Millisecond),
		gjobs.WithNoLogger(), // silence the WARN-on-skip logs so the demo output stays clean
	)
	if err != nil {
		log.Fatal(err)
	}

	// Counters to prove dedup actually worked.
	var welcomeRuns, refreshRuns, reportRuns atomic.Int32
	var lastVersion atomic.Int32

	// A 100 ms sleep in each handler ensures the first enqueue lands in
	// 'running' state before the second/third can attempt to enqueue —
	// making the demo deterministic.
	gjobs.HandleDef[Welcome](q, SendWelcome, func(_ context.Context, w Welcome) error {
		welcomeRuns.Add(1)
		fmt.Printf("  [welcome]  → user=%s\n", w.UserID)
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	gjobs.HandleDef[CacheRefresh](q, RefreshCache, func(_ context.Context, c CacheRefresh) error {
		refreshRuns.Add(1)
		lastVersion.Store(int32(c.Version))
		fmt.Printf("  [refresh]  → key=%s version=%d\n", c.Key, c.Version)
		return nil
	})

	gjobs.HandleDef[DailyReport](q, GenReport, func(_ context.Context, r DailyReport) error {
		reportRuns.Add(1)
		fmt.Printf("  [report]   → date=%s\n", r.Date)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// ── Scenario 1: Ignore mode (default) ─────────────────────────────────
	// Three enqueues with the same DedupKey. The first lands; the next two
	// see an active duplicate and are silently skipped.
	fmt.Println("─── Ignore mode (default) ─────────────────────")
	for i := 0; i < 3; i++ {
		_ = q.Enqueue(ctx, SendWelcome,
			Welcome{UserID: "u-42"},
			gjobs.DedupKey("welcome:u-42"),
		)
	}

	// ── Scenario 2: Replace mode ──────────────────────────────────────────
	// Three enqueues with the same key + a 600 ms delay. They all queue up
	// while pending; each new one REPLACES the previous pending payload.
	// Only the last (version=3) survives to run.
	fmt.Println("\n─── Replace mode ──────────────────────────────")
	for v := 1; v <= 3; v++ {
		_ = q.Enqueue(ctx, RefreshCache,
			CacheRefresh{Key: "home", Version: v},
			gjobs.DedupKey("refresh:home"),
			gjobs.DedupReplace(),
			gjobs.After(600*time.Millisecond),
		)
	}

	// ── Scenario 3: TTL — key stays locked past completion ─────────────────
	// First enqueue runs and completes. A second trigger 300 ms later is
	// within the TTL window and is silently skipped, even though the job
	// is no longer running.
	fmt.Println("\n─── TTL mode (lock past completion) ───────────")
	_ = q.Enqueue(ctx, GenReport,
		DailyReport{Date: "2026-06-01"},
		gjobs.DedupKey("report:daily:2026-06-01"),
		gjobs.DedupTTL(24*time.Hour),
	)
	time.AfterFunc(300*time.Millisecond, func() {
		_ = q.Enqueue(ctx, GenReport,
			DailyReport{Date: "2026-06-01"},
			gjobs.DedupKey("report:daily:2026-06-01"),
			gjobs.DedupTTL(24*time.Hour),
		)
		fmt.Println("  [TTL]      → second trigger silently skipped (within TTL window)")
	})

	if err := q.Start(ctx); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n─── Summary ────────────────────────────────────")
	fmt.Printf("  welcomeRuns  = %d   (expected 1 — ignored duplicates)\n", welcomeRuns.Load())
	fmt.Printf("  refreshRuns  = %d, lastVersion = %d   (expected 1 / 3 — last replaced wins)\n",
		refreshRuns.Load(), lastVersion.Load())
	fmt.Printf("  reportRuns   = %d   (expected 1 — TTL blocked the retrigger)\n", reportRuns.Load())
}
