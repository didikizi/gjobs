<div align="center">

# ⚡ gjobs

**Persistent background jobs for Go. Just a file. No Redis, no Postgres, no Docker.**

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat-square&logo=go&logoColor=white)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)](#license)
[![SQLite](https://img.shields.io/badge/storage-SQLite%20%7C%20Memory-blue?style=flat-square)](#-storage-backends)
[![CI](https://github.com/didikizi/gjobs/actions/workflows/ci.yml/badge.svg)](https://github.com/didikizi/gjobs/actions/workflows/ci.yml)

[Quick start](#-quick-start) · [Who this is for](#-who-this-is-for) · [Crash safety](#-crash-safety) · [Storage](#-storage-backends) · [API](#-api-reference)

</div>

---

## Who this is for

gjobs is for Go apps that run on **a single machine and need reliable background work** without standing up Redis, a message broker, or a separate worker process.

It's the right fit if you deploy on **Fly.io, Railway, Hetzner, or any VPS** where adding a Redis instance feels like overkill. It works especially well with **Litestream** — WAL mode is enabled by default, so your job queue replicates for free.

It's the wrong fit if you need **multiple machines processing the same queue simultaneously** — switch to a dedicated broker (River, asynq).

---

## ✨ Quick start

```go
package main

import (
    "context"
    "fmt"
    "log/slog"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/didikizi/gjobs"
)

type Email struct {
    To      string
    Subject string
}

var SendEmail = gjobs.Def("send_email")

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    q, _ := gjobs.New(
        gjobs.WithLogger(slog.Default()),
        gjobs.WithShutdownTimeout(30 * time.Second),
    )

    gjobs.HandleDef[Email](q, SendEmail, func(ctx context.Context, e Email) error {
        fmt.Printf("sending to %s: %s\n", e.To, e.Subject)
        return nil
    })

    // Push work from anywhere — an HTTP handler, a cron, another goroutine.
    q.Enqueue(ctx, SendEmail, Email{To: "alice@example.com", Subject: "Welcome!"})

    if err := q.Start(ctx); err != nil {
        slog.Error("queue stopped", "error", err)
    }
}
```

Jobs persist across restarts, retry on failure, and survive `kill -9`. Zero configuration, zero infrastructure.

---

## 📦 Installation

```bash
go get github.com/didikizi/gjobs
```

> **Requirements:** Go 1.22+. Pure Go — no CGO, no GCC required.
> Cross-compiles to Linux from macOS: `GOOS=linux go build ./...`

---

## 🛡 Crash safety

**What happens when your process is killed mid-job?**

gjobs stores job state in SQLite. On every `Start()`, the queue calls `RecoverStuck()`, which resets any jobs left in `running` state back to `pending`. They will be picked up and re-executed by the next run.

```
Process A starts job → kill -9 → job stays "running" in DB
Process B starts     → RecoverStuck() → job becomes "pending" → re-executed ✓
```

This is tested: `TestRecoverStuck_QueueRestart` simulates a crash and verifies the job re-runs on restart.

**What about panics inside handlers?**

Worker goroutines recover from panics. A panicking handler does not kill the worker, does not leave the job stuck in `running`, and does not crash the process. The panic is converted to an error, `last_error` is set to `"panic: <message>\n<stack>"`, and the normal retry/dead-letter logic applies.

---

## 🏷️ Job definitions

Define jobs **once as typed package-level variables** — no magic strings anywhere.

```go
var (
    SendEmail      = gjobs.Def("send_email")
    ChargeCard     = gjobs.Def("charge_card").WithAttempts(10).WithTimeout(2*time.Minute)
    GenerateReport = gjobs.Def("generate_report").WithTimeout(15*time.Minute)
)
```

| Method | Description |
|--------|-------------|
| `.WithAttempts(n)` | Total execution attempts (default: 3). Pass `Unlimited` to retry forever. |
| `.WithTimeout(d)` | Cancel handler context after duration |
| `.WithBackoff(base, max)` | Override retry delays for this job |

---

## 🔧 Handlers

```go
// Typed (recommended) — payload is unmarshalled automatically.
gjobs.HandleDef[Email](q, SendEmail, func(ctx context.Context, e Email) error {
    return smtp.Send(e)
})

// Raw bytes — maximum control.
q.Register(SendEmail, func(ctx context.Context, payload []byte) error {
    var e Email
    json.Unmarshal(payload, &e)
    return smtp.Send(e)
})
```

> **Note:** All handlers must be registered before calling `Start`. Calling `Register` after `Start` panics.

---

## 📬 Pushing jobs

```go
ctx := context.Background()

q.Enqueue(ctx, SendEmail, Email{To: "alice@example.com"})

// Override attempts for a single push.
q.Enqueue(ctx, ChargeCard, payment, gjobs.Attempts(15))

// Delayed — run after 10 minutes.
q.Enqueue(ctx, SendEmail, data, gjobs.After(10*time.Minute))

// Scheduled — run at an exact time.
q.Enqueue(ctx, GenerateReport, data, gjobs.At(billingDate))
```

---

## 🔁 Retries & dead-letter

Failed jobs retry with **exponential backoff**: `base × 2^(attempt-1)`, capped at `max`.

| Attempt | Default delay (base=30s, cap=1h) |
|:-------:|:--------------------------------:|
| 1 | 30s |
| 2 | 1m |
| 3 | 2m |
| 4 | 4m |
| 5 | 8m |
| … | … (max 1h) |

After all retries are exhausted the job moves to the **dead-letter queue** (`status = 'failed'`).

```go
// Queue-level backoff defaults.
q, _ := gjobs.New(
    gjobs.WithBackoffBase(1 * time.Minute),
    gjobs.WithBackoffCap(6 * time.Hour),
)

// Per-job override via JobDef.
var HeavySync = gjobs.Def("heavy_sync").WithAttempts(5).WithBackoff(2*time.Minute, 12*time.Hour)
```

---

## 🔑 Deduplication keys

Attach a string key to a job and gjobs guarantees only one job per key is active at a time. Useful for "don't send the welcome email twice", debounced refresh jobs, or per-user one-at-a-time queues.

```go
// Default mode (Ignore) — second enqueue is silently skipped, returns nil.
q.Enqueue(ctx, SendWelcome, payload, gjobs.DedupKey("welcome:user-42"))
q.Enqueue(ctx, SendWelcome, payload, gjobs.DedupKey("welcome:user-42")) // no-op
```

**Replace mode** swaps the pending duplicate for the new payload. A *running* duplicate is left alone — it will either succeed or fail and retry, which covers the new enqueue automatically.

```go
q.Enqueue(ctx, RefreshCache, fresh, gjobs.DedupKey("cache:home"), gjobs.DedupReplace())
```

**TTL** extends the lock past completion. Within the window, new enqueues with the same key are still treated as duplicates. Time is measured from the moment of completion; storage granularity is one second.

```go
// One report per day, even if the trigger fires multiple times.
q.Enqueue(ctx, GenerateReport, args,
    gjobs.DedupKey("report:daily:2026-06-01"),
    gjobs.DedupTTL(24*time.Hour),
)
```

`DedupReplace` and `DedupTTL` without `DedupKey` are no-ops — `DedupKey` is the trigger.

---

## ⏱️ Delayed & scheduled jobs

```go
// Run once, 5 minutes from now.
q.Enqueue(ctx, Reminder, data, gjobs.After(5*time.Minute))

// Run at a specific moment.
q.Enqueue(ctx, Invoice, data, gjobs.At(time.Date(2025, 12, 1, 9, 0, 0, 0, time.UTC)))
```

The scheduled time is stored in the database — survives restarts.

---

## 🕐 Recurring jobs

```go
var Cleanup = gjobs.Def("cleanup")

q.Schedule(ctx, Cleanup, "1h", func(ctx context.Context) error {
    return db.DeleteExpired()
})
```

Schedule format: any Go duration string — `"5s"`, `"30m"`, `"2h"`, `"24h"`.

Schedules persist in the database. Missed runs fire once on restart.

> **Note:** This is interval-based scheduling, not cron expressions. The timer starts from when the job last ran. For `"0 9 * * MON"` semantics, use a cron library and call `Enqueue` from it.

---

## 🗄️ Storage backends

### SQLite (default)

```go
q, _ := gjobs.New()                             // → jobs.db in cwd
q, _ := gjobs.New(gjobs.WithDB("/data/jobs.db")) // custom path
```

Pure Go, WAL mode enabled. No CGO. Works with [Litestream](https://litestream.io) for streaming replication — just point Litestream at `jobs.db`.

### Memory — for tests

```go
q, _ := gjobs.New(gjobs.WithStorage(gjobs.NewMemoryStorage()))
```

No disk. Jobs lost on exit. Use in tests and CI.

> **Schema:** gjobs creates its tables automatically on first run. There are no migration tools — the schema is stable within a major version. If you need to reset, deleting `jobs.db` is safe; no other data is stored there.

---

## ⚙️ Configuration

```go
q, _ := gjobs.New(
    gjobs.WithDB("myapp.db"),
    gjobs.WithConcurrency(20),
    gjobs.WithPollInterval(200 * time.Millisecond),
    gjobs.WithShutdownTimeout(30 * time.Second),
    gjobs.WithLogger(slog.Default()),
)
```

| Option | Default | Description |
|--------|:-------:|-------------|
| `WithDB(path)` | `"jobs.db"` | SQLite file path |
| `WithConcurrency(n)` | `10` | Max parallel handlers |
| `WithPollInterval(d)` | `500ms` | Storage poll cadence |
| `WithShutdownTimeout(d)` | wait forever | Max time to drain in-flight jobs on shutdown |
| `WithStorage(s)` | — | Custom storage backend |
| `WithBackoffBase(d)` | `30s` | Initial retry delay |
| `WithBackoffCap(d)` | `1h` | Maximum retry delay |
| `WithLogger(l)` | stdlib log | Any `gjobs.Logger` — `*slog.Logger` works directly |
| `WithNoLogger()` | — | Disable all log output |
| `WithErrorChannel(ch)` | — | Receive `JobError` on every failure |

---

## 📊 Dashboard

```go
srv, err := q.Dashboard(":8080")

// With authentication (recommended for production).
srv, err := q.Dashboard(":8080", gjobs.WithDashboardAuth("admin", os.Getenv("DASH_PASSWORD")))
```

Open `http://localhost:8080` for a live view of pending, running, done, and failed jobs with a retry button.

`GET /stats.json` returns job counts as JSON — suitable for health checks and Prometheus scraping:

```json
{"Pending":3,"Running":1,"Done":142,"Failed":2}
```

The server shuts down automatically when the queue stops.

---

## 🔇 Logging & errors

`gjobs.Logger` matches `*slog.Logger` exactly — pass it directly:

```go
gjobs.New(gjobs.WithLogger(slog.Default()))
```

For programmatic error handling without any log output:

```go
errCh := make(chan gjobs.JobError, 64)
q, _ := gjobs.New(gjobs.WithNoLogger(), gjobs.WithErrorChannel(errCh))

go func() {
    for e := range errCh {
        if e.Final {
            alerting.DeadLetter(e) // exhausted all retries
        }
    }
}()
```

---

## 🆚 Comparison

|  | Infra | Multi-machine | Pure Go | Typed handlers | Cron | Dashboard | Dedup keys |
|--|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| **gjobs** | file | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |
| [liteq](https://github.com/khepin/liteq) | file | ❌ | ❌ (CGO) | ❌ | ❌ | ❌ | ✅ |
| [River](https://riverqueue.com) | Postgres | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| [asynq](https://github.com/hibiken/asynq) | Redis | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| [machinery](https://github.com/RichardKnop/machinery) | Redis / AMQP | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ |

gjobs sits between liteq and River/asynq: same zero-infrastructure footprint as liteq, with batteries-included ergonomics (typed handlers, cron, dashboard) that liteq doesn't include — trading the horizontal scalability of River/asynq.

---

## 🧪 Testing

```go
q, _ := gjobs.New(gjobs.WithStorage(gjobs.NewMemoryStorage()))

// Or use the configurable mock for unit tests.
import "github.com/didikizi/gjobs/testutil"

mock := testutil.NewMockStorage()
mock.ClaimFn = func(ctx context.Context, n int) ([]*gjobs.Job, error) { ... }
```

---

## 📄 License

MIT — see [LICENSE](LICENSE).
