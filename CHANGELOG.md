# Changelog

All notable changes to this project will be documented in this file.

## [0.4.1] - 2026-05-30

### Breaking Changes

- **Package renamed from `jobs` to `gjobs`.**
  Update all call sites: `import "github.com/didikizi/gjobs"` already
  gives you the `gjobs` identifier — no alias needed.
  Before: `jobs.New(...)` → After: `gjobs.New(...)`.
- **`Job.MaxRetries` renamed to `Job.MaxAttempts`.**
  `JobDef.MaxRetries` → `JobDef.MaxAttempts`,
  `JobDef.WithRetries(n)` → `JobDef.WithAttempts(n)`,
  `Retries(n) PushOption` → `Attempts(n) PushOption`.
  The field now unambiguously means *total execution attempts* (not
  retries after the first). Semantics unchanged: default 3 = 3 attempts.
- **`WithBackoff(base, cap)`** second parameter renamed to **`max`.**
  `cap` shadowed the Go builtin of the same name.

### Fixed

- **`storage.go` was missing from the v0.4.0 commit** — `Storage`,
  `DashboardStorage`, and `JobStats` were extracted from `queue.go` and
  `dashboard.go` but the new file was not staged, causing 43 compile
  errors on CI. This release adds the file.
- **Quick Start example in README did not compile:**
  missing `"time"` import and `ctx` used before its declaration.
  `ctx, stop := signal.NotifyContext(...)` now appears before `q.Enqueue`.

### Changed

- **`examples/basic`** reduced from 107 to 38 lines — one job type,
  no noise. Complex patterns stay in `examples/jobdef` and
  `examples/errors`.
- **README** — schema note added: deleting `jobs.db` is safe; no
  migration tooling is planned within a major version.

---

## [0.4.0] - 2026-05-30

### Breaking Changes

- **PostgreSQL backend removed.** The `github.com/didikizi/gjobs/postgres`
  subpackage and `examples/postgres` are deleted. gjobs is now a
  single-machine, SQLite-only library. Users who need multi-machine
  processing should use [River](https://riverqueue.com) or
  [asynq](https://github.com/hibiken/asynq).

### Changed

- **Minimum Go version lowered to 1.22** (was 1.25).
  Downgraded `modernc.org/sqlite` to v1.36.1 — the last version whose
  full transitive dependency graph stays within Go 1.22.
  Removed `pgx/v5` from the module entirely.
- **CI matrix** now tests on Go 1.22, 1.23, 1.24, and 1.25.
- **Project layout:** `Storage`, `DashboardStorage`, and `JobStats`
  extracted from `queue.go`/`dashboard.go` into a dedicated `storage.go`.
- **README** — removed `README_RU.md`; English README is the single
  source of truth.

### Fixed

- **Timing attack in dashboard `basicAuth`:** comparisons now use
  `crypto/subtle.ConstantTimeCompare` instead of `==`.
- **Flaky test:** `TestRecoverStuck_QueueRestart` deadline raised
  2s → 5s to prevent spurious failures under `-race` on loaded CI runners.

### Added

- `.github/social-preview.svg` — 1280×640 social preview image.

---

## [0.3.0] - 2026-05-27

### Breaking Changes

- **`Enqueue` and `Schedule` now accept `context.Context` as first argument.**
  Migrate: `q.Enqueue(def, payload)` → `q.Enqueue(ctx, def, payload)`
- **Logger interface uses key-value pairs (slog-style), not Printf format.**
  `*slog.Logger` now satisfies `jobs.Logger` directly — no adapter needed.
- **`PostgresStorage` moved to `github.com/didikizi/gjobs/postgres` subpackage.**
  Migrate: `jobs.NewPostgresStorage(ctx, dsn)` → `postgres.New(ctx, dsn)`

### Fixed

- **Stuck jobs on crash:** `Start()` now calls `RecoverStuck()` which resets any
  jobs left in `running` state back to `pending`. Jobs survive `kill -9`.
- **Panic in handler:** A panicking handler no longer kills the worker goroutine
  or leaves the job stuck in `running`. The panic is caught, turned into an error,
  and the normal retry/dead-letter logic applies. `last_error` contains `"panic: ..."`.
- **Backoff formula:** Fixed `base × 2^attempt` → `base × 2^(attempt-1)` so that
  attempt 1 = 30s, attempt 2 = 1m, attempt 3 = 2m.
- **`MarkDone` context race:** Bookkeeping calls now use `context.Background()`
  so a concurrent `CancelAll` can no longer prevent status updates from reaching storage.
- **Shutdown deadline:** `Start()` no longer calls `Stop(context.Background())`.
  Use the new `WithShutdownTimeout` option to set a graceful drain deadline.
- **Register after Start:** Calling `Register` after `Start` now panics with a
  clear message instead of silently having no effect.

### Added

- `Storage.RecoverStuck(ctx)` — new interface method; all built-in backends implement it.
- `WithShutdownTimeout(d)` option.
- `WithDashboardAuth(username, password)` — HTTP Basic Auth for the dashboard.
- `GET /stats.json` endpoint on the dashboard.
- Cancellable context in `workerPool.poll()`.

### Changed

- **SQLite driver:** Replaced `mattn/go-sqlite3` (CGO) with `modernc.org/sqlite`
  (pure Go). No GCC required. Cross-compilation from macOS works out of the box.
- **SQLite `Claim`:** Removed unnecessary `BEGIN`/`COMMIT` wrapper.

---

## [0.2.0] - 2026-05-26

### Added
- SQLite storage backend (WAL mode, CGO via `mattn/go-sqlite3`)
- PostgreSQL storage backend (`FOR UPDATE SKIP LOCKED`, multi-process safe)
- In-memory storage backend for tests and local development
- `JobDef` typed job descriptor — `Def(name)` with chainable `.WithRetries()`, `.WithTimeout()`, `.WithBackoff()`
- `HandleDef[T]` generic helper for typed handler registration
- Exponential backoff with configurable base and cap (per-queue and per-job)
- `Unlimited` sentinel (`-1`) for infinite retries
- Delayed jobs — `After(d)` and `At(t)` push options
- Cron scheduler — persistent in database, fires missed runs on restart
- Graceful shutdown — waits for in-flight handlers before closing storage
- `CancelAll(def)` — cancel all running jobs of a given type
- Web dashboard — live stats, job table with pagination, retry button
- Error channel (`WithErrorChannel`) — receive `JobError` on every failure
- Logger interface — stdlib `log`, `slog`, zap adapter supported
- `testutil.MockStorage` — configurable mock for unit tests
