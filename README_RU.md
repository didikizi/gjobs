<div align="center">

# ⚡ jobs

**Фоновые задачи для Go — без Redis, Docker и YAML.**

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat-square&logo=go&logoColor=white)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)](#лицензия)
[![SQLite](https://img.shields.io/badge/хранилище-SQLite%20%7C%20Postgres%20%7C%20Memory-blue?style=flat-square)](#-хранилища)
[![Tests](https://img.shields.io/badge/тесты-passing-brightgreen?style=flat-square)](#-тестирование)

Один файл. Одна база данных. Никакой инфраструктуры.

[Быстрый старт](#-быстрый-старт) · [Определение задач](#️-определение-задач) · [Хранилища](#-хранилища) · [Бенчмарки](#-бенчмарки) · [API](#-api)

</div>

---

## ✨ Быстрый старт

```go
var SendEmail = jobs.Def("send_email")

q, _ := jobs.New()

jobs.HandleDef[Email](q, SendEmail, func(ctx context.Context, e Email) error {
    return smtp.Send(e)
})
q.Enqueue(SendEmail, Email{To: "user@example.com"})

q.Start(ctx)
```

Задача сохраняется при перезапуске, повторяется при ошибке, корректно завершается — **без настройки**.

---

## 📦 Установка

```bash
go get github.com/vkorolev/gjobs
```

> **Требования:** Go 1.21+. Бекенд SQLite требует CGO (GCC в `PATH`).  
> Используйте `MemoryStorage` или `PostgresStorage` для сборки без CGO.

---

## 🏷️ Определение задач

Определяйте задачи **один раз как типизированные переменные** — никаких строковых констант, никаких повторяющихся опций.

```go
var (
    SendEmail      = jobs.Def("send_email")
    ChargeCard     = jobs.Def("charge_card").WithRetries(10).WithTimeout(2*time.Minute)
    GenerateReport = jobs.Def("generate_report").WithTimeout(15*time.Minute)
    Heartbeat      = jobs.Def("heartbeat")
)
```

`Def` возвращает `JobDef` с 3 повторами и без таймаута. Настраивается через цепочку методов:

| Метод | Описание |
|-------|----------|
| `.WithRetries(n)` | Максимальное количество попыток |
| `.WithTimeout(d)` | Отменить обработчик после истечения времени |
| `.WithBackoff(base, cap)` | Переопределить задержку повтора для этой задачи |

---

## 🔧 Обработчики

### Типизированные дженерики `HandleDef` (рекомендуется)

```go
jobs.HandleDef[Email](q, SendEmail, func(ctx context.Context, e Email) error {
    return smtp.Send(e)
})
```

### Сырые байты (максимальный контроль)

```go
q.Register(SendEmail, func(ctx context.Context, payload []byte) error {
    var e Email
    json.Unmarshal(payload, &e)
    return smtp.Send(e)
})
```

---

## 📬 Постановка задач в очередь

```go
// Используются дефолтные настройки (3 повтора, без таймаута)
q.Enqueue(SendEmail, Email{To: "alice@example.com", Subject: "Привет!"})

// Переопределение повторов для одного запуска
q.Enqueue(ChargeCard, payment, jobs.Retries(15))

// Отложенная задача — выполнить через 10 минут
q.Enqueue(SendEmail, data, jobs.After(10*time.Minute))

// По расписанию — выполнить в конкретное время
q.Enqueue(GenerateReport, data, jobs.At(billingDate))
```

---

## 🔁 Повторы и dead-letter очередь

Упавшие задачи повторяются с **экспоненциальным откатом**: `base × 2^attempt`, не больше `cap`.

| Попытка | Задержка по умолчанию (base=30s, cap=1h) |
|:-------:|:----------------------------------------:|
| 1 | 30s |
| 2 | 1m |
| 3 | 2m |
| 4 | 4m |
| 5 | 8m |
| … | … (максимум 1h) |

После исчерпания всех попыток задача переходит в **dead-letter очередь** (`status = 'failed'`).

**Дашборд** показывает точное время следующей попытки и обратный отсчёт ("in 4m 32s") для задач, ожидающих повтора.

### Настройка backoff

**Глобально** (применяется ко всем задачам, если не переопределено):

```go
q, _ := jobs.New(
    jobs.WithBackoffBase(1 * time.Minute), // по умолчанию: 30s
    jobs.WithBackoffCap(6 * time.Hour),    // по умолчанию: 1h
)
```

**Для конкретного типа задачи** через `JobDef`:

```go
// Быстрые повторы для лёгких задач
var Notify = jobs.Def("notify").WithBackoff(5*time.Second, 5*time.Minute)

// Медленные повторы для тяжёлых задач
var HeavySync = jobs.Def("heavy_sync").WithBackoff(2*time.Minute, 12*time.Hour)
```

### Неограниченное количество повторов

Передайте `jobs.Unlimited` (`-1`) для бесконечных повторов до успеха:

```go
q.Enqueue(Sync, data, jobs.Retries(jobs.Unlimited))

// или сразу в JobDef
var Sync = jobs.Def("sync").WithRetries(jobs.Unlimited)
```

---

## ⏱️ Отложенные и запланированные задачи

```go
// Выполнить один раз через 5 минут
q.Enqueue(Reminder, data, jobs.After(5*time.Minute))

// Выполнить в конкретный момент
q.Enqueue(Invoice, data, jobs.At(time.Date(2025, 12, 1, 9, 0, 0, 0, time.UTC)))
```

Время хранится в базе данных — пережит перезапуск.

---

## 🕐 Cron-задачи

```go
var Cleanup = jobs.Def("cleanup")

q.Schedule(Cleanup, "1h", func(ctx context.Context) error {
    return db.DeleteExpired()
})
```

Формат расписания: любая строка Go-длительности — `"5s"`, `"30m"`, `"2h"`, `"24h"`.

Состояние cron сохраняется в базе. Пропущенные запуски срабатывают один раз при рестарте.

---

## 🗄️ Хранилища

| Бекенд | Сохранность | Мульти-процесс | Применение |
|--------|:-----------:|:--------------:|------------|
| **SQLite** (по умолчанию) | ✅ | ❌ | Продакшн на одной машине |
| **Memory** | ❌ | ❌ | Юнит-тесты, разработка |
| **PostgreSQL** | ✅ | ✅ | Высокая нагрузка, несколько инстансов |

### SQLite (по умолчанию)

```go
q, _ := jobs.New()                              // → jobs.db в текущей директории
q, _ := jobs.New(jobs.WithDB("/data/jobs.db"))  // свой путь
```

Режим WAL включён. Один писатель одновременно.

### Memory

```go
q, _ := jobs.New(jobs.WithStorage(jobs.NewMemoryStorage()))
```

Без диска, без CGO. Задачи теряются при выходе. Идеально для тестов.

### PostgreSQL

```go
pg, err := jobs.NewPostgresStorage(ctx, "postgres://user:pass@host/db?sslmode=disable")
q, _ := jobs.New(jobs.WithStorage(pg))
```

Использует `FOR UPDATE SKIP LOCKED` для конкурентного захвата задач.
Несколько процессов могут безопасно работать с одной базой.

---

## ⚙️ Конфигурация

```go
q, _ := jobs.New(
    jobs.WithDB("myapp.db"),
    jobs.WithConcurrency(20),
    jobs.WithPollInterval(200 * time.Millisecond),
)
```

| Опция | По умолчанию | Описание |
|-------|:-----------:|----------|
| `WithDB(path)` | `"jobs.db"` | Путь к файлу SQLite |
| `WithConcurrency(n)` | `10` | Макс. параллельных обработчиков |
| `WithPollInterval(d)` | `500ms` | Частота опроса хранилища |
| `WithStorage(s)` | — | Кастомный бекенд хранилища |
| `WithBackoffBase(d)` | `30s` | Начальная задержка повтора |
| `WithBackoffCap(d)` | `1h` | Максимальная задержка повтора |
| `WithLogger(l)` | `stdLogger` (stdout) | Кастомный логгер |
| `WithNoLogger()` | — | Отключить весь вывод логов |
| `WithErrorChannel(ch)` | — | Получать ошибки задач в канал |

---

## 🪵 Логирование

По умолчанию библиотека пишет в `log.Printf`. Подставьте любой логгер, удовлетворяющий интерфейсу:

```go
type Logger interface {
    Info(msg string, args ...any)
    Error(msg string, args ...any)
}
```

**Использование `log/slog`:**

```go
q, _ := jobs.New(jobs.WithLogger(slog.Default()))
```

**Использование zap** (через тонкий адаптер):

```go
type zapAdapter struct{ *zap.SugaredLogger }
func (z zapAdapter) Info(msg string, args ...any)  { z.SugaredLogger.Infof(msg, args...) }
func (z zapAdapter) Error(msg string, args ...any) { z.SugaredLogger.Errorf(msg, args...) }

q, _ := jobs.New(jobs.WithLogger(zapAdapter{sugar}))
```

**Отключить весь вывод** и обрабатывать ошибки через канал:

```go
q, _ := jobs.New(jobs.WithNoLogger(), jobs.WithErrorChannel(errCh))
```

---

## 📡 Канал ошибок

Получайте `JobError` при каждом сбое без зависимости от логов:

```go
errCh := make(chan jobs.JobError, 64)

q, _ := jobs.New(jobs.WithErrorChannel(errCh))

go func() {
    for e := range errCh {
        if e.Final {
            alerting.DeadLetter(e) // больше попыток нет — принять меры
        } else {
            metrics.Inc("job.retry", e.Type)
        }
    }
}()
```

Поля `JobError`:

| Поле | Тип | Описание |
|------|-----|----------|
| `JobID` | `string` | ID задачи в базе данных |
| `Type` | `string` | Имя типа задачи |
| `Err` | `error` | Ошибка, возвращённая обработчиком |
| `Attempt` | `int` | Номер попытки (с 1) |
| `Final` | `bool` | `true` если задача попала в dead-letter |

Отправка **неблокирующая** — если канал заполнен, ошибка логируется и отбрасывается.

---

## 🖥️ Веб-дашборд

Запустите встроенный дашборд одной строкой:

```go
srv, err := q.Dashboard(":8080")
```

Откройте `http://localhost:8080` и вы увидите:

- **Живую статистику** — количество задач по статусам (автообновление каждые 5 с)
- **Таблицу задач** — с фильтрацией по статусу и пагинацией (50 задач на страницу)
- **Кнопку Retry** — поставить упавшую задачу в очередь повторно прямо из UI

Дашборд сервер останавливается автоматически при остановке очереди. Это опциональная функция без внешних зависимостей — только стандартная библиотека `net/http` и `html/template`.

Кастомные бекенды поддерживают дашборд, реализовав три дополнительных метода:

```go
type DashboardStorage interface {
    Stats(ctx context.Context) (JobStats, error)
    Jobs(ctx context.Context, status Status, limit, offset int) ([]*Job, error)
    RetryJob(ctx context.Context, id string) error
}
```

---

## ⏹️ Отмена запущенных задач

Отменить все выполняющиеся задачи заданного типа одной строкой:

```go
n := q.CancelAll(SendEmail)
fmt.Printf("отменено %d задач send_email\n", n)
```

Каждый работающий обработчик получит `ctx.Err() == context.Canceled`. Обычная логика повторов сохраняется — задача будет поставлена в очередь заново, если остались попытки, или перейдёт в dead-letter, если они исчерпаны.

Задачи в статусе pending (ещё не взятые воркером) не затрагиваются.

---

## 🛑 Graceful shutdown

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

q.Start(ctx) // блокирует; корректно завершается по SIGTERM
```

`Start` отменяется → останавливает опрос → ждёт завершения текущих задач → закрывает хранилище.

---

## 📂 Примеры

| Пример | Описание |
|--------|----------|
| [examples/basic](examples/basic/main.go) | Минимальная настройка — типизированные обработчики, cron, отложенные задачи |
| [examples/jobdef](examples/jobdef/main.go) | `JobDef` с кастомными retry, timeout, backoff |
| [examples/errors](examples/errors/main.go) | Кастомный логгер (slog), канал ошибок, неограниченные повторы |
| [examples/dashboard](examples/dashboard/main.go) | Веб-дашборд — живая статистика, retry, отложенные задачи |
| [examples/postgres](examples/postgres/main.go) | Бекенд PostgreSQL, мульти-процессная настройка |

---

## 📊 Бенчмарки

> Intel Xeon E5-2697 v3 @ 2.60GHz · Go 1.26 · SQLite WAL · Linux (WSL2)

### Пропускная способность

| Payload | Размер | ops/sec | µs/op | B/op |
|---------|-------:|--------:|------:|-----:|
| Уведомление | 64 B | ~127k | 26µs | 1 104 |
| Email | ~700 B | ~155k | 27µs | 1 104 |
| Платёж | ~400 B | ~141k | 26µs | 1 104 |
| Параметры отчёта | ~5 KB | ~132k | 24µs | 1 104 |

Стоимость постановки в очередь определяется латентностью записи SQLite, а не размером payload. Все типы задач записывают ~130–155k/сек.

### Полный цикл (push → обработчик → done)

| Payload | Воркеры | jobs/sec | µs/job | allocs/op |
|---------|:-------:|--------:|------:|----------:|
| Уведомление | 10 | ~20k | 160µs | 69 |
| Email | 10 | ~17k | 186µs | 76 |
| Платёж | 20 | ~27k | 152µs | 75 |
| Параметры отчёта | 5 | ~10k | 328µs | 81 |

### Масштабирование конкурентности

| Воркеры | jobs/sec | µs/job |
|:-------:|--------:|------:|
| 1 | ~2.6k | 1 300µs |
| 10 | ~27k | 138µs |
| 50 | ~29k | 128µs |

Переход с 10 до 50 воркеров даёт лишь ~5% прироста — узкое место в SQLite (один писатель).
Для высокой параллельности используйте **PostgreSQL**.

---

## 🧪 Тестирование

### Юнит-тесты (без инфраструктуры)

```bash
go test ./...
```

Используют in-memory SQLite и `MemoryStorage` — работают везде.

### Интеграционные тесты с PostgreSQL

```bash
# Запуск Postgres
docker compose up -d

# Запуск тестов
JOBS_TEST_POSTGRES="postgres://jobs:jobs@localhost:5432/jobs_test?sslmode=disable" \
  go test ./...

# Остановка
docker compose down
```

### Mock хранилище для ваших тестов

```go
import "github.com/vkorolev/gjobs/testutil"

mock := testutil.NewMockStorage()

mock.ClaimFn = func(ctx context.Context, limit int) ([]*jobs.Job, error) {
    return []*jobs.Job{{ID: "1", Type: "send_email", MaxRetries: 3}}, nil
}

q, _ := jobs.New(jobs.WithStorage(mock))

var SendEmail = jobs.Def("send_email")
q.Register(SendEmail, myHandler)

// ... запуск очереди ...

if calls := mock.CallsFor("MarkDone"); len(calls) != 1 {
    t.Errorf("ожидался 1 вызов MarkDone, получено %d", len(calls))
}
```

---

## 📖 API

### Создание очереди

```go
jobs.New(opts ...Option) (*Queue, error)
```

### Регистрация и запуск

```go
// Регистрация
q.Register(def JobDef, handler HandlerFunc)
jobs.HandleDef[T](q *Queue, def JobDef, fn func(ctx, T) error)

// Постановка в очередь
q.Enqueue(def JobDef, payload any, opts ...PushOption) error

// Cron
q.Schedule(def JobDef, interval string, fn func(ctx) error) error

// Отменить все запущенные задачи типа; возвращает количество отменённых
q.CancelAll(def JobDef) int
```

### Опции постановки в очередь

| Опция | По умолчанию | Описание |
|-------|:-----------:|----------|
| `Retries(n)` | `3` | Макс. попыток повтора |
| `After(d)` | немедленно | Отложить на длительность |
| `At(t)` | немедленно | Запланировать на время |

### Интерфейс Storage

Реализуйте для добавления любого бекенда (MySQL, Redis, …):

```go
type Storage interface {
    Enqueue(ctx context.Context, job *Job) error
    Claim(ctx context.Context, limit int) ([]*Job, error)
    MarkDone(ctx context.Context, id string) error
    MarkFailed(ctx context.Context, id string, errMsg string, retryAt *time.Time) error
    MarkPending(ctx context.Context, id string, runAt time.Time) error
    UpsertCron(ctx context.Context, c *CronEntry) error
    DueCrons(ctx context.Context) ([]*CronEntry, error)
    UpdateCronRun(ctx context.Context, name string, last, next time.Time) error
    Close() error
}
```

---

## 🗺️ Дорожная карта

| Статус | Фича |
|:------:|------|
| ✅ | SQLite, пул воркеров, повторы |
| ✅ | Отложенные задачи, cron, graceful shutdown |
| ✅ | Memory + PostgreSQL бекенды |
| ✅ | Интерфейс Logger, канал ошибок, неограниченные повторы |
| ✅ | Веб-дашборд (`q.Dashboard(":8080")`) |
| ✅ | `CancelAll` — отмена всех запущенных задач по типу |
| 🔜 | Batch push, приоритеты задач |
| 🔜 | MySQL бекенд |
| 🔜 | Стабильный API |

---

## 📄 Лицензия

MIT
