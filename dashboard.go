package jobs

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"strconv"
	"time"
)

// DashboardStorage extends Storage with listing and retry capabilities used
// by the web dashboard. All built-in backends (SQLite, Memory, Postgres)
// implement this interface. Custom backends can add these three methods to
// unlock q.Dashboard().
type DashboardStorage interface {
	// Stats returns per-status job counts.
	Stats(ctx context.Context) (JobStats, error)
	// Jobs returns jobs ordered by updated_at DESC. status="" means all statuses.
	Jobs(ctx context.Context, status Status, limit, offset int) ([]*Job, error)
	// RetryJob moves a failed job back to pending with attempts reset to 0.
	RetryJob(ctx context.Context, id string) error
}

// JobStats holds per-status job counts.
type JobStats struct {
	Pending int
	Running int
	Done    int
	Failed  int
}

// DashboardOption configures the dashboard HTTP server.
type DashboardOption func(*dashConfig)

type dashConfig struct {
	username string
	password string
}

// WithDashboardAuth enables HTTP Basic Auth on the dashboard.
// Set DASH_PASSWORD via an environment variable; never hard-code credentials.
//
//	q.Dashboard(":8080", jobs.WithDashboardAuth("admin", os.Getenv("DASH_PASSWORD")))
func WithDashboardAuth(username, password string) DashboardOption {
	return func(c *dashConfig) {
		c.username = username
		c.password = password
	}
}

func basicAuth(username, password string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		userOK := subtle.ConstantTimeCompare([]byte(u), []byte(username)) == 1
		passOK := subtle.ConstantTimeCompare([]byte(p), []byte(password)) == 1
		if !ok || !userOK || !passOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="jobs dashboard"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Dashboard starts an HTTP server on addr and returns it. The server is
// automatically stopped when the queue is stopped. Returns an error if the
// storage backend does not implement DashboardStorage.
//
//	srv, err := q.Dashboard(":8080")
//	srv, err := q.Dashboard(":8080", jobs.WithDashboardAuth("admin", os.Getenv("DASH_PASSWORD")))
func (q *Queue) Dashboard(addr string, opts ...DashboardOption) (*http.Server, error) {
	cfg := &dashConfig{}
	for _, o := range opts {
		o(cfg)
	}
	ds, ok := q.storage.(DashboardStorage)
	if !ok {
		return nil, fmt.Errorf("jobs: storage does not implement DashboardStorage; use SQLiteStorage, MemoryStorage, or PostgresStorage")
	}

	tmpl, err := template.New("dash").Funcs(template.FuncMap{
		"inc": func(n int) int { return n + 1 },
		"dec": func(n int) int { return n - 1 },
		"short": func(s string) string {
			if len(s) >= 8 {
				return s[:8]
			}
			return s
		},
		"retries": func(n int) string {
			if n < 0 {
				return "∞"
			}
			return strconv.Itoa(n)
		},
		"fmtTime": func(t time.Time) string {
			return t.Local().Format("2006-01-02 15:04:05")
		},
		"isFuture": func(t time.Time) bool {
			return t.After(time.Now())
		},
		"relTime": func(t time.Time) string {
			d := time.Until(t)
			if d <= 0 {
				return ""
			}
			if d < time.Minute {
				return fmt.Sprintf("in %ds", int(d.Seconds()))
			}
			if d < time.Hour {
				m := int(d.Minutes())
				s := int(d.Seconds()) % 60
				if s == 0 {
					return fmt.Sprintf("in %dm", m)
				}
				return fmt.Sprintf("in %dm %ds", m, s)
			}
			h := int(d.Hours())
			m := int(d.Minutes()) % 60
			if m == 0 {
				return fmt.Sprintf("in %dh", h)
			}
			return fmt.Sprintf("in %dh %dm", h, m)
		},
	}).Parse(dashboardTmpl)
	if err != nil {
		return nil, fmt.Errorf("jobs: parse dashboard template: %w", err)
	}

	h := &dashHandler{storage: ds, tmpl: tmpl}
	mux := http.NewServeMux()
	mux.HandleFunc("/", h.index)
	mux.HandleFunc("/retry", h.retry)
	mux.HandleFunc("/stats.json", h.statsJSON)

	var handler http.Handler = mux
	if cfg.username != "" {
		handler = basicAuth(cfg.username, cfg.password, mux)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("jobs: dashboard listen %s: %w", addr, err)
	}

	srv := &http.Server{Handler: handler}
	go srv.Serve(ln) //nolint:errcheck

	q.mu.Lock()
	q.stopDash = func(ctx context.Context) { srv.Shutdown(ctx) } //nolint:errcheck
	q.mu.Unlock()

	return srv, nil
}

const dashPageSize = 50

type dashHandler struct {
	storage DashboardStorage
	tmpl    *template.Template
}

type dashData struct {
	Stats        JobStats
	Jobs         []*Job
	StatusFilter string
	Page         int
	HasPrev      bool
	HasNext      bool
}

func (h *dashHandler) index(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	sf := r.URL.Query().Get("status")
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * dashPageSize

	stats, err := h.storage.Stats(ctx)
	if err != nil {
		http.Error(w, "stats: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Fetch one extra to detect whether a next page exists.
	jobs, err := h.storage.Jobs(ctx, Status(sf), dashPageSize+1, offset)
	if err != nil {
		http.Error(w, "list: "+err.Error(), http.StatusInternalServerError)
		return
	}

	hasNext := len(jobs) > dashPageSize
	if hasNext {
		jobs = jobs[:dashPageSize]
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = h.tmpl.Execute(w, dashData{
		Stats:        stats,
		Jobs:         jobs,
		StatusFilter: sf,
		Page:         page,
		HasPrev:      page > 1,
		HasNext:      hasNext,
	})
}

func (h *dashHandler) statsJSON(w http.ResponseWriter, r *http.Request) {
	stats, err := h.storage.Stats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats) //nolint:errcheck
}

func (h *dashHandler) retry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.FormValue("id")
	sf := r.FormValue("status")
	pg := r.FormValue("page")
	if pg == "" {
		pg = "1"
	}

	if err := h.storage.RetryJob(r.Context(), id); err != nil {
		http.Error(w, "retry: "+err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/?status="+sf+"&page="+pg, http.StatusSeeOther)
}

const dashboardTmpl = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>jobs · dashboard</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f1f5f9;color:#1e293b;font-size:14px}
a{text-decoration:none;color:inherit}
.header{background:#0f172a;color:#f8fafc;height:52px;display:flex;align-items:center;padding:0 24px;gap:10px}
.logo{font-size:17px;font-weight:700;letter-spacing:-.4px}
.logo-sub{font-size:12px;color:#64748b;padding:2px 8px;background:#1e293b;border-radius:4px}
.hright{margin-left:auto;font-size:12px;color:#475569;display:flex;align-items:center;gap:6px}
.dot{width:7px;height:7px;border-radius:50%;background:#22c55e;animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
.wrap{max-width:1180px;margin:0 auto;padding:22px 20px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:22px}
.sc{background:#fff;border-radius:10px;padding:15px 18px;box-shadow:0 1px 3px rgba(0,0,0,.07);border-top:3px solid transparent}
.sc.sp{border-color:#f59e0b}.sc.sr{border-color:#10b981}.sc.sd{border-color:#94a3b8}.sc.sf{border-color:#ef4444}
.slabel{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.7px;margin-bottom:7px}
.sc.sp .slabel{color:#b45309}.sc.sr .slabel{color:#047857}.sc.sd .slabel{color:#64748b}.sc.sf .slabel{color:#b91c1c}
.snum{font-size:28px;font-weight:700;line-height:1}
.sc.sp .snum{color:#f59e0b}.sc.sr .snum{color:#10b981}.sc.sd .snum{color:#94a3b8}.sc.sf .snum{color:#ef4444}
.panel{background:#fff;border-radius:10px;box-shadow:0 1px 3px rgba(0,0,0,.07);overflow:hidden}
.tabs{display:flex;border-bottom:1px solid #e2e8f0;padding:0 6px}
.tab{display:block;padding:11px 16px;font-size:13px;font-weight:500;color:#64748b;border-bottom:2px solid transparent;margin-bottom:-1px;transition:color .12s}
.tab:hover{color:#3b82f6}
.tab.on{color:#3b82f6;border-bottom-color:#3b82f6}
table{width:100%;border-collapse:collapse}
thead th{background:#f8fafc;padding:9px 14px;text-align:left;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.6px;color:#94a3b8;white-space:nowrap;border-bottom:1px solid #e2e8f0}
tbody td{padding:10px 14px;border-top:1px solid #f1f5f9;vertical-align:middle}
tbody tr:hover{background:#fafbff}
.badge{display:inline-block;padding:2px 9px;border-radius:20px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.4px}
.b-pending{background:#fef3c7;color:#92400e}
.b-running{background:#dcfce7;color:#14532d}
.b-done{background:#f1f5f9;color:#475569}
.b-failed{background:#fee2e2;color:#991b1b}
.mono{font-family:ui-monospace,"Cascadia Code",monospace;font-size:12px;color:#94a3b8}
.err{font-size:12px;color:#dc2626;max-width:210px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:block}
.ttime{font-size:12px;color:#64748b;white-space:nowrap;font-family:ui-monospace,monospace}
.rbtn{padding:3px 11px;font-size:12px;font-weight:500;border:1px solid #dc2626;border-radius:5px;background:#fff;color:#dc2626;cursor:pointer;transition:all .12s}
.rbtn:hover{background:#dc2626;color:#fff}
.empty{padding:52px 20px;text-align:center;color:#94a3b8;font-size:14px}
.pager{display:flex;align-items:center;justify-content:space-between;padding:11px 14px;background:#f8fafc;border-top:1px solid #e2e8f0}
.pinfo{font-size:12px;color:#94a3b8}
.pbtns{display:flex;gap:6px}
.pbtn{display:block;padding:5px 14px;font-size:13px;border:1px solid #e2e8f0;border-radius:6px;background:#fff;color:#3b82f6;font-weight:500;transition:all .12s}
.pbtn:hover{background:#3b82f6;color:#fff;border-color:#3b82f6}
</style>
</head>
<body>
<div class="header">
  <span class="logo">&#9889; jobs</span>
  <span class="logo-sub">dashboard</span>
  <div class="hright"><span class="dot"></span> live &middot; refresh 5s</div>
</div>
<div class="wrap">
  <div class="stats">
    <div class="sc sp"><div class="slabel">Pending</div><div class="snum">{{.Stats.Pending}}</div></div>
    <div class="sc sr"><div class="slabel">Running</div><div class="snum">{{.Stats.Running}}</div></div>
    <div class="sc sd"><div class="slabel">Done</div><div class="snum">{{.Stats.Done}}</div></div>
    <div class="sc sf"><div class="slabel">Failed</div><div class="snum">{{.Stats.Failed}}</div></div>
  </div>
  <div class="panel">
    <div class="tabs">
      <a class="tab{{if eq .StatusFilter ""}} on{{end}}"        href="?status=">All</a>
      <a class="tab{{if eq .StatusFilter "pending"}} on{{end}}" href="?status=pending">Pending</a>
      <a class="tab{{if eq .StatusFilter "running"}} on{{end}}" href="?status=running">Running</a>
      <a class="tab{{if eq .StatusFilter "done"}} on{{end}}"    href="?status=done">Done</a>
      <a class="tab{{if eq .StatusFilter "failed"}} on{{end}}"  href="?status=failed">Failed</a>
    </div>
    <table>
      <thead>
        <tr>
          <th>ID</th><th>Type</th><th>Status</th><th>Attempts</th><th>Run At</th><th>Last Error</th><th></th>
        </tr>
      </thead>
      <tbody>
      {{range .Jobs}}
        <tr>
          <td><span class="mono">{{short .ID}}</span></td>
          <td><b>{{.Type}}</b></td>
          <td><span class="badge b-{{.Status}}">{{.Status}}</span></td>
          <td class="mono">{{.Attempts}}&thinsp;/&thinsp;{{retries .MaxRetries}}</td>
          <td class="ttime">
            {{fmtTime .RunAt}}
            {{if and (eq .Status "pending") (isFuture .RunAt)}}<br><span style="color:#f59e0b;font-size:11px;font-weight:600">{{relTime .RunAt}}</span>{{end}}
          </td>
          <td>{{if .LastError}}<span class="err" title="{{.LastError}}">{{.LastError}}</span>{{end}}</td>
          <td>{{if eq .Status "failed"}}
            <form method="POST" action="/retry" style="margin:0">
              <input type="hidden" name="id"     value="{{.ID}}">
              <input type="hidden" name="status" value="{{$.StatusFilter}}">
              <input type="hidden" name="page"   value="{{$.Page}}">
              <button class="rbtn" type="submit">Retry</button>
            </form>
          {{end}}</td>
        </tr>
      {{else}}
        <tr><td colspan="7"><div class="empty">No jobs found.</div></td></tr>
      {{end}}
      </tbody>
    </table>
    <div class="pager">
      <span class="pinfo">Page {{.Page}}</span>
      <div class="pbtns">
        {{if .HasPrev}}<a class="pbtn" href="?status={{.StatusFilter}}&page={{dec .Page}}">&#8592; Prev</a>{{end}}
        {{if .HasNext}}<a class="pbtn" href="?status={{.StatusFilter}}&page={{inc .Page}}">Next &#8594;</a>{{end}}
      </div>
    </div>
  </div>
</div>
<script>setTimeout(function(){location.reload()},5000)</script>
</body>
</html>`
