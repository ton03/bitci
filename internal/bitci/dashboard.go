package bitci

import (
	"fmt"
	"html/template"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type dashboardPage struct {
	Now            time.Time
	Jobs           []dashboardJob
	Counts         []dashboardCount
	PassedLastWeek int
	AveragePassed  string
	DiskFree       string
	DiskGuard      string
	Resources      []resourceUsage
}

type dashboardJob struct {
	Job
	QueueWait     string
	StageDuration string
	Timeout       string
	Resources     string
}

type dashboardCount struct {
	State string
	Count int
}

type resourceUsage struct {
	Name  string
	InUse int
	Limit int
}

func listenDashboard(address string) (net.Listener, error) {
	if err := validateDashboardAddress(address); err != nil {
		return nil, err
	}
	return net.Listen("tcp", address)
}

func validateDashboardAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		return fmt.Errorf("dashboard address must be 127.0.0.1:PORT")
	}
	value, err := strconv.Atoi(port)
	if err != nil || value < 1 || value > 65535 {
		return fmt.Errorf("dashboard address must use a valid port")
	}
	return nil
}

func (controller *Controller) DashboardHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", controller.dashboard)
	mux.HandleFunc("GET /jobs/{id}/logs", controller.dashboardLogs)
	return mux
}

func (controller *Controller) dashboard(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/" {
		http.NotFound(writer, request)
		return
	}
	page, err := controller.dashboardData(time.Now().UTC())
	if err != nil {
		http.Error(writer, "BitCI could not read local state.", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashboardTemplate.Execute(writer, page); err != nil {
		return
	}
}

func (controller *Controller) dashboardLogs(writer http.ResponseWriter, request *http.Request) {
	id, err := strconv.ParseInt(request.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		http.NotFound(writer, request)
		return
	}
	lines, err := controller.TailLog(id, 80)
	if err != nil {
		http.Error(writer, "BitCI could not read this log.", http.StatusNotFound)
		return
	}
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = writer.Write([]byte(strings.Join(lines, "\n")))
}

func (controller *Controller) dashboardData(now time.Time) (dashboardPage, error) {
	jobs, err := controller.Jobs()
	if err != nil {
		return dashboardPage{}, err
	}
	page := dashboardPage{
		Now:       now,
		Counts:    make([]dashboardCount, 0, 5),
		DiskGuard: byteSize(controller.config.MinFreeBytes),
	}
	counts := map[string]int{"queued": 0, "running": 0, "passed": 0, "failed": 0, "cancelled": 0}
	var passed time.Duration
	for _, job := range jobs {
		counts[job.State]++
		started := parseJobTime(job.StartedAt)
		finished := parseJobTime(job.FinishedAt)
		created := parseJobTime(job.CreatedAt)
		jobView := dashboardJob{
			Job:           job,
			QueueWait:     elapsed(created, started, now),
			StageDuration: elapsed(started, finished, now),
			Timeout:       "unavailable",
			Resources:     "unavailable",
		}
		if _, task, err := controller.jobConfig(job); err == nil {
			jobView.Timeout = timeout(task.Timeout)
			jobView.Resources = strings.Join(task.Resources, ", ")
			if jobView.Resources == "" {
				jobView.Resources = "—"
			}
		}
		page.Jobs = append(page.Jobs, jobView)
		if job.State == "passed" && !finished.IsZero() && !started.IsZero() && finished.After(now.Add(-7*24*time.Hour)) {
			page.PassedLastWeek++
			passed += finished.Sub(started)
		}
	}
	for _, state := range []string{"queued", "running", "passed", "failed", "cancelled"} {
		page.Counts = append(page.Counts, dashboardCount{State: state, Count: counts[state]})
	}
	if page.PassedLastWeek > 0 {
		page.AveragePassed = duration(passed / time.Duration(page.PassedLastWeek))
	} else {
		page.AveragePassed = "—"
	}
	free, err := controller.diskFree()
	if err != nil {
		return dashboardPage{}, err
	}
	page.DiskFree = byteSize(free)
	page.Resources, err = controller.resourceUsage()
	return page, err
}

func (controller *Controller) resourceUsage() ([]resourceUsage, error) {
	names := make([]string, 0, len(controller.config.Resources))
	for name := range controller.config.Resources {
		names = append(names, name)
	}
	sort.Strings(names)
	rows, err := controller.db.Query("SELECT resource, COUNT(*) FROM leases GROUP BY resource")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	inUse := make(map[string]int, len(names))
	for rows.Next() {
		var name string
		var count int
		if err := rows.Scan(&name, &count); err != nil {
			return nil, err
		}
		inUse[name] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	usage := make([]resourceUsage, 0, len(names))
	for _, name := range names {
		usage = append(usage, resourceUsage{Name: name, InUse: inUse[name], Limit: controller.config.Resources[name]})
	}
	return usage, nil
}

func (controller *Controller) diskFree() (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(controller.stateDir, &stat); err != nil {
		return 0, err
	}
	return uint64(stat.Bavail) * uint64(stat.Bsize), nil
}

func parseJobTime(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339, value)
	return parsed
}

func elapsed(start, end, now time.Time) string {
	if start.IsZero() {
		return "—"
	}
	if end.IsZero() {
		end = now
	}
	if end.Before(start) {
		return "—"
	}
	return duration(end.Sub(start))
}

func duration(value time.Duration) string {
	if value < time.Second {
		return "<1s"
	}
	return value.Round(time.Second).String()
}

func timeout(seconds int) string {
	if seconds == 0 {
		return "none"
	}
	return duration(time.Duration(seconds) * time.Second)
}

func byteSize(value uint64) string {
	const unit = 1024 * 1024 * 1024
	if value >= unit {
		return fmt.Sprintf("%.1f GiB", float64(value)/unit)
	}
	return fmt.Sprintf("%.1f MiB", float64(value)/(1024*1024))
}

var dashboardTemplate = template.Must(template.New("dashboard").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta http-equiv="refresh" content="3"><title>BitCI</title><style>
:root{color-scheme:dark;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;background:#101418;color:#e8edf2}*{box-sizing:border-box}body{margin:0;padding:32px;max-width:1440px}header{display:flex;justify-content:space-between;align-items:baseline;border-bottom:1px solid #34404a;padding-bottom:20px}h1{font:600 24px system-ui;margin:0}h2{font:600 15px system-ui;margin:0 0 12px}.muted{color:#9aa6b2;font-size:13px}.summary{display:flex;gap:24px;padding:20px 0;border-bottom:1px solid #34404a}.count{font:600 18px system-ui}.state{font-size:12px;color:#9aa6b2;text-transform:uppercase}.passed{color:#61d095}.failed{color:#ff8075}.running{color:#7ea8ff}.queued{color:#e4b965}section{margin-top:28px}table{width:100%;border-collapse:collapse;font-size:13px}th{text-align:left;color:#9aa6b2;font-weight:500;border-bottom:1px solid #34404a;padding:8px}td{border-bottom:1px solid #263039;padding:10px 8px;vertical-align:top}.num{font-variant-numeric:tabular-nums;white-space:nowrap}.sha{max-width:170px;overflow:hidden;text-overflow:ellipsis;display:inline-block;vertical-align:bottom}a{color:#9fc1ff}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#151c22;border:1px solid #34404a;padding:16px;line-height:1.45}@media(max-width:760px){body{padding:18px}header{display:block}.summary{gap:14px;overflow:auto}table{min-width:980px;display:block;overflow:auto}}
</style></head><body><header><h1>BitCI</h1><span class="muted">local dashboard · refreshes every 3s · {{.Now.Format "15:04:05 UTC"}}</span></header>
<div class="summary">{{range .Counts}}<div><div class="count {{.State}} num">{{.Count}}</div><div class="state">{{.State}}</div></div>{{end}}<div><div class="count num">{{.AveragePassed}}</div><div class="state">avg passed · 7d ({{.PassedLastWeek}})</div></div><div><div class="count num">{{.DiskFree}}</div><div class="state">disk free · guard {{.DiskGuard}}</div></div>{{range .Resources}}<div><div class="count num">{{.InUse}}/{{.Limit}}</div><div class="state">resource · {{.Name}}</div></div>{{end}}</div>
<section><h2>Recent jobs</h2><table><thead><tr><th>Job</th><th>State</th><th>Stage</th><th>Submitted ref</th><th>Tested SHA</th><th>Queue wait</th><th>Stage duration</th><th>Timeout</th><th>Resources</th><th>Log</th></tr></thead><tbody>{{range .Jobs}}<tr><td class="num">{{.ID}}</td><td><span class="{{.State}}">{{.State}}</span></td><td>{{.Task}}</td><td><span class="sha" title="{{.SubmittedRef}}">{{if .SubmittedRef}}{{.SubmittedRef}}{{else}}—{{end}}</span></td><td><span class="sha" title="{{.TestedSHA}}">{{if .TestedSHA}}{{.TestedSHA}}{{else}}—{{end}}</span></td><td class="num">{{.QueueWait}}</td><td class="num">{{.StageDuration}}</td><td class="num">{{.Timeout}}</td><td>{{.Resources}}</td><td>{{if .LogPath}}<a href="/jobs/{{.ID}}/logs">last 80 lines</a>{{else}}—{{end}}</td></tr>{{else}}<tr><td colspan="10" class="muted">No jobs yet. Submit configured task IDs from the CLI or MCP.</td></tr>{{end}}</tbody></table></section></body></html>`))
