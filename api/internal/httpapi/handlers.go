package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/hlog"

	"github.com/gscho/vercel-go-example/api/internal/checks"
	"github.com/gscho/vercel-go-example/api/internal/queue"
	"github.com/gscho/vercel-go-example/api/internal/store"
)

const (
	defaultIntervalSeconds = 60
	minIntervalSeconds     = 30
	maxIntervalSeconds     = 3600
	recentChecksPerMonitor = 30
	historyWindow          = 24 * time.Hour
	// A worker counts as online if its last heartbeat (sent every 15s) is this recent.
	workerOnlineWindow = 45 * time.Second
)

type Handlers struct {
	store *store.Store
	queue *queue.Client
}

func NewHandlers(s *store.Store, q *queue.Client) *Handlers {
	return &Handlers{store: s, queue: q}
}

// monitorView is a monitor plus the check history the UI needs to draw it.
type monitorView struct {
	store.Monitor
	Status       string              `json:"status"`
	Uptime24h    *float64            `json:"uptime24h"`
	RecentChecks []store.CheckResult `json:"recentChecks"`
}

func newMonitorView(monitor store.Monitor, recent []store.CheckResult, uptime *float64) monitorView {
	if recent == nil {
		recent = []store.CheckResult{}
	}
	status := "pending"
	switch {
	case monitor.Paused:
		status = "paused"
	case len(recent) > 0 && recent[0].OK:
		status = "up"
	case len(recent) > 0:
		status = "down"
	}
	return monitorView{Monitor: monitor, Status: status, Uptime24h: uptime, RecentChecks: recent}
}

func (h *Handlers) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (h *Handlers) workerStatus(w http.ResponseWriter, r *http.Request) {
	workers, err := h.store.ActiveWorkers(r.Context(), time.Now().Add(-workerOnlineWindow))
	if err != nil {
		writeServerError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"online": len(workers) > 0, "instances": workers})
}

func (h *Handlers) listMonitors(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	since := time.Now().Add(-historyWindow)

	monitors, err := h.store.ListMonitors(ctx)
	if err != nil {
		writeServerError(w, r, err)
		return
	}
	recent, err := h.store.LatestChecks(ctx, since, recentChecksPerMonitor)
	if err != nil {
		writeServerError(w, r, err)
		return
	}
	uptime, err := h.store.UptimeSince(ctx, since)
	if err != nil {
		writeServerError(w, r, err)
		return
	}

	views := make([]monitorView, 0, len(monitors))
	for _, monitor := range monitors {
		var monitorUptime *float64
		if value, ok := uptime[monitor.ID]; ok {
			monitorUptime = &value
		}
		views = append(views, newMonitorView(monitor, recent[monitor.ID], monitorUptime))
	}
	writeJSON(w, http.StatusOK, map[string]any{"monitors": views})
}

// getMonitor returns the monitor with every check from the last 24 hours.
func (h *Handlers) getMonitor(w http.ResponseWriter, r *http.Request) {
	monitor, ok := h.loadMonitor(w, r)
	if !ok {
		return
	}
	history, err := h.store.ChecksSince(r.Context(), monitor.ID, time.Now().Add(-historyWindow))
	if err != nil {
		writeServerError(w, r, err)
		return
	}

	var uptime *float64
	if len(history) > 0 {
		succeeded := 0
		for _, check := range history {
			if check.OK {
				succeeded++
			}
		}
		value := float64(succeeded) / float64(len(history))
		uptime = &value
	}
	writeJSON(w, http.StatusOK, newMonitorView(monitor, history, uptime))
}

type monitorRequest struct {
	Name            *string `json:"name"`
	URL             *string `json:"url"`
	IntervalSeconds *int    `json:"intervalSeconds"`
	Paused          *bool   `json:"paused"`
}

func (h *Handlers) createMonitor(w http.ResponseWriter, r *http.Request) {
	var req monitorRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Name == nil || req.URL == nil {
		writeError(w, http.StatusBadRequest, "name and url are required")
		return
	}
	if req.IntervalSeconds == nil {
		interval := defaultIntervalSeconds
		req.IntervalSeconds = &interval
	}
	if err := validateMonitorRequest(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	monitor := store.Monitor{Name: *req.Name, URL: *req.URL, IntervalSeconds: *req.IntervalSeconds}
	if req.Paused != nil {
		monitor.Paused = *req.Paused
	}
	if err := h.store.CreateMonitor(r.Context(), &monitor); err != nil {
		writeServerError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, newMonitorView(monitor, nil, nil))
}

func (h *Handlers) updateMonitor(w http.ResponseWriter, r *http.Request) {
	id, ok := monitorID(w, r)
	if !ok {
		return
	}
	var req monitorRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := validateMonitorRequest(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	monitor, err := h.store.UpdateMonitor(r.Context(), id, store.MonitorUpdate{
		Name:            req.Name,
		URL:             req.URL,
		IntervalSeconds: req.IntervalSeconds,
		Paused:          req.Paused,
	})
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "monitor not found")
		return
	}
	if err != nil {
		writeServerError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, monitor)
}

func (h *Handlers) deleteMonitor(w http.ResponseWriter, r *http.Request) {
	id, ok := monitorID(w, r)
	if !ok {
		return
	}
	err := h.store.DeleteMonitor(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "monitor not found")
		return
	}
	if err != nil {
		writeServerError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// checkNow queues a one-off check. The worker picks it up from the same topic as
// scheduled checks.
func (h *Handlers) checkNow(w http.ResponseWriter, r *http.Request) {
	monitor, ok := h.loadMonitor(w, r)
	if !ok {
		return
	}
	job := checks.Job{MonitorID: monitor.ID, Trigger: checks.TriggerManual}
	if err := checks.Enqueue(r.Context(), h.queue, job); err != nil {
		hlog.FromRequest(r).Error().Err(err).Msg("enqueue manual check")
		writeError(w, http.StatusBadGateway, "could not queue the check")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"queued": true})
}

func (h *Handlers) loadMonitor(w http.ResponseWriter, r *http.Request) (store.Monitor, bool) {
	id, ok := monitorID(w, r)
	if !ok {
		return store.Monitor{}, false
	}
	monitor, err := h.store.GetMonitor(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "monitor not found")
		return store.Monitor{}, false
	}
	if err != nil {
		writeServerError(w, r, err)
		return store.Monitor{}, false
	}
	return monitor, true
}

func monitorID(w http.ResponseWriter, r *http.Request) (uint, bool) {
	id, err := strconv.ParseUint(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid monitor id")
		return 0, false
	}
	return uint(id), true
}

func validateMonitorRequest(req *monitorRequest) error {
	if req.Name != nil {
		name := strings.TrimSpace(*req.Name)
		if name == "" || len(name) > 100 {
			return errors.New("name must be between 1 and 100 characters")
		}
		req.Name = &name
	}
	if req.URL != nil {
		parsed, err := url.Parse(strings.TrimSpace(*req.URL))
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return errors.New("url must be an absolute http or https URL")
		}
		normalized := parsed.String()
		req.URL = &normalized
	}
	if req.IntervalSeconds != nil {
		interval := *req.IntervalSeconds
		if interval < minIntervalSeconds || interval > maxIntervalSeconds {
			return errors.New("intervalSeconds must be between 30 and 3600")
		}
	}
	return nil
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	if err := json.NewDecoder(r.Body).Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeServerError(w http.ResponseWriter, r *http.Request, err error) {
	hlog.FromRequest(r).Error().Err(err).Msg("request failed")
	writeError(w, http.StatusInternalServerError, "internal error")
}
