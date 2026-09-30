package app

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"github.com/maxp/hookrelay/internal/gen"
)

// workBarrier drains admitted API requests and background rounds before a
// startup/recovery scan. It coordinates this process only, not other writers.
// The zero value admits administrative work; public/maintenance admission also
// requires readiness. A closed idle channel wakes a drain without polling.
type workBarrier struct {
	mu     sync.Mutex
	paused bool
	active int
	idle   chan struct{}
}

func (r *Readiness) beginWork(requireReady bool) (func(), bool) {
	b := &r.work
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.paused || (requireReady && !r.Ready()) {
		return nil, false
	}
	if b.active == 0 {
		b.idle = make(chan struct{})
	}
	b.active++
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.active--
		if b.active == 0 {
			close(b.idle)
		}
	}, true
}

func (r *Readiness) pauseWork(ctx context.Context) error {
	b := &r.work
	b.mu.Lock()
	b.paused = true
	active, idle := b.active, b.idle
	b.mu.Unlock()
	if active == 0 {
		return ctx.Err()
	}
	select {
	case <-idle:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Readiness) resumeWork() {
	r.work.mu.Lock()
	defer r.work.mu.Unlock()
	r.work.paused = false
}

func (r *Readiness) workPaused() bool {
	r.work.mu.Lock()
	defer r.work.mu.Unlock()
	return r.work.paused
}

// RunMaintenanceRound admits one bounded background round while ready. Due
// transitions executed by reconciliation itself do not enter this barrier.
func (r *Readiness) RunMaintenanceRound(ctx context.Context, round func(context.Context)) {
	finish, ok := r.beginWork(true)
	if !ok {
		return
	}
	defer finish()
	round(ctx)
}

func (a *App) guardStorageRequests(next http.Handler, public bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		storage := strings.HasPrefix(r.URL.Path, "/admin/v1/")
		if public {
			storage = strings.HasPrefix(r.URL.Path, "/webhook/") || strings.HasPrefix(r.URL.Path, "/v1/")
		}
		if !storage {
			next.ServeHTTP(w, r)
			return
		}
		finish, ok := a.deps.Readiness.beginWork(public)
		if ok {
			defer finish()
			next.ServeHTTP(w, r)
			return
		}
		requestID := gen.Crypto{}.UUIDv7()
		w.Header().Set("X-Request-Id", requestID)
		w.Header().Set("Retry-After", "1")
		w.Header().Set("Cache-Control", "no-store")
		if strings.HasPrefix(r.URL.Path, "/webhook/") {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{
			"code": "dependency_unavailable", "message": "service recovering; retry after readiness returns", "request_id": requestID,
		}})
	})
}
