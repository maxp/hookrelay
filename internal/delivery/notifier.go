package delivery

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Ready-signal sources: the transitions that may expose claimable work
// (bounded values of the source label).
const (
	SignalAccept          = "accept"
	SignalAck             = "ack"
	SignalDeadLetter      = "dead_letter"
	SignalRetryActivation = "retry_activation"
	SignalReplay          = "replay"
	SignalBlockClear      = "block_clear"
)

// ReadySignaler receives hints that claimable work may exist. Signals are
// hints only: the ready index stays the source of truth.
type ReadySignaler interface {
	Signal(source string)
}

// ReadyNotifier wakes waiting claims early (ADR 0008). Each signal wakes
// at most the oldest registered waiter, which then rechecks the ready index;
// a signal with nobody waiting is dropped, because every new claim checks
// first. Periodic rechecks recover lost signals. A nil or disabled notifier
// never wakes anyone.
type ReadyNotifier struct {
	mu      sync.Mutex
	enabled bool
	waiters *list.List // of *readyWaiter, oldest first
	signals *prometheus.CounterVec
}

// readyWaiter is one registration of a waiting claim. Its channel carries
// at most one wake and is never reused across registrations.
type readyWaiter struct {
	c    chan struct{}
	elem *list.Element // nil once woken or deregistered
}

// NewReadyNotifier registers hookrelay_ready_signals_total on reg; enabled
// false makes Signal a no-op.
func NewReadyNotifier(enabled bool, reg prometheus.Registerer) (*ReadyNotifier, error) {
	n := &ReadyNotifier{
		enabled: enabled,
		waiters: list.New(),
		signals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_ready_signals_total",
			Help: "Ready-work hints by source and result (delivered woke a waiting claim, no_waiter was dropped).",
		}, []string{"source", "result"}),
	}
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	if err := reg.Register(n.signals); err != nil {
		return nil, fmt.Errorf("delivery: register notifier metrics: %w", err)
	}
	return n, nil
}

// Signal wakes the oldest waiting claim, if any.
func (n *ReadyNotifier) Signal(source string) {
	if n == nil || !n.enabled {
		return
	}
	n.mu.Lock()
	woke := n.wakeOldestLocked()
	n.mu.Unlock()
	if woke {
		n.signals.WithLabelValues(source, "delivered").Inc()
	} else {
		n.signals.WithLabelValues(source, "no_waiter").Inc()
	}
}

func (n *ReadyNotifier) wakeOldestLocked() bool {
	front := n.waiters.Front()
	if front == nil {
		return false
	}
	w := n.waiters.Remove(front).(*readyWaiter)
	w.elem = nil
	w.c <- struct{}{} // buffered 1; a registration is woken at most once
	return true
}

// register enqueues a new waiter; nil when notifications are off.
func (n *ReadyNotifier) register() *readyWaiter {
	if n == nil || !n.enabled {
		return nil
	}
	w := &readyWaiter{c: make(chan struct{}, 1)}
	n.mu.Lock()
	w.elem = n.waiters.PushBack(w)
	n.mu.Unlock()
	return w
}

// deregister ends a registration. A wake the waiter received but will not
// consume is handed on to the next oldest waiter, so leaving never loses it.
func (n *ReadyNotifier) deregister(w *readyWaiter) {
	if w == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if w.elem != nil {
		n.waiters.Remove(w.elem)
		w.elem = nil
		return
	}
	select {
	case <-w.c:
		n.wakeOldestLocked()
	default:
	}
}

// wake is the waiter's channel; a nil waiter never fires.
func (w *readyWaiter) wake() <-chan struct{} {
	if w == nil {
		return nil
	}
	return w.c
}
