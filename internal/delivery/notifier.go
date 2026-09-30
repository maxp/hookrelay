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
	// signalHandoff counts a wake passed on by a claim that left without
	// consuming it; signalUnknown replaces any unlisted source so the
	// label stays bounded.
	signalHandoff = "handoff"
	signalUnknown = "unknown"
)

var knownSignals = map[string]bool{
	SignalAccept: true, SignalAck: true, SignalDeadLetter: true,
	SignalRetryActivation: true, SignalReplay: true, SignalBlockClear: true,
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
			Help: "Ready-work hints by source and result (delivered woke a waiting claim, no_waiter was dropped); source handoff is a wake passed on by a claim that left without using it.",
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
	if !knownSignals[source] {
		source = signalUnknown
	}
	n.mu.Lock()
	woke := n.wakeOldestLocked()
	n.mu.Unlock()
	n.count(source, woke)
}

func (n *ReadyNotifier) count(source string, woke bool) {
	result := "no_waiter"
	if woke {
		result = "delivered"
	}
	n.signals.WithLabelValues(source, result).Inc()
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
	if w.elem != nil {
		n.waiters.Remove(w.elem)
		w.elem = nil
		n.mu.Unlock()
		return
	}
	handedOn, woke := false, false
	select {
	case <-w.c:
		handedOn, woke = true, n.wakeOldestLocked()
	default:
	}
	n.mu.Unlock()
	if handedOn {
		n.count(signalHandoff, woke)
	}
}

// wake is the waiter's channel; a nil waiter never fires.
func (w *readyWaiter) wake() <-chan struct{} {
	if w == nil {
		return nil
	}
	return w.c
}
