package cli

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"
)

// TestWatchSecondSignalForces pins that a genuinely second signal (after the
// duplicate of the first is drained) selects the forced path.
func TestWatchSecondSignalForces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	forceCh := make(chan os.Signal, 4)
	appDone := make(chan struct{})

	go func() {
		cancel()
		forceCh <- syscall.SIGTERM // duplicate of the first signal
		forceCh <- syscall.SIGTERM // the forced second signal
	}()
	if !watchSecondSignal(ctx, forceCh, appDone) {
		t.Fatal("second signal did not select the forced path")
	}
}

// TestWatchSecondSignalCompletes pins the clean path: without a second signal
// the watchdog completes through appDone and reports not-forced.
func TestWatchSecondSignalCompletes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	forceCh := make(chan os.Signal, 4)
	appDone := make(chan struct{})

	go func() {
		cancel()
		forceCh <- syscall.SIGINT // duplicate of the first signal
		close(appDone)
	}()
	if watchSecondSignal(ctx, forceCh, appDone) {
		t.Fatal("clean completion was reported as forced")
	}
}

// TestWatchSecondSignalNoSignal pins completion when forceCh stays empty.
func TestWatchSecondSignalNoSignal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	appDone := make(chan struct{})
	go func() {
		cancel()
		close(appDone)
	}()
	if watchSecondSignal(ctx, forceChan(), appDone) {
		t.Fatal("empty force channel was reported as forced")
	}
}

func forceChan() <-chan os.Signal {
	ch := make(chan os.Signal, 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(ch)
	}()
	return ch
}
