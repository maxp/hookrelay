// Package valkey is the concrete Valkey adapter and the only module that
// knows hr1: keys, Valkey commands, client behavior, and embedded Lua
// scripts. It satisfies the small storage interfaces declared by the feature
// modules; it never exposes generic Get/Set repositories.
package valkey

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"
)

// Script is one versioned Lua contract: its embedded body, the body's local
// SHA-256 identity digest, the SHA-1 digest Valkey must report on load, and
// the bounded first-position status codes with the number of positional
// fields each carries.
type Script struct {
	Name    string
	Version int // contract version; the filename suffix must match
	Body    string
	BodySHA string // lowercase hex SHA-256 of the exact embedded body
	LoadSHA string // lowercase hex SHA-1 of the body: the EVALSHA identity
	Arity   map[string]int
}

// registry holds every versioned script by name.
var registry map[string]*Script

// register embeds a script body and computes its digests at init. The
// version must match the "_v<N>" filename suffix, making changed contracts
// explicit. arity maps every accepted status to its positional field count.
func register(name string, version int, arity map[string]int) {
	if !strings.HasSuffix(name, "_v"+strconv.Itoa(version)) {
		panic(fmt.Sprintf("valkey: script %s registry version %d does not match the filename suffix", name, version))
	}
	body := mustScript(name)
	sum := sha256.Sum256([]byte(body))
	loadSum := sha1.Sum([]byte(body))
	registry[name] = &Script{
		Name:    name,
		Version: version,
		Body:    body,
		BodySHA: hex.EncodeToString(sum[:]),
		LoadSHA: hex.EncodeToString(loadSum[:]),
		Arity:   arity,
	}
}

func mustScript(name string) string {
	body, err := fs.ReadFile(scriptsFS, "scripts/"+name+".lua")
	if err != nil {
		panic(fmt.Sprintf("valkey: embedded script %s: %v", name, err))
	}
	return string(body)
}

// Adapter owns the client connection and the loaded script registry.
type Adapter struct {
	client    valkey.Client
	mu        sync.RWMutex
	shaByName map[string]string // script name → server-side SHA (from SCRIPT LOAD)
	metrics   *adapterMetrics   // nil until Instrument
}

// ParseURL re-exports the client URL parser so composition modules do not
// need the client package directly.
func ParseURL(raw string) (valkey.ClientOption, error) { return valkey.ParseURL(raw) }

// NewAdapter dials Valkey (eagerly: construction failure is the first
// dependency check) and prepares the empty script registry. Scripts load via
// LoadScripts before readiness.
func NewAdapter(opt valkey.ClientOption) (*Adapter, error) {
	client, err := valkey.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("valkey: connect: %w", err)
	}
	return &Adapter{client: client, shaByName: map[string]string{}}, nil
}

// Close releases the client connection.
func (a *Adapter) Close() { a.client.Close() }

// FlushAll clears every database — test helper for dedicated instances.
func (a *Adapter) FlushAll(ctx context.Context) error {
	_, err := a.client.Do(ctx, a.client.B().Flushall().Build()).ToMessage()
	return err
}

// FlushDB clears the selected database — test helper safe on shared
// instances where each test package owns its own database index.
func (a *Adapter) FlushDB(ctx context.Context) error {
	_, err := a.client.Do(ctx, a.client.B().Flushdb().Build()).ToMessage()
	return err
}

// Ping verifies connectivity.
func (a *Adapter) Ping(ctx context.Context) error {
	resp, err := a.client.Do(ctx, a.client.B().Ping().Build()).ToMessage()
	if err != nil {
		return fmt.Errorf("valkey: ping: %w", err)
	}
	if s, _ := resp.ToString(); s != "PONG" {
		return fmt.Errorf("valkey: ping returned %q", s)
	}
	return nil
}

// LoadScripts runs SCRIPT LOAD for every registered script and verifies that
// the digest Valkey reports equals the locally computed SHA-1 of the embedded
// body. A load failure or digest mismatch fails readiness.
func (a *Adapter) LoadScripts(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for name, script := range registry {
		resp, err := a.client.Do(ctx, a.client.B().Arbitrary("SCRIPT", "LOAD", script.Body).Build()).ToMessage()
		if err != nil {
			return fmt.Errorf("valkey: script load %s: %w", name, err)
		}
		sha, err := resp.ToString()
		if err != nil {
			return fmt.Errorf("valkey: script load %s: digest is not a string: %w", name, err)
		}
		if !strings.EqualFold(sha, script.LoadSHA) {
			return fmt.Errorf("valkey: script load %s: server digest %q does not match embedded body digest %q", name, sha, script.LoadSHA)
		}
		a.shaByName[name] = script.LoadSHA
	}
	return nil
}

// ErrNotDispatched marks a RunScript failure that happened before any
// command was sent: the script certainly did not run. Every other RunScript
// error is an uncertain outcome — the script may have run, fully or
// partially — and callers must not report it as a definite failure.
var ErrNotDispatched = errors.New("valkey: script not dispatched")

// RunScript executes a registered script by EVALSHA with one EVAL reload of
// the same embedded body on NOSCRIPT. Ambiguous transport errors are returned
// as-is and never retried (server errors are *valkey.ValkeyError; transport
// failures are ordinary errors — see ADR 0006).
func (a *Adapter) RunScript(ctx context.Context, name string, keys, args []string) (res *Result, err error) {
	start := time.Now()
	defer func() { a.metrics.observeScript(name, start, err) }()
	script, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("%w: unknown script %q", ErrNotDispatched, name)
	}
	a.mu.RLock()
	sha := a.shaByName[name]
	a.mu.RUnlock()
	if sha == "" {
		return nil, fmt.Errorf("%w: script %s not loaded", ErrNotDispatched, name)
	}

	run := func(evalSHA bool) (*Result, error) {
		argv := make([]string, 0, 3+len(keys)+len(args))
		if evalSHA {
			argv = append(argv, "EVALSHA", sha)
		} else {
			argv = append(argv, "EVAL", script.Body)
		}
		argv = append(argv, fmt.Sprint(len(keys)))
		argv = append(argv, keys...)
		argv = append(argv, args...)
		cmd := a.client.B().Arbitrary(argv...).Build()
		msg, err := a.client.Do(ctx, cmd).ToMessage()
		if err != nil {
			return nil, err
		}
		return parseResult(name, script.Arity, msg)
	}

	res, err = run(true)
	if err == nil {
		return res, nil
	}
	// NOSCRIPT is a precise server error: reload the exact embedded body once.
	if isNoScript(err) {
		if res, evalErr := run(false); evalErr == nil {
			return res, nil
		} else {
			return nil, fmt.Errorf("valkey: script %s EVAL after NOSCRIPT: %w", name, evalErr)
		}
	}
	return nil, err
}

func isNoScript(err error) bool {
	// Server errors surface as *valkey.ValkeyError with a NOSCRIPT prefix;
	// transport errors never carry the prefix (ADR 0006).
	return err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT")
}
