package valkey

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// info reads one INFO section as a field map.
func (a *Adapter) info(ctx context.Context, section string) (map[string]string, error) {
	raw, err := a.client.Do(ctx, a.client.B().Info().Section(section).Build()).ToString()
	if err != nil {
		return nil, fmt.Errorf("valkey: info %s: %w", section, err)
	}
	out := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			out[k] = v
		}
	}
	return out, nil
}

type memoryInfo struct {
	used, max int64
}

// memory reads used_memory and maxmemory (0 when Valkey has no limit).
func (a *Adapter) memory(ctx context.Context) (memoryInfo, error) {
	f, err := a.info(ctx, "memory")
	if err != nil {
		return memoryInfo{}, err
	}
	used, err1 := strconv.ParseInt(f["used_memory"], 10, 64)
	max, err2 := strconv.ParseInt(f["maxmemory"], 10, 64)
	if err1 != nil || err2 != nil {
		return memoryInfo{}, fmt.Errorf("valkey: info memory: used_memory/maxmemory missing or malformed")
	}
	return memoryInfo{used: used, max: max}, nil
}

// SampleServer reads Valkey memory (INFO memory) and AOF state (INFO
// persistence) into the Valkey metrics. Maintenance calls it once per
// round.
func (a *Adapter) SampleServer(ctx context.Context) error {
	mem, err := a.memory(ctx)
	if err != nil {
		return err
	}
	p, err := a.info(ctx, "persistence")
	if err != nil {
		return err
	}
	aof, err := strconv.ParseInt(p["aof_enabled"], 10, 64)
	if err != nil {
		return fmt.Errorf("valkey: info persistence: aof_enabled missing or malformed")
	}
	// aof_delayed_fsync is reported only while AOF is enabled.
	var delayed int64
	if v, ok := p["aof_delayed_fsync"]; ok {
		if delayed, err = strconv.ParseInt(v, 10, 64); err != nil {
			return fmt.Errorf("valkey: info persistence: aof_delayed_fsync malformed")
		}
	}
	a.metrics.setServer(mem, aof == 1, delayed)
	return nil
}
