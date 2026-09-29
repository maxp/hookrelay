package valkey

import (
	"testing"

	"github.com/valkey-io/valkey-go"
)

// TestApplyConnectionLimits pins that the pipelined ring and the blocking
// pool together never exceed the configured maximum.
func TestApplyConnectionLimits(t *testing.T) {
	for _, tc := range []struct {
		max, minIdle        int
		multiplex, blocking int
		blockingMin         int
	}{
		{2, 2, 0, 1, 1},
		{3, 0, 1, 1, 0},
		{5, 2, 2, 1, 1},
		{20, 2, 2, 16, 2},
		{100, 50, 2, 96, 50},
	} {
		var opt valkey.ClientOption
		ApplyConnectionLimits(&opt, tc.max, tc.minIdle)
		if opt.PipelineMultiplex != tc.multiplex || opt.BlockingPoolSize != tc.blocking || opt.BlockingPoolMinSize != tc.blockingMin {
			t.Errorf("max %d: multiplex %d blocking %d min %d; want %d %d %d", tc.max,
				opt.PipelineMultiplex, opt.BlockingPoolSize, opt.BlockingPoolMinSize, tc.multiplex, tc.blocking, tc.blockingMin)
		}
		if total := 1<<opt.PipelineMultiplex + opt.BlockingPoolSize; total > tc.max {
			t.Errorf("max %d: %d connections", tc.max, total)
		}
	}
}
