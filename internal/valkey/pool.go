package valkey

import "github.com/valkey-io/valkey-go"

// maxPipelineExponent caps the pipelined connection ring at 2^2 = 4
// connections, the multiplexing verified in the client spike (ADR 0006).
const maxPipelineExponent = 2

// ApplyConnectionLimits bounds the client to maxConns connections in total
// (HOOKRELAY_VALKEY_MAX_CONNECTIONS, at least 2). Every hookrelay command,
// including long-poll rechecks, runs on the pipelined ring of 2^N
// connections; the blocking pool — used only by blocking commands, which
// Milestone 1 does not issue — receives the remainder, with minIdle
// (HOOKRELAY_VALKEY_MIN_IDLE) as its idle floor.
func ApplyConnectionLimits(opt *valkey.ClientOption, maxConns, minIdle int) {
	exp := 0
	for exp < maxPipelineExponent && 1<<(exp+1) <= maxConns-1 {
		exp++
	}
	opt.PipelineMultiplex = exp
	blocking := max(maxConns-1<<exp, 1)
	opt.BlockingPoolSize = blocking
	opt.BlockingPoolMinSize = min(max(minIdle, 0), blocking)
}
