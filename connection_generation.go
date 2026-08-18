package gorpc

import "sync/atomic"

var processConnectionGeneration atomic.Uint64

// nextConnectionGeneration allocates an opaque process-local identity for one
// physical connection. Zero is reserved for "no active connection".
func nextConnectionGeneration() uint64 {
	for {
		generation := processConnectionGeneration.Add(1)
		if generation != 0 {
			return generation
		}
	}
}
