package gorpc

// BackpressureOptions controls optional limits that reject new work before
// memory grows without bound. Zero values keep existing unlimited behavior.
type BackpressureOptions struct {
	MaxPendingCalls     int
	MaxActiveStreams    int
	MaxConcurrentWrites int
	OnBackpressure      func(BackpressureInfo)
}

// BackpressureInfo describes a rejected operation.
type BackpressureInfo struct {
	Side      string
	Reason    string
	Limit     int
	RequestID uint64
	Function  string
	FrameType FrameType
}

const (
	// BackpressureSideClient means the dialing side rejected new local work.
	BackpressureSideClient = "client"
	// BackpressureSideServer means the accepting side rejected new local work.
	BackpressureSideServer = "server"

	// BackpressureReasonPendingCalls means the pending request limit was reached.
	BackpressureReasonPendingCalls = "pending_calls"
	// BackpressureReasonActiveStreams means the active stream limit was reached.
	BackpressureReasonActiveStreams = "active_streams"
	// BackpressureReasonConcurrentWrites means the concurrent write limit was reached.
	BackpressureReasonConcurrentWrites = "concurrent_writes"
)

type writeLimiter struct {
	slots chan struct{}
}

func newWriteLimiter(limit int) *writeLimiter {
	if limit <= 0 {
		return nil
	}

	return &writeLimiter{slots: make(chan struct{}, limit)}
}

func (l *writeLimiter) acquire() bool {
	if l == nil {
		return true
	}

	select {
	case l.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *writeLimiter) release() {
	if l == nil {
		return
	}

	select {
	case <-l.slots:
	default:
	}
}

func normalizeBackpressureOptions(opts BackpressureOptions) BackpressureOptions {
	if opts.MaxPendingCalls < 0 {
		opts.MaxPendingCalls = 0
	}
	if opts.MaxActiveStreams < 0 {
		opts.MaxActiveStreams = 0
	}
	if opts.MaxConcurrentWrites < 0 {
		opts.MaxConcurrentWrites = 0
	}

	return opts
}

func reportBackpressure(opts BackpressureOptions, info BackpressureInfo) {
	if opts.OnBackpressure != nil {
		defer func() {
			_ = recover()
		}()
		opts.OnBackpressure(info)
	}
}
