package gorpc

import (
	"context"
	"time"
)

// BroadcastResult reports the outcome of Server.NotifyAll.
type BroadcastResult struct {
	Total  int
	Sent   int
	Failed int
	Errors map[*Conn]error
}

// OK reports whether every connection accepted the broadcast frame.
func (r BroadcastResult) OK() bool {
	return r.Failed == 0
}

// NotifyAll sends a one-way notification to every currently accepted
// connection. It snapshots the connection list before sending.
func (s *Server) NotifyAll(function string, req any) BroadcastResult {
	return s.NotifyAllContext(context.Background(), function, req)
}

// NotifyAllWithTimeout sends a notification to every currently accepted
// connection with a timeout for the broadcast write loop.
func (s *Server) NotifyAllWithTimeout(function string, req any, timeout time.Duration) BroadcastResult {
	if timeout <= 0 {
		return s.NotifyAll(function, req)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return s.NotifyAllContext(ctx, function, req)
}

// NotifyAllContext sends a notification to every currently accepted connection.
// The context controls each write attempt; it does not wait for remote handler
// completion because notifications do not have responses.
func (s *Server) NotifyAllContext(ctx context.Context, function string, req any) BroadcastResult {
	result := BroadcastResult{}
	if s == nil {
		result.Errors = map[*Conn]error{nil: ErrClosed}
		result.Failed = 1
		return result
	}
	ctx = normalizeContext(ctx)

	conns := s.Connections()
	result.Total = len(conns)
	for _, conn := range conns {
		if err := conn.NotifyContext(ctx, function, req); err != nil {
			if result.Errors == nil {
				result.Errors = make(map[*Conn]error)
			}
			result.Errors[conn] = err
			result.Failed++
			continue
		}
		result.Sent++
	}

	return result
}
