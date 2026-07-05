package gorpc

import (
	"context"
	"net"
	"time"
)

// Context is the request-scoped context passed to server handlers.
type Context struct {
	context.Context

	clientName string
	requestID  uint64
	function   string
	remoteAddr net.Addr
	localAddr  net.Addr
	conn       *Conn
}

var _ context.Context = (*Context)(nil)

// ClientName returns the self-reported client name from the connection
// handshake. It is useful for logs and metrics, but is not authenticated.
func (c *Context) ClientName() string {
	if c == nil {
		return ""
	}

	return c.clientName
}

// RequestID returns the request ID from the GoRPC frame.
func (c *Context) RequestID() uint64 {
	if c == nil {
		return 0
	}

	return c.requestID
}

// Function returns the remote function name for the request.
func (c *Context) Function() string {
	if c == nil {
		return ""
	}

	return c.function
}

// RemoteAddr returns the peer address for the connection.
func (c *Context) RemoteAddr() net.Addr {
	if c == nil {
		return nil
	}

	return c.remoteAddr
}

// LocalAddr returns the local address for the connection.
func (c *Context) LocalAddr() net.Addr {
	if c == nil {
		return nil
	}

	return c.localAddr
}

// Conn returns the accepted connection that delivered this request when the
// handler is running on a Server. Client-side handlers return nil here because
// they can already call back through their Client.
func (c *Context) Conn() *Conn {
	if c == nil {
		return nil
	}

	return c.conn
}

// Call performs a unary request/response call back over the same accepted
// connection that delivered this request.
func (c *Context) Call(function string, req any, resp any) error {
	if c == nil || c.conn == nil {
		return ErrUnavailable
	}

	return c.conn.Call(function, req, resp)
}

// CallWithTimeout performs a unary request/response call back over the same
// accepted connection with a timeout.
func (c *Context) CallWithTimeout(function string, req any, resp any, timeout time.Duration) error {
	if c == nil || c.conn == nil {
		return ErrUnavailable
	}

	return c.conn.CallWithTimeout(function, req, resp, timeout)
}

// CallContext performs a unary request/response call back over the same accepted
// connection.
func (c *Context) CallContext(ctx context.Context, function string, req any, resp any) error {
	if c == nil || c.conn == nil {
		return ErrUnavailable
	}

	return c.conn.CallContext(ctx, function, req, resp)
}
