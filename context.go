package gorpc

import (
	"context"
	"net"
)

// Context is the request-scoped context passed to server handlers.
type Context struct {
	context.Context

	clientName string
	requestID  uint64
	function   string
	remoteAddr net.Addr
	localAddr  net.Addr
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
