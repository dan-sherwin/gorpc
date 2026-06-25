package gorpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ClientOptions configures Dial.
type ClientOptions struct {
	ClientName          string
	ExpectedServiceName string
	Codec               Codec
	MaxFrameSize        int64
	HandshakeTimeout    time.Duration
	Logger              *slog.Logger
	Dialer              *net.Dialer
}

// Client is a single full-duplex connection to a GoRPC server.
type Client struct {
	conn             net.Conn
	codec            Codec
	maxFrameSize     int64
	handshakeTimeout time.Duration
	logger           *slog.Logger
	remoteService    string

	nextID atomic.Uint64

	writeMu sync.Mutex

	pendingMu sync.Mutex
	pending   map[uint64]chan clientResponse

	closeOnce sync.Once
	closed    chan struct{}

	closeErrMu sync.Mutex
	closeErr   error
}

type clientResponse struct {
	frame Frame
	err   error
}

// Dial connects to a GoRPC server and completes the protocol handshake.
func Dial(ctx context.Context, network, address string, opts ClientOptions) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if network == "" {
		network = "tcp"
	}

	dialer := opts.Dialer
	if dialer == nil {
		dialer = &net.Dialer{}
	}

	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}

	client := &Client{
		conn:             conn,
		codec:            defaultCodec(opts.Codec),
		maxFrameSize:     normalizeMaxFrameSize(opts.MaxFrameSize),
		handshakeTimeout: normalizeHandshakeTimeout(opts.HandshakeTimeout),
		logger:           opts.Logger,
		pending:          make(map[uint64]chan clientResponse),
		closed:           make(chan struct{}),
	}

	if err := client.clientHandshake(opts.ClientName, opts.ExpectedServiceName); err != nil {
		_ = conn.Close()
		return nil, err
	}

	go client.readLoop()

	return client, nil
}

// Call performs a typed unary request/response call.
func Call[Req, Resp any](ctx context.Context, client *Client, service, method string, req Req) (Resp, error) {
	var zero Resp
	if ctx == nil {
		ctx = context.Background()
	}
	if client == nil {
		return zero, ErrClosed
	}
	if service == "" || method == "" {
		return zero, ErrInvalidRoute
	}

	payload, err := client.codec.Marshal(req)
	if err != nil {
		return zero, fmt.Errorf("encode request: %w", err)
	}

	requestID := client.nextID.Add(1)
	responseCh := make(chan clientResponse, 1)
	if err := client.addPending(requestID, responseCh); err != nil {
		return zero, err
	}

	frame := Frame{
		Type:      FrameRequest,
		RequestID: requestID,
		Service:   service,
		Method:    method,
		Payload:   payload,
	}
	if deadline, ok := ctx.Deadline(); ok {
		frame.DeadlineUnixNano = deadline.UnixNano()
	}

	if err := client.write(frame); err != nil {
		client.removePending(requestID)
		return zero, err
	}

	select {
	case response := <-responseCh:
		if response.err != nil {
			return zero, response.err
		}

		switch response.frame.Type {
		case FrameResponse:
			var resp Resp
			if err := client.codec.Unmarshal(response.frame.Payload, &resp); err != nil {
				return zero, fmt.Errorf("decode response: %w", err)
			}
			return resp, nil
		case FrameError:
			var remoteErr RemoteError
			if err := client.codec.Unmarshal(response.frame.Payload, &remoteErr); err != nil {
				return zero, fmt.Errorf("decode remote error: %w", err)
			}
			return zero, &remoteErr
		default:
			return zero, fmt.Errorf("%w: unexpected response frame %s", ErrProtocol, response.frame.Type.String())
		}
	case <-ctx.Done():
		client.removePending(requestID)
		_ = client.write(Frame{Type: FrameCancel, RequestID: requestID})
		return zero, ctx.Err()
	case <-client.closed:
		client.removePending(requestID)
		return zero, client.closedError()
	}
}

// Method returns a typed function bound to a service and method.
func Method[Req, Resp any](client *Client, service, method string) HandlerFunc[Req, Resp] {
	return func(ctx context.Context, req Req) (Resp, error) {
		return Call[Req, Resp](ctx, client, service, method, req)
	}
}

// Close closes the client connection and fails any pending calls.
func (c *Client) Close() error {
	c.closeWithError(ErrClosed)
	return nil
}

// RemoteService returns the service name reported by the server handshake.
func (c *Client) RemoteService() string {
	if c == nil {
		return ""
	}

	return c.remoteService
}

func (c *Client) clientHandshake(clientName, expectedServiceName string) error {
	if c.handshakeTimeout > 0 {
		_ = c.conn.SetDeadline(time.Now().Add(c.handshakeTimeout))
		defer func() {
			_ = c.conn.SetDeadline(time.Time{})
		}()
	}

	payload, err := c.codec.Marshal(hello{
		ProtocolVersion: ProtocolVersion,
		Codec:           c.codec.Name(),
		ServiceName:     clientName,
	})
	if err != nil {
		return fmt.Errorf("encode hello: %w", err)
	}

	if err := writeFrame(c.conn, c.maxFrameSize, c.codec, Frame{
		Type:    FrameHello,
		Payload: payload,
	}); err != nil {
		return err
	}

	frame, err := readFrame(c.conn, c.maxFrameSize, c.codec)
	if err != nil {
		return err
	}
	if frame.Type != FrameHelloAck {
		return fmt.Errorf("%w: expected hello_ack, got %s", ErrProtocol, frame.Type.String())
	}

	var ack helloAck
	if err := c.codec.Unmarshal(frame.Payload, &ack); err != nil {
		return fmt.Errorf("decode hello ack: %w", err)
	}
	if ack.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrProtocol, ack.ProtocolVersion)
	}
	if ack.Codec != c.codec.Name() {
		return fmt.Errorf("%w: unsupported codec %q", ErrProtocol, ack.Codec)
	}
	if expectedServiceName != "" && ack.ServiceName != expectedServiceName {
		return fmt.Errorf("%w: expected service %q, got %q", ErrProtocol, expectedServiceName, ack.ServiceName)
	}

	c.remoteService = ack.ServiceName

	return nil
}

func (c *Client) readLoop() {
	for {
		frame, err := readFrame(c.conn, c.maxFrameSize, c.codec)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				c.closeWithError(ErrClosed)
				return
			}
			c.logDebug("gorpc read failed", "error", err)
			c.closeWithError(err)
			return
		}

		switch frame.Type {
		case FrameResponse, FrameError:
			if !c.complete(frame.RequestID, clientResponse{frame: frame}) {
				c.logDebug("gorpc discarded response for unknown request", "request_id", frame.RequestID)
			}
		case FramePing:
			_ = c.write(Frame{Type: FramePong, RequestID: frame.RequestID})
		case FramePong:
		default:
			c.logDebug("gorpc ignored frame", "type", frame.Type.String(), "request_id", frame.RequestID)
		}
	}
}

func (c *Client) addPending(requestID uint64, ch chan clientResponse) error {
	select {
	case <-c.closed:
		return c.closedError()
	default:
	}

	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	select {
	case <-c.closed:
		return c.closedError()
	default:
		c.pending[requestID] = ch
		return nil
	}
}

func (c *Client) removePending(requestID uint64) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	delete(c.pending, requestID)
}

func (c *Client) complete(requestID uint64, response clientResponse) bool {
	c.pendingMu.Lock()
	ch := c.pending[requestID]
	if ch != nil {
		delete(c.pending, requestID)
	}
	c.pendingMu.Unlock()

	if ch == nil {
		return false
	}

	ch <- response
	return true
}

func (c *Client) write(frame Frame) error {
	select {
	case <-c.closed:
		return c.closedError()
	default:
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return writeFrame(c.conn, c.maxFrameSize, c.codec, frame)
}

func (c *Client) closeWithError(err error) {
	if err == nil {
		err = ErrClosed
	}

	c.closeOnce.Do(func() {
		c.closeErrMu.Lock()
		c.closeErr = err
		c.closeErrMu.Unlock()

		close(c.closed)
		_ = c.conn.Close()

		c.pendingMu.Lock()
		for requestID, ch := range c.pending {
			delete(c.pending, requestID)
			ch <- clientResponse{err: err}
		}
		c.pendingMu.Unlock()
	})
}

func (c *Client) closedError() error {
	c.closeErrMu.Lock()
	defer c.closeErrMu.Unlock()

	if c.closeErr == nil {
		return ErrClosed
	}

	return c.closeErr
}

func (c *Client) logDebug(msg string, args ...any) {
	if c.logger != nil {
		c.logger.Debug(msg, args...)
	}
}
