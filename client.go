package gorpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// Reconnect defaults used by Client when options are unset.
const (
	DefaultDialTimeout       = 5 * time.Second
	DefaultWriteTimeout      = 10 * time.Second
	DefaultReconnectMinDelay = 100 * time.Millisecond
	DefaultReconnectMaxDelay = 5 * time.Second
	DefaultReconnectJitter   = 0.2
	DefaultPingInterval      = 10 * time.Second
	DefaultPingTimeout       = 3 * time.Second
)

// ClientOptions configures Dial and the network-specific dial helpers.
type ClientOptions struct {
	ClientName       string
	Codec            Codec
	MaxFrameSize     int64
	HandshakeTimeout time.Duration
	Auth             Auth
	DialTimeout      time.Duration
	WriteTimeout     time.Duration
	Logger           *slog.Logger
	Dialer           *net.Dialer

	ReconnectMinDelay time.Duration
	ReconnectMaxDelay time.Duration
	ReconnectJitter   float64
	PingInterval      time.Duration
	PingTimeout       time.Duration
}

// Client is the dialing side of a long-lived full-duplex GoRPC connection. It
// can send requests, register functions for the accepted side to call, and
// reconnects automatically after connection loss until Close is called.
type Client struct {
	network string
	address string
	dialer  *net.Dialer

	codec            Codec
	maxFrameSize     int64
	handshakeTimeout time.Duration
	auth             Auth
	dialTimeout      time.Duration
	writeTimeout     time.Duration
	logger           *slog.Logger
	clientName       string

	reconnectMinDelay time.Duration
	reconnectMaxDelay time.Duration
	reconnectJitter   float64
	pingInterval      time.Duration
	pingTimeout       time.Duration

	nextID atomic.Uint64

	writeMu sync.Mutex

	handlerMu sync.RWMutex
	handlers  map[string]handler

	connMu sync.Mutex
	conn   net.Conn
	ready  chan struct{}

	lastFrameUnixNano atomic.Int64

	reconnectCh chan struct{}
	startOnce   sync.Once

	pendingMu sync.Mutex
	pending   map[uint64]pendingCall

	requestMu sync.Mutex
	requests  map[uint64]context.CancelFunc

	closeOnce sync.Once
	closed    chan struct{}

	closeErrMu sync.Mutex
	closeErr   error
}

type clientResponse struct {
	frame Frame
	err   error
}

type pendingCall any

type syncPendingCall struct {
	ch chan clientResponse
}

type asyncPendingCall struct {
	function      string
	correlationID string
	handler       reflect.Value
	responseType  reflect.Type
}

// ClientContext is passed to asynchronous response handlers for requests made
// by either a Client or an accepted Conn.
type ClientContext interface {
	CorrelationID() string
	RequestID() uint64
	Function() string
	Error() error
}

var clientContextType = reflect.TypeOf((*ClientContext)(nil)).Elem()

type clientContext struct {
	correlationID string
	requestID     uint64
	function      string
	err           error
}

func (c *clientContext) CorrelationID() string {
	return c.correlationID
}

func (c *clientContext) RequestID() uint64 {
	return c.requestID
}

func (c *clientContext) Function() string {
	return c.function
}

func (c *clientContext) Error() error {
	return c.err
}

// NewClient creates a client without connecting it. Use this when the dialing
// side needs to register functions before the accepted side can call them.
func NewClient(network, address string, opts ClientOptions) *Client {
	if network == "" {
		network = "tcp"
	}

	dialer := opts.Dialer
	if dialer == nil {
		dialer = &net.Dialer{}
	}

	client := &Client{
		network:           network,
		address:           address,
		dialer:            dialer,
		codec:             defaultCodec(opts.Codec),
		maxFrameSize:      normalizeMaxFrameSize(opts.MaxFrameSize),
		handshakeTimeout:  normalizeHandshakeTimeout(opts.HandshakeTimeout),
		auth:              opts.Auth,
		dialTimeout:       normalizeDialTimeout(opts.DialTimeout),
		writeTimeout:      normalizeWriteTimeout(opts.WriteTimeout),
		logger:            opts.Logger,
		clientName:        opts.ClientName,
		reconnectMinDelay: normalizeReconnectMinDelay(opts.ReconnectMinDelay),
		reconnectMaxDelay: normalizeReconnectMaxDelay(opts.ReconnectMaxDelay),
		reconnectJitter:   normalizeReconnectJitter(opts.ReconnectJitter),
		pingInterval:      normalizePingInterval(opts.PingInterval),
		pingTimeout:       normalizePingTimeout(opts.PingTimeout),
		ready:             make(chan struct{}),
		reconnectCh:       make(chan struct{}, 1),
		handlers:          make(map[string]handler),
		pending:           make(map[uint64]pendingCall),
		requests:          make(map[uint64]context.CancelFunc),
		closed:            make(chan struct{}),
	}

	return client
}

// NewTCPClient creates a TCP client without connecting it.
func NewTCPClient(address, clientName string, opts ...ClientOptions) *Client {
	return newClientWithOptions("tcp", address, clientName, opts...)
}

// NewUnixClient creates a Unix socket client without connecting it.
func NewUnixClient(path, clientName string, opts ...ClientOptions) *Client {
	return newClientWithOptions("unix", path, clientName, opts...)
}

// NewUnixPacketClient creates a Unix packet socket client without connecting it.
func NewUnixPacketClient(path, clientName string, opts ...ClientOptions) *Client {
	return newClientWithOptions("unixpacket", path, clientName, opts...)
}

func newClientWithOptions(network, address, clientName string, opts ...ClientOptions) *Client {
	options := ClientOptions{}
	if len(opts) > 0 {
		options = opts[0]
	}
	options.ClientName = clientName

	return NewClient(network, address, options)
}

// Dial connects to a GoRPC server, completes the protocol handshake, and starts
// background reconnect monitoring.
func Dial(ctx context.Context, network, address string, opts ClientOptions) (*Client, error) {
	client := NewClient(network, address, opts)

	if err := client.Connect(ctx); err != nil {
		return nil, err
	}

	return client, nil
}

// TCPDial connects to address using TCP and reconnects automatically until Close is called.
func TCPDial(address, clientName string, opts ...ClientOptions) (*Client, error) {
	return dialWithOptions(context.Background(), "tcp", address, clientName, opts...)
}

// UnixDial connects to path using a Unix socket and reconnects automatically until Close is called.
func UnixDial(path, clientName string, opts ...ClientOptions) (*Client, error) {
	return dialWithOptions(context.Background(), "unix", path, clientName, opts...)
}

// UnixPacketDial connects to path using a Unix packet socket and reconnects automatically until Close is called.
func UnixPacketDial(path, clientName string, opts ...ClientOptions) (*Client, error) {
	return dialWithOptions(context.Background(), "unixpacket", path, clientName, opts...)
}

func dialWithOptions(ctx context.Context, network, address, clientName string, opts ...ClientOptions) (*Client, error) {
	options := ClientOptions{}
	if len(opts) > 0 {
		options = opts[0]
	}
	options.ClientName = clientName

	return Dial(ctx, network, address, options)
}

// Call performs a unary request/response call using context.Background.
func (c *Client) Call(function string, req any, resp any) error {
	return c.CallContext(context.Background(), function, req, resp)
}

// CallWithTimeout performs a unary request/response call with a timeout.
func (c *Client) CallWithTimeout(function string, req any, resp any, timeout time.Duration) error {
	if timeout <= 0 {
		return c.Call(function, req, resp)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return c.CallContext(ctx, function, req, resp)
}

// CallContext performs a unary request/response call. If the client is
// reconnecting, CallContext waits for the next connection until ctx is canceled.
func (c *Client) CallContext(ctx context.Context, function string, req any, resp any) error {
	if c == nil {
		return ErrClosed
	}
	ctx = normalizeContext(ctx)
	if err := validateResponseTarget(resp); err != nil {
		return err
	}

	responseCh := make(chan clientResponse, 1)
	requestID, err := c.sendRequest(ctx, function, req, syncPendingCall{ch: responseCh})
	if err != nil {
		return err
	}

	select {
	case response := <-responseCh:
		if response.err != nil {
			return response.err
		}

		return decodeResponse(c.codec, response.frame, resp)
	case <-ctx.Done():
		c.removePending(requestID)
		if conn, ok := c.currentConn(); ok {
			_ = c.writeTo(conn, Frame{Type: FrameCancel, RequestID: requestID})
		}
		return ctx.Err()
	case <-c.closed:
		c.removePending(requestID)
		return c.closedError()
	}
}

// AsyncCall sends a unary request and invokes handler when the response arrives.
func (c *Client) AsyncCall(function string, req any, handler any, correlationID string) error {
	return c.AsyncCallContext(context.Background(), function, req, handler, correlationID)
}

// AsyncCallWithTimeout sends a unary request using a timeout while waiting for a
// connection and writing the request frame. The response handler runs later.
func (c *Client) AsyncCallWithTimeout(function string, req any, handler any, correlationID string, timeout time.Duration) error {
	if timeout <= 0 {
		return c.AsyncCall(function, req, handler, correlationID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return c.AsyncCallContext(ctx, function, req, handler, correlationID)
}

// AsyncCallContext sends a unary request and invokes handler when the response
// arrives. The context only controls waiting for a connection and writing the
// request frame.
func (c *Client) AsyncCallContext(ctx context.Context, function string, req any, handler any, correlationID string) error {
	if c == nil {
		return ErrClosed
	}
	ctx = normalizeContext(ctx)

	pending, err := newAsyncPendingCall(function, correlationID, handler)
	if err != nil {
		return err
	}

	_, err = c.sendRequest(ctx, function, req, pending)
	return err
}

// Call performs a typed unary request/response call.
func Call[Req, Resp any](ctx context.Context, client *Client, function string, req Req) (Resp, error) {
	var resp Resp
	if client == nil {
		return resp, ErrClosed
	}

	err := client.CallContext(ctx, function, req, &resp)
	return resp, err
}

// ClientFunc is the typed function shape returned by Function.
type ClientFunc[Req, Resp any] func(context.Context, Req) (Resp, error)

// Function returns a typed client function bound to a remote function name.
func Function[Req, Resp any](client *Client, function string) ClientFunc[Req, Resp] {
	return func(ctx context.Context, req Req) (Resp, error) {
		return Call[Req, Resp](ctx, client, function, req)
	}
}

func (c *Client) sendRequest(ctx context.Context, function string, req any, pending pendingCall) (uint64, error) {
	ctx = normalizeContext(ctx)
	if c == nil {
		return 0, ErrClosed
	}
	if function == "" {
		return 0, ErrInvalidFunction
	}

	payload, err := c.codec.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("encode request: %w", err)
	}

	conn, err := c.waitConn(ctx)
	if err != nil {
		return 0, err
	}

	requestID := c.nextID.Add(1)
	if err := c.addPending(requestID, pending); err != nil {
		return 0, err
	}

	frame := Frame{
		Type:      FrameRequest,
		RequestID: requestID,
		Function:  function,
		Payload:   payload,
	}
	if deadline, ok := ctx.Deadline(); ok {
		frame.DeadlineUnixNano = deadline.UnixNano()
	}

	if err := c.writeTo(conn, frame); err != nil {
		c.removePending(requestID)
		c.connectionLost(conn, err)
		return 0, err
	}

	return requestID, nil
}

func decodeResponse(codec Codec, frame Frame, resp any) error {
	codec = defaultCodec(codec)

	switch frame.Type {
	case FrameResponse:
		if err := codec.Unmarshal(frame.Payload, resp); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
		return nil
	case FrameError:
		var remoteErr RemoteError
		if err := codec.Unmarshal(frame.Payload, &remoteErr); err != nil {
			return fmt.Errorf("decode remote error: %w", err)
		}
		return &remoteErr
	default:
		return fmt.Errorf("%w: unexpected response frame %s", ErrProtocol, frame.Type.String())
	}
}

func (c *Client) decodeResponse(frame Frame, resp any) error {
	if c == nil {
		return ErrClosed
	}

	return decodeResponse(c.codec, frame, resp)
}

func newAsyncPendingCall(function, correlationID string, handler any) (asyncPendingCall, error) {
	var pending asyncPendingCall
	if handler == nil {
		return pending, fmt.Errorf("%w: handler is nil", ErrInvalidHandler)
	}

	handlerValue := reflect.ValueOf(handler)
	if handlerValue.Kind() != reflect.Func || handlerValue.IsNil() {
		return pending, fmt.Errorf("%w: handler must be func(gorpc.ClientContext, *Response)", ErrInvalidHandler)
	}

	handlerType := handlerValue.Type()
	if handlerType.NumIn() != 2 || handlerType.NumOut() != 0 {
		return pending, fmt.Errorf("%w: handler must be func(gorpc.ClientContext, *Response)", ErrInvalidHandler)
	}
	if handlerType.In(0) != clientContextType {
		return pending, fmt.Errorf("%w: first handler argument must be gorpc.ClientContext", ErrInvalidHandler)
	}

	responseType := handlerType.In(1)
	if responseType.Kind() != reflect.Ptr {
		return pending, fmt.Errorf("%w: second handler argument must be *Response", ErrInvalidHandler)
	}

	return asyncPendingCall{
		function:      function,
		correlationID: correlationID,
		handler:       handlerValue,
		responseType:  responseType,
	}, nil
}

func validateResponseTarget(resp any) error {
	if resp == nil {
		return fmt.Errorf("%w: response must be a non-nil pointer", ErrInvalidResponse)
	}

	value := reflect.ValueOf(resp)
	if value.Kind() != reflect.Ptr || value.IsNil() {
		return fmt.Errorf("%w: response must be a non-nil pointer", ErrInvalidResponse)
	}

	return nil
}

func (c *Client) registerHandler(function string, h handler) error {
	if c == nil {
		return ErrClosed
	}
	if function == "" || h == nil {
		return ErrInvalidFunction
	}

	c.handlerMu.Lock()
	defer c.handlerMu.Unlock()

	if c.handlers == nil {
		c.handlers = make(map[string]handler)
	}
	if _, exists := c.handlers[function]; exists {
		return ErrDuplicateFunction
	}
	c.handlers[function] = h

	return nil
}

func (c *Client) handlerCodec() Codec {
	if c == nil {
		return nil
	}

	return c.codec
}

func (c *Client) findHandler(function string) handler {
	c.handlerMu.RLock()
	defer c.handlerMu.RUnlock()

	return c.handlers[function]
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}

	return ctx
}

// Connect establishes the first connection and starts background reconnect
// monitoring. It is called automatically by Dial and the TCPDial helpers.
func (c *Client) Connect(ctx context.Context) error {
	if c == nil {
		return ErrClosed
	}
	ctx = normalizeContext(ctx)

	if _, ok := c.currentConn(); ok {
		c.startReconnectLoop()
		return nil
	}

	if err := c.connectUntilReady(ctx); err != nil {
		return err
	}
	c.startReconnectLoop()

	return nil
}

func (c *Client) startReconnectLoop() {
	c.startOnce.Do(func() {
		go c.reconnectLoop()
	})
}

// WaitReady blocks until the client has an active connection or ctx is canceled.
func (c *Client) WaitReady(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	_, err := c.waitConn(ctx)
	return err
}

// Close closes the client and stops reconnect attempts.
func (c *Client) Close() error {
	c.closeWithError(ErrClosed)
	return nil
}

func (c *Client) connectUntilReady(ctx context.Context) error {
	delay := c.reconnectMinDelay
	var lastErr error

	for {
		if err := c.connectOnce(ctx); err != nil {
			if isPermanentInitialConnectError(err) {
				return err
			}
			lastErr = err
			c.logDebug("gorpc connect failed", "network", c.network, "address", c.address, "error", err)
		} else {
			return nil
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("%w: %v", ctx.Err(), lastErr)
			}
			return ctx.Err()
		case <-c.closed:
			return c.closedError()
		case <-time.After(c.jitterReconnectDelay(delay)):
			delay = nextReconnectDelay(delay, c.reconnectMaxDelay)
		}
	}
}

func isPermanentInitialConnectError(err error) bool {
	return errors.Is(err, ErrAuthentication) || errors.Is(err, ErrProtocol)
}

func (c *Client) reconnectLoop() {
	for {
		select {
		case <-c.closed:
			return
		case <-c.reconnectCh:
		}

		delay := c.reconnectMinDelay
		for {
			select {
			case <-c.closed:
				return
			default:
			}

			if err := c.connectOnce(context.Background()); err != nil {
				c.logDebug("gorpc reconnect failed", "network", c.network, "address", c.address, "error", err)
				select {
				case <-c.closed:
					return
				case <-time.After(c.jitterReconnectDelay(delay)):
					delay = nextReconnectDelay(delay, c.reconnectMaxDelay)
					continue
				}
			}

			break
		}
	}
}

func (c *Client) connectOnce(ctx context.Context) error {
	ctx = normalizeContext(ctx)
	if c.dialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.dialTimeout)
		defer cancel()
	}

	conn, err := c.dialer.DialContext(ctx, c.network, c.address)
	if err != nil {
		return err
	}

	if err := c.clientHandshake(conn); err != nil {
		_ = conn.Close()
		return err
	}

	c.setConn(conn)
	c.logDebug("gorpc connected", "network", c.network, "address", c.address)

	go c.readLoop(conn)
	if c.pingInterval > 0 && c.pingTimeout > 0 {
		go c.pingLoop(conn)
	}

	return nil
}

func (c *Client) setConn(conn net.Conn) {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn != nil {
		_ = c.conn.Close()
	}

	c.conn = conn
	c.lastFrameUnixNano.Store(time.Now().UnixNano())
	close(c.ready)
}

func (c *Client) waitConn(ctx context.Context) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		c.connMu.Lock()
		conn := c.conn
		ready := c.ready
		c.connMu.Unlock()

		if conn != nil {
			return conn, nil
		}

		select {
		case <-ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, c.closedError()
		}
	}
}

func (c *Client) currentConn() (net.Conn, bool) {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn == nil {
		return nil, false
	}

	return c.conn, true
}

func (c *Client) isCurrentConn(conn net.Conn) bool {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	return c.conn == conn
}

func (c *Client) clientHandshake(conn net.Conn) error {
	if c.handshakeTimeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(c.handshakeTimeout))
		defer func() {
			_ = conn.SetDeadline(time.Time{})
		}()
	}

	payload, err := c.codec.Marshal(hello{
		ProtocolVersion: ProtocolVersion,
		Codec:           c.codec.Name(),
		ClientName:      c.clientName,
		AuthMethod:      c.auth.method(),
	})
	if err != nil {
		return fmt.Errorf("encode hello: %w", err)
	}

	if err := writeFrame(conn, c.maxFrameSize, c.codec, Frame{
		Type:    FrameHello,
		Payload: payload,
	}); err != nil {
		return err
	}

	frame, err := readFrame(conn, c.maxFrameSize, c.codec)
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
	if ack.AuthRequired {
		if err := c.clientAuth(conn, ack); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) clientAuth(conn net.Conn, ack helloAck) error {
	if !c.auth.enabledAuth() {
		return fmt.Errorf("%w: server requires authentication", ErrAuthentication)
	}
	if ack.AuthMethod != c.auth.method() {
		return fmt.Errorf("%w: unsupported method %q", ErrAuthentication, ack.AuthMethod)
	}
	if len(ack.AuthChallenge) == 0 {
		return fmt.Errorf("%w: missing challenge", ErrAuthentication)
	}

	signature := c.auth.sign(ack.AuthChallenge, ack.ProtocolVersion, ack.Codec, c.clientName)
	payload, err := c.codec.Marshal(authRequest{
		Method:    c.auth.method(),
		Signature: signature,
	})
	if err != nil {
		return fmt.Errorf("encode auth: %w", err)
	}

	if err := writeFrame(conn, c.maxFrameSize, c.codec, Frame{
		Type:    FrameAuth,
		Payload: payload,
	}); err != nil {
		return err
	}

	frame, err := readFrame(conn, c.maxFrameSize, c.codec)
	if err != nil {
		return err
	}
	switch frame.Type {
	case FrameAuthAck:
		var authAck authAck
		if err := c.codec.Unmarshal(frame.Payload, &authAck); err != nil {
			return fmt.Errorf("decode auth ack: %w", err)
		}
		if !authAck.OK {
			return ErrAuthentication
		}
		return nil
	case FrameError:
		var remoteErr RemoteError
		if err := c.codec.Unmarshal(frame.Payload, &remoteErr); err != nil {
			return fmt.Errorf("%w: decode auth error: %v", ErrAuthentication, err)
		}
		return fmt.Errorf("%w: %s", ErrAuthentication, remoteErr.Message)
	default:
		return fmt.Errorf("%w: expected auth_ack, got %s", ErrProtocol, frame.Type.String())
	}
}

func (c *Client) readLoop(conn net.Conn) {
	for {
		frame, err := readFrame(conn, c.maxFrameSize, c.codec)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				c.connectionLost(conn, ErrUnavailable)
				return
			}
			c.logDebug("gorpc read failed", "error", err)
			c.connectionLost(conn, err)
			return
		}

		c.lastFrameUnixNano.Store(time.Now().UnixNano())

		switch frame.Type {
		case FrameRequest:
			c.startRequest(conn, frame)
		case FrameResponse, FrameError:
			if !c.complete(frame.RequestID, clientResponse{frame: frame}) {
				c.logDebug("gorpc discarded response for unknown request", "request_id", frame.RequestID)
			}
		case FrameCancel:
			c.cancel(frame.RequestID)
		case FramePing:
			if err := c.writeTo(conn, Frame{Type: FramePong, RequestID: frame.RequestID}); err != nil {
				c.connectionLost(conn, err)
				return
			}
		case FramePong:
		default:
			c.logDebug("gorpc ignored frame", "type", frame.Type.String(), "request_id", frame.RequestID)
		}
	}
}

func (c *Client) startRequest(conn net.Conn, frame Frame) {
	if frame.RequestID == 0 {
		_ = c.writeErrorTo(conn, frame, RemoteError{
			Code:    ErrorCodeInvalidRequest,
			Message: "request_id is required",
		})
		return
	}
	if frame.Function == "" {
		_ = c.writeErrorTo(conn, frame, RemoteError{
			Code:    ErrorCodeInvalidRequest,
			Message: "function is required",
		})
		return
	}

	h := c.findHandler(frame.Function)
	if h == nil {
		_ = c.writeErrorTo(conn, frame, RemoteError{
			Code:    ErrorCodeNotFound,
			Message: fmt.Sprintf("function %q is not registered", frame.Function),
		})
		return
	}

	var ctx context.Context
	var cancel context.CancelFunc
	if frame.DeadlineUnixNano > 0 {
		deadline := time.Unix(0, frame.DeadlineUnixNano)
		ctx, cancel = context.WithDeadline(context.Background(), deadline)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}

	rpcCtx := &Context{
		Context:    ctx,
		requestID:  frame.RequestID,
		function:   frame.Function,
		remoteAddr: conn.RemoteAddr(),
		localAddr:  conn.LocalAddr(),
	}

	c.requestMu.Lock()
	c.requests[frame.RequestID] = cancel
	c.requestMu.Unlock()

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				_ = c.writeErrorTo(conn, frame, RemoteError{
					Code:    ErrorCodeInternal,
					Message: fmt.Sprintf("handler panic: %v", recovered),
				})
			}
			cancel()
			c.requestMu.Lock()
			delete(c.requests, frame.RequestID)
			c.requestMu.Unlock()
		}()

		payload, err := h(rpcCtx, frame.Payload)
		if err != nil {
			_ = c.writeErrorTo(conn, frame, remoteErrorFromError(err))
			return
		}

		_ = c.writeTo(conn, Frame{
			Type:      FrameResponse,
			RequestID: frame.RequestID,
			Function:  frame.Function,
			Payload:   payload,
		})
	}()
}

func (c *Client) cancel(requestID uint64) {
	c.requestMu.Lock()
	cancel := c.requests[requestID]
	c.requestMu.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (c *Client) writeErrorTo(conn net.Conn, request Frame, remoteErr RemoteError) error {
	payload, err := c.codec.Marshal(remoteErr)
	if err != nil {
		return err
	}

	return c.writeTo(conn, Frame{
		Type:      FrameError,
		RequestID: request.RequestID,
		Function:  request.Function,
		Payload:   payload,
	})
}

func (c *Client) pingLoop(conn net.Conn) {
	timer := time.NewTimer(c.pingInterval)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
		case <-c.closed:
			return
		}

		if !c.isCurrentConn(conn) {
			return
		}

		start := time.Now()
		requestID := c.nextID.Add(1)
		if err := c.writeTo(conn, Frame{Type: FramePing, RequestID: requestID}); err != nil {
			c.connectionLost(conn, err)
			return
		}

		select {
		case <-time.After(c.pingTimeout):
		case <-c.closed:
			return
		}

		if !c.isCurrentConn(conn) {
			return
		}
		if time.Unix(0, c.lastFrameUnixNano.Load()).Before(start) {
			c.connectionLost(conn, fmt.Errorf("%w: ping timeout", ErrUnavailable))
			return
		}

		timer.Reset(c.pingInterval)
	}
}

func (c *Client) connectionLost(conn net.Conn, err error) {
	if err == nil {
		err = ErrUnavailable
	}
	if errors.Is(err, ErrClosed) {
		err = ErrUnavailable
	}

	c.connMu.Lock()
	if c.conn != conn {
		c.connMu.Unlock()
		return
	}
	c.conn = nil
	c.ready = make(chan struct{})
	c.connMu.Unlock()

	_ = conn.Close()
	c.cancelRequests()
	c.failPending(fmt.Errorf("%w: %v", ErrUnavailable, err))
	c.signalReconnect()
}

func (c *Client) signalReconnect() {
	select {
	case c.reconnectCh <- struct{}{}:
	default:
	}
}

func (c *Client) addPending(requestID uint64, pending pendingCall) error {
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
		c.pending[requestID] = pending
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
	pending := c.pending[requestID]
	if pending != nil {
		delete(c.pending, requestID)
	}
	c.pendingMu.Unlock()

	if pending == nil {
		return false
	}

	c.deliverPending(requestID, pending, response)
	return true
}

func (c *Client) failPending(err error) {
	if err == nil {
		err = ErrUnavailable
	}

	c.pendingMu.Lock()
	pendingCalls := make(map[uint64]pendingCall, len(c.pending))
	for requestID, pending := range c.pending {
		delete(c.pending, requestID)
		pendingCalls[requestID] = pending
	}
	c.pendingMu.Unlock()

	for requestID, pending := range pendingCalls {
		c.deliverPending(requestID, pending, clientResponse{err: err})
	}
}

func (c *Client) deliverPending(requestID uint64, pending pendingCall, response clientResponse) {
	switch call := pending.(type) {
	case syncPendingCall:
		call.ch <- response
	case asyncPendingCall:
		go c.invokeAsyncHandler(requestID, call, response)
	}
}

func (c *Client) invokeAsyncHandler(requestID uint64, pending asyncPendingCall, response clientResponse) {
	defer func() {
		if recovered := recover(); recovered != nil {
			c.logDebug("gorpc async handler panic", "function", pending.function, "request_id", requestID, "panic", recovered)
		}
	}()

	resp := reflect.New(pending.responseType.Elem())
	err := response.err
	if err == nil {
		err = c.decodeResponse(response.frame, resp.Interface())
	}

	ctx := &clientContext{
		correlationID: pending.correlationID,
		requestID:     requestID,
		function:      pending.function,
		err:           err,
	}

	pending.handler.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		resp,
	})
}

func (c *Client) writeTo(conn net.Conn, frame Frame) error {
	select {
	case <-c.closed:
		return c.closedError()
	default:
	}

	if !c.isCurrentConn(conn) {
		return ErrUnavailable
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if !c.isCurrentConn(conn) {
		return ErrUnavailable
	}

	if c.writeTimeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			return fmt.Errorf("set write deadline: %w", err)
		}
		defer func() {
			_ = conn.SetWriteDeadline(time.Time{})
		}()
	}

	return writeFrame(conn, c.maxFrameSize, c.codec, frame)
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

		c.connMu.Lock()
		if c.conn != nil {
			_ = c.conn.Close()
			c.conn = nil
		}
		select {
		case <-c.ready:
		default:
			close(c.ready)
		}
		c.connMu.Unlock()

		c.cancelRequests()
		c.failPending(err)
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

func (c *Client) cancelRequests() {
	c.requestMu.Lock()
	for _, cancel := range c.requests {
		cancel()
	}
	c.requests = make(map[uint64]context.CancelFunc)
	c.requestMu.Unlock()
}

func normalizeReconnectMinDelay(delay time.Duration) time.Duration {
	if delay < 0 {
		return 0
	}
	if delay == 0 {
		return DefaultReconnectMinDelay
	}

	return delay
}

func normalizeDialTimeout(timeout time.Duration) time.Duration {
	if timeout < 0 {
		return 0
	}
	if timeout == 0 {
		return DefaultDialTimeout
	}

	return timeout
}

func normalizeWriteTimeout(timeout time.Duration) time.Duration {
	if timeout < 0 {
		return 0
	}
	if timeout == 0 {
		return DefaultWriteTimeout
	}

	return timeout
}

func normalizeReconnectMaxDelay(delay time.Duration) time.Duration {
	if delay < 0 {
		return 0
	}
	if delay == 0 {
		return DefaultReconnectMaxDelay
	}

	return delay
}

func normalizeReconnectJitter(jitter float64) float64 {
	switch {
	case jitter < 0:
		return 0
	case jitter == 0:
		return DefaultReconnectJitter
	case jitter > 1:
		return 1
	default:
		return jitter
	}
}

func normalizePingInterval(interval time.Duration) time.Duration {
	if interval < 0 {
		return 0
	}
	if interval == 0 {
		return DefaultPingInterval
	}

	return interval
}

func normalizePingTimeout(timeout time.Duration) time.Duration {
	if timeout < 0 {
		return 0
	}
	if timeout == 0 {
		return DefaultPingTimeout
	}

	return timeout
}

func (c *Client) jitterReconnectDelay(delay time.Duration) time.Duration {
	if delay <= 0 || c.reconnectJitter <= 0 {
		return delay
	}

	maxOffset := time.Duration(float64(delay) * c.reconnectJitter)
	if maxOffset <= 0 {
		return delay
	}

	span := int64(maxOffset)*2 + 1
	if span <= 0 {
		return delay
	}

	n := time.Now().UnixNano()
	if n < 0 {
		n = -n
	}

	jitter := time.Duration(n%span) - maxOffset
	result := delay + jitter
	if result < 0 {
		return 0
	}

	return result
}

func nextReconnectDelay(delay, maxDelay time.Duration) time.Duration {
	if delay <= 0 {
		return maxDelay
	}
	if maxDelay <= 0 {
		return delay
	}

	next := delay * 2
	if next > maxDelay || next < delay {
		return maxDelay
	}

	return next
}
