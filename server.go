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

// DefaultHandshakeTimeout is the default timeout for the initial protocol handshake.
const DefaultHandshakeTimeout = 5 * time.Second

// ServerOptions configures a GoRPC server.
type ServerOptions struct {
	Codec            Codec
	MaxFrameSize     int64
	HandshakeTimeout time.Duration
	Auth             Auth
	WriteTimeout     time.Duration
	Logger           *slog.Logger
	OnConnect        func(*Conn)
	OnDisconnect     func(*Conn)
}

// HandlerFunc is the typed function shape used by registered unary functions.
type HandlerFunc[Req, Resp any] func(*Context, Req) (Resp, error)

// Server accepts GoRPC connections, dispatches registered functions, and exposes
// accepted connections that can initiate requests back to the dialing side.
type Server struct {
	codec            Codec
	maxFrameSize     int64
	handshakeTimeout time.Duration
	auth             Auth
	writeTimeout     time.Duration
	logger           *slog.Logger
	onConnect        func(*Conn)
	onDisconnect     func(*Conn)

	handlerMu sync.RWMutex
	handlers  map[string]handler

	listenerMu sync.Mutex
	listener   net.Listener

	connMu sync.Mutex
	conns  map[*Conn]struct{}

	shuttingDown atomic.Bool
	wg           sync.WaitGroup
}

type handler func(*Context, []byte) ([]byte, error)

type handlerRegistrar interface {
	handlerCodec() Codec
	registerHandler(function string, h handler) error
}

// NewServer creates a Server with default codec and limits where options are unset.
func NewServer(opts ServerOptions) *Server {
	return &Server{
		codec:            defaultCodec(opts.Codec),
		maxFrameSize:     normalizeMaxFrameSize(opts.MaxFrameSize),
		handshakeTimeout: normalizeHandshakeTimeout(opts.HandshakeTimeout),
		auth:             opts.Auth,
		writeTimeout:     normalizeWriteTimeout(opts.WriteTimeout),
		logger:           opts.Logger,
		onConnect:        opts.OnConnect,
		onDisconnect:     opts.OnDisconnect,
		handlers:         make(map[string]handler),
		conns:            make(map[*Conn]struct{}),
	}
}

// Register binds a typed unary handler to a function name. The target can be a
// *Server, for functions the accepted side handles, or a *Client, for functions
// the dialing side handles after the connection is established.
func Register[Req, Resp any](target any, function string, fn HandlerFunc[Req, Resp]) error {
	if target == nil {
		return ErrClosed
	}
	if function == "" || fn == nil {
		return ErrInvalidFunction
	}

	registrar, ok := target.(handlerRegistrar)
	if !ok {
		return ErrInvalidHandler
	}

	codec := registrar.handlerCodec()
	if codec == nil {
		return ErrClosed
	}

	return registrar.registerHandler(function, wrapHandler(codec, fn))
}

func wrapHandler[Req, Resp any](codec Codec, fn HandlerFunc[Req, Resp]) handler {
	wrapped := func(ctx *Context, payload []byte) ([]byte, error) {
		var req Req
		if err := codec.Unmarshal(payload, &req); err != nil {
			return nil, NewRemoteError(ErrorCodeInvalidRequest, fmt.Sprintf("decode request: %v", err), nil)
		}

		resp, err := fn(ctx, req)
		if err != nil {
			return nil, err
		}

		data, err := codec.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("encode response: %w", err)
		}

		return data, nil
	}

	return wrapped
}

func (s *Server) registerHandler(function string, h handler) error {
	if s == nil {
		return ErrClosed
	}
	if function == "" || h == nil {
		return ErrInvalidFunction
	}

	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()

	if s.handlers == nil {
		s.handlers = make(map[string]handler)
	}
	if _, exists := s.handlers[function]; exists {
		return ErrDuplicateFunction
	}
	s.handlers[function] = h

	return nil
}

func (s *Server) handlerCodec() Codec {
	if s == nil {
		return nil
	}

	return s.codec
}

// MustRegister is Register that panics on error.
func MustRegister[Req, Resp any](target any, function string, fn HandlerFunc[Req, Resp]) {
	if err := Register(target, function, fn); err != nil {
		panic(err)
	}
}

// ServeTCP listens on address with the "tcp" network and serves GoRPC connections.
func (s *Server) ServeTCP(address string) error {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	return s.ServeListener(ln)
}

// ServeUnix listens on path with the "unix" network and serves GoRPC connections.
func (s *Server) ServeUnix(path string) error {
	ln, err := net.Listen("unix", path)
	if err != nil {
		return err
	}

	return s.ServeListener(ln)
}

// ServeUnixPacket listens on path with the "unixpacket" network and serves GoRPC connections.
func (s *Server) ServeUnixPacket(path string) error {
	ln, err := net.Listen("unixpacket", path)
	if err != nil {
		return err
	}

	return s.ServeListener(ln)
}

// ServeListener accepts GoRPC connections from ln until Shutdown is called or
// the listener returns an unrecoverable error.
func (s *Server) ServeListener(ln net.Listener) error {
	if ln == nil {
		return fmt.Errorf("%w: nil listener", ErrProtocol)
	}
	if !s.setListener(ln) {
		return ErrClosed
	}
	defer s.clearListener(ln)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.shuttingDown.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serveConn(conn)
		}()
	}
}

// Shutdown closes the listener, closes active connections, and waits for handlers to exit.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	s.shuttingDown.Store(true)
	s.listenerMu.Lock()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.listenerMu.Unlock()

	s.connMu.Lock()
	for conn := range s.conns {
		_ = conn.close()
	}
	s.connMu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) serveConn(conn net.Conn) {
	sc := newConn(s, conn)
	s.addConn(sc)
	defer func() {
		s.removeConn(sc)
		_ = sc.close()
	}()

	clientName, err := s.serverHandshake(conn)
	if err != nil {
		s.logDebug("gorpc handshake failed", "error", err)
		return
	}
	sc.clientName = clientName
	go s.safeOnConnect(sc)
	defer s.safeOnDisconnect(sc)

	for {
		frame, err := readFrame(conn, s.maxFrameSize, s.codec)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.logDebug("gorpc read failed", "error", err)
			}
			sc.closeWithError(fmt.Errorf("%w: %v", ErrUnavailable, err))
			return
		}

		switch frame.Type {
		case FrameRequest:
			sc.startRequest(frame)
		case FrameResponse, FrameError:
			if !sc.complete(frame.RequestID, clientResponse{frame: frame}) {
				s.logDebug("gorpc discarded response for unknown request", "request_id", frame.RequestID)
			}
		case FrameCancel:
			sc.cancel(frame.RequestID)
		case FramePing:
			_ = sc.write(Frame{Type: FramePong, RequestID: frame.RequestID})
		default:
			s.logDebug("gorpc ignored frame", "type", frame.Type.String(), "request_id", frame.RequestID)
		}
	}
}

func (s *Server) serverHandshake(conn net.Conn) (string, error) {
	if s.handshakeTimeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(s.handshakeTimeout))
		defer func() {
			_ = conn.SetDeadline(time.Time{})
		}()
	}

	frame, err := readFrame(conn, s.maxFrameSize, s.codec)
	if err != nil {
		return "", err
	}
	if frame.Type != FrameHello {
		return "", fmt.Errorf("%w: expected hello, got %s", ErrProtocol, frame.Type.String())
	}

	var hello hello
	if err := s.codec.Unmarshal(frame.Payload, &hello); err != nil {
		return "", fmt.Errorf("decode hello: %w", err)
	}
	if hello.ProtocolVersion != ProtocolVersion {
		return "", fmt.Errorf("%w: unsupported version %d", ErrProtocol, hello.ProtocolVersion)
	}
	if hello.Codec != s.codec.Name() {
		return "", fmt.Errorf("%w: unsupported codec %q", ErrProtocol, hello.Codec)
	}

	ack := helloAck{
		ProtocolVersion: ProtocolVersion,
		Codec:           s.codec.Name(),
	}
	var authChallenge []byte
	if s.auth.enabledAuth() {
		authChallenge, err = s.auth.challenge()
		if err != nil {
			return "", err
		}
		ack.AuthRequired = true
		ack.AuthMethod = s.auth.method()
		ack.AuthChallenge = authChallenge
	}

	payload, err := s.codec.Marshal(ack)
	if err != nil {
		return "", fmt.Errorf("encode hello ack: %w", err)
	}

	if err := writeFrame(conn, s.maxFrameSize, s.codec, Frame{
		Type:    FrameHelloAck,
		Payload: payload,
	}); err != nil {
		return "", err
	}

	if s.auth.enabledAuth() {
		if err := s.readAuth(conn, hello, authChallenge); err != nil {
			return "", err
		}
	}

	return hello.ClientName, nil
}

func (s *Server) readAuth(conn net.Conn, hello hello, challenge []byte) error {
	frame, err := readFrame(conn, s.maxFrameSize, s.codec)
	if err != nil {
		return err
	}
	if frame.Type != FrameAuth {
		_ = s.writeHandshakeError(conn, RemoteError{
			Code:    ErrorCodeUnauthorized,
			Message: "authentication is required",
		})
		return fmt.Errorf("%w: expected auth, got %s", ErrAuthentication, frame.Type.String())
	}

	var request authRequest
	if err := s.codec.Unmarshal(frame.Payload, &request); err != nil {
		_ = s.writeHandshakeError(conn, RemoteError{
			Code:    ErrorCodeUnauthorized,
			Message: "invalid authentication payload",
		})
		return fmt.Errorf("%w: decode auth: %v", ErrAuthentication, err)
	}
	if request.Method != s.auth.method() {
		_ = s.writeHandshakeError(conn, RemoteError{
			Code:    ErrorCodeUnauthorized,
			Message: "unsupported authentication method",
		})
		return fmt.Errorf("%w: unsupported method %q", ErrAuthentication, request.Method)
	}
	if !s.auth.verify(challenge, hello.ProtocolVersion, hello.Codec, hello.ClientName, request.Signature) {
		_ = s.writeHandshakeError(conn, RemoteError{
			Code:    ErrorCodeUnauthorized,
			Message: "authentication failed",
		})
		return ErrAuthentication
	}

	payload, err := s.codec.Marshal(authAck{OK: true})
	if err != nil {
		return fmt.Errorf("encode auth ack: %w", err)
	}

	return writeFrame(conn, s.maxFrameSize, s.codec, Frame{
		Type:    FrameAuthAck,
		Payload: payload,
	})
}

func (s *Server) writeHandshakeError(conn net.Conn, remoteErr RemoteError) error {
	payload, err := s.codec.Marshal(remoteErr)
	if err != nil {
		return err
	}

	return writeFrame(conn, s.maxFrameSize, s.codec, Frame{
		Type:    FrameError,
		Payload: payload,
	})
}

func (s *Server) findHandler(function string) handler {
	s.handlerMu.RLock()
	defer s.handlerMu.RUnlock()

	return s.handlers[function]
}

func (s *Server) setListener(ln net.Listener) bool {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()

	if s.shuttingDown.Load() {
		return false
	}
	s.listener = ln

	return true
}

func (s *Server) clearListener(ln net.Listener) {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()

	if s.listener == ln {
		s.listener = nil
	}
}

// Connections returns a snapshot of currently accepted connections.
func (s *Server) Connections() []*Conn {
	if s == nil {
		return nil
	}

	s.connMu.Lock()
	defer s.connMu.Unlock()

	conns := make([]*Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}

	return conns
}

func (s *Server) addConn(conn *Conn) {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	s.conns[conn] = struct{}{}
}

func (s *Server) removeConn(conn *Conn) {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	delete(s.conns, conn)
}

func (s *Server) safeOnConnect(conn *Conn) {
	if s == nil || s.onConnect == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logDebug("gorpc on connect panic", "panic", recovered)
		}
	}()

	s.onConnect(conn)
}

func (s *Server) safeOnDisconnect(conn *Conn) {
	if s == nil || s.onDisconnect == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logDebug("gorpc on disconnect panic", "panic", recovered)
		}
	}()

	s.onDisconnect(conn)
}

func (s *Server) logDebug(msg string, args ...any) {
	if s.logger != nil {
		s.logger.Debug(msg, args...)
	}
}

// Conn is one accepted GoRPC connection. A Conn can receive requests through
// server-registered functions and can also initiate requests back to the client
// over the same full-duplex connection.
type Conn struct {
	server     *Server
	conn       net.Conn
	clientName string

	nextID  atomic.Uint64
	writeMu sync.Mutex

	pendingMu sync.Mutex
	pending   map[uint64]pendingCall

	requestMu sync.Mutex
	requests  map[uint64]context.CancelFunc

	closeOnce  sync.Once
	closed     chan struct{}
	closeErrMu sync.Mutex
	closeErr   error
}

func newConn(server *Server, conn net.Conn) *Conn {
	return &Conn{
		server:   server,
		conn:     conn,
		pending:  make(map[uint64]pendingCall),
		requests: make(map[uint64]context.CancelFunc),
		closed:   make(chan struct{}),
	}
}

// ClientName returns the self-reported client name from the connection
// handshake. It is useful for logs and metrics, but is not authenticated.
func (c *Conn) ClientName() string {
	if c == nil {
		return ""
	}

	return c.clientName
}

// RemoteAddr returns the peer address for the connection.
func (c *Conn) RemoteAddr() net.Addr {
	if c == nil || c.conn == nil {
		return nil
	}

	return c.conn.RemoteAddr()
}

// LocalAddr returns the local address for the connection.
func (c *Conn) LocalAddr() net.Addr {
	if c == nil || c.conn == nil {
		return nil
	}

	return c.conn.LocalAddr()
}

// Call performs a unary request/response call to the connected client.
func (c *Conn) Call(function string, req any, resp any) error {
	return c.CallContext(context.Background(), function, req, resp)
}

// CallWithTimeout performs a unary request/response call to the connected
// client with a timeout.
func (c *Conn) CallWithTimeout(function string, req any, resp any, timeout time.Duration) error {
	if timeout <= 0 {
		return c.Call(function, req, resp)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return c.CallContext(ctx, function, req, resp)
}

// CallContext performs a unary request/response call to the connected client.
func (c *Conn) CallContext(ctx context.Context, function string, req any, resp any) error {
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

		return decodeResponse(c.server.codec, response.frame, resp)
	case <-ctx.Done():
		c.removePending(requestID)
		_ = c.write(Frame{Type: FrameCancel, RequestID: requestID})
		return ctx.Err()
	case <-c.closed:
		c.removePending(requestID)
		return c.closedError()
	}
}

// AsyncCall sends a unary request to the connected client and invokes handler
// when the response arrives.
func (c *Conn) AsyncCall(function string, req any, handler any, correlationID string) error {
	return c.AsyncCallContext(context.Background(), function, req, handler, correlationID)
}

// AsyncCallWithTimeout sends a unary request to the connected client using a
// timeout while writing the request frame. The response handler runs later.
func (c *Conn) AsyncCallWithTimeout(function string, req any, handler any, correlationID string, timeout time.Duration) error {
	if timeout <= 0 {
		return c.AsyncCall(function, req, handler, correlationID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return c.AsyncCallContext(ctx, function, req, handler, correlationID)
}

// AsyncCallContext sends a unary request to the connected client and invokes
// handler when the response arrives.
func (c *Conn) AsyncCallContext(ctx context.Context, function string, req any, handler any, correlationID string) error {
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

func (c *Conn) sendRequest(ctx context.Context, function string, req any, pending pendingCall) (uint64, error) {
	ctx = normalizeContext(ctx)
	if c == nil || c.server == nil {
		return 0, ErrClosed
	}
	if function == "" {
		return 0, ErrInvalidFunction
	}

	payload, err := c.server.codec.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("encode request: %w", err)
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

	if err := c.write(frame); err != nil {
		c.removePending(requestID)
		c.closeWithError(err)
		return 0, err
	}

	return requestID, nil
}

func (c *Conn) startRequest(frame Frame) {
	if frame.RequestID == 0 {
		_ = c.writeError(frame, RemoteError{
			Code:    ErrorCodeInvalidRequest,
			Message: "request_id is required",
		})
		return
	}
	if frame.Function == "" {
		_ = c.writeError(frame, RemoteError{
			Code:    ErrorCodeInvalidRequest,
			Message: "function is required",
		})
		return
	}

	h := c.server.findHandler(frame.Function)
	if h == nil {
		_ = c.writeError(frame, RemoteError{
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
		clientName: c.clientName,
		requestID:  frame.RequestID,
		function:   frame.Function,
		remoteAddr: c.conn.RemoteAddr(),
		localAddr:  c.conn.LocalAddr(),
		conn:       c,
	}

	c.requestMu.Lock()
	c.requests[frame.RequestID] = cancel
	c.requestMu.Unlock()

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				_ = c.writeError(frame, RemoteError{
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
			_ = c.writeError(frame, remoteErrorFromError(err))
			return
		}

		_ = c.write(Frame{
			Type:      FrameResponse,
			RequestID: frame.RequestID,
			Function:  frame.Function,
			Payload:   payload,
		})
	}()
}

func (c *Conn) cancel(requestID uint64) {
	c.requestMu.Lock()
	cancel := c.requests[requestID]
	c.requestMu.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (c *Conn) writeError(request Frame, remoteErr RemoteError) error {
	payload, err := c.server.codec.Marshal(remoteErr)
	if err != nil {
		return err
	}

	return c.write(Frame{
		Type:      FrameError,
		RequestID: request.RequestID,
		Function:  request.Function,
		Payload:   payload,
	})
}

func (c *Conn) write(frame Frame) error {
	select {
	case <-c.closed:
		return c.closedError()
	default:
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	select {
	case <-c.closed:
		return c.closedError()
	default:
	}

	if c.server.writeTimeout > 0 {
		if err := c.conn.SetWriteDeadline(time.Now().Add(c.server.writeTimeout)); err != nil {
			return fmt.Errorf("set write deadline: %w", err)
		}
		defer func() {
			_ = c.conn.SetWriteDeadline(time.Time{})
		}()
	}

	return writeFrame(c.conn, c.server.maxFrameSize, c.server.codec, frame)
}

func (c *Conn) addPending(requestID uint64, pending pendingCall) error {
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

func (c *Conn) removePending(requestID uint64) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	delete(c.pending, requestID)
}

func (c *Conn) complete(requestID uint64, response clientResponse) bool {
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

func (c *Conn) failPending(err error) {
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

func (c *Conn) deliverPending(requestID uint64, pending pendingCall, response clientResponse) {
	switch call := pending.(type) {
	case syncPendingCall:
		call.ch <- response
	case asyncPendingCall:
		go c.invokeAsyncHandler(requestID, call, response)
	}
}

func (c *Conn) invokeAsyncHandler(requestID uint64, pending asyncPendingCall, response clientResponse) {
	defer func() {
		if recovered := recover(); recovered != nil {
			c.server.logDebug("gorpc async handler panic", "function", pending.function, "request_id", requestID, "panic", recovered)
		}
	}()

	resp := reflect.New(pending.responseType.Elem())
	err := response.err
	if err == nil {
		err = decodeResponse(c.server.codec, response.frame, resp.Interface())
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

// Close closes the accepted connection, cancels active inbound handlers, and
// fails pending outbound calls.
func (c *Conn) Close() error {
	return c.close()
}

func (c *Conn) close() error {
	c.closeWithError(ErrClosed)
	return nil
}

func (c *Conn) closeWithError(err error) {
	if err == nil {
		err = ErrClosed
	}

	c.closeOnce.Do(func() {
		c.closeErrMu.Lock()
		c.closeErr = err
		c.closeErrMu.Unlock()

		close(c.closed)

		c.requestMu.Lock()
		for _, cancel := range c.requests {
			cancel()
		}
		c.requests = make(map[uint64]context.CancelFunc)
		c.requestMu.Unlock()

		_ = c.conn.Close()
		c.failPending(err)
	})
}

func (c *Conn) closedError() error {
	c.closeErrMu.Lock()
	defer c.closeErrMu.Unlock()

	if c.closeErr == nil {
		return ErrClosed
	}

	return c.closeErr
}

func normalizeHandshakeTimeout(timeout time.Duration) time.Duration {
	if timeout < 0 {
		return 0
	}
	if timeout == 0 {
		return DefaultHandshakeTimeout
	}

	return timeout
}
