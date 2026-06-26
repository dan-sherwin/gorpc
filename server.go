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

// DefaultHandshakeTimeout is the default timeout for the initial protocol handshake.
const DefaultHandshakeTimeout = 5 * time.Second

// ServerOptions configures a GoRPC server.
type ServerOptions struct {
	Codec            Codec
	MaxFrameSize     int64
	HandshakeTimeout time.Duration
	Auth             Auth
	Logger           *slog.Logger
}

// HandlerFunc is the typed function shape used by registered unary functions.
type HandlerFunc[Req, Resp any] func(*Context, Req) (Resp, error)

// Server accepts GoRPC connections and dispatches registered functions.
type Server struct {
	codec            Codec
	maxFrameSize     int64
	handshakeTimeout time.Duration
	auth             Auth
	logger           *slog.Logger

	handlerMu sync.RWMutex
	handlers  map[string]handler

	listenerMu sync.Mutex
	listener   net.Listener

	connMu sync.Mutex
	conns  map[*serverConn]struct{}

	shuttingDown atomic.Bool
	wg           sync.WaitGroup
}

type handler func(*Context, []byte) ([]byte, error)

// NewServer creates a Server with default codec and limits where options are unset.
func NewServer(opts ServerOptions) *Server {
	return &Server{
		codec:            defaultCodec(opts.Codec),
		maxFrameSize:     normalizeMaxFrameSize(opts.MaxFrameSize),
		handshakeTimeout: normalizeHandshakeTimeout(opts.HandshakeTimeout),
		auth:             opts.Auth,
		logger:           opts.Logger,
		handlers:         make(map[string]handler),
		conns:            make(map[*serverConn]struct{}),
	}
}

// Register binds a typed unary handler to a function name.
func Register[Req, Resp any](s *Server, function string, fn HandlerFunc[Req, Resp]) error {
	if s == nil {
		return ErrClosed
	}
	if function == "" || fn == nil {
		return ErrInvalidFunction
	}

	wrapped := func(ctx *Context, payload []byte) ([]byte, error) {
		var req Req
		if err := s.codec.Unmarshal(payload, &req); err != nil {
			return nil, NewRemoteError(ErrorCodeInvalidRequest, fmt.Sprintf("decode request: %v", err), nil)
		}

		resp, err := fn(ctx, req)
		if err != nil {
			return nil, err
		}

		data, err := s.codec.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("encode response: %w", err)
		}

		return data, nil
	}

	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()

	if _, exists := s.handlers[function]; exists {
		return ErrDuplicateFunction
	}
	s.handlers[function] = wrapped

	return nil
}

// MustRegister is Register that panics on error.
func MustRegister[Req, Resp any](s *Server, function string, fn HandlerFunc[Req, Resp]) {
	if err := Register(s, function, fn); err != nil {
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
	sc := newServerConn(s, conn)
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

	for {
		frame, err := readFrame(conn, s.maxFrameSize, s.codec)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.logDebug("gorpc read failed", "error", err)
			}
			return
		}

		switch frame.Type {
		case FrameRequest:
			sc.startRequest(frame)
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

func (s *Server) addConn(conn *serverConn) {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	s.conns[conn] = struct{}{}
}

func (s *Server) removeConn(conn *serverConn) {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	delete(s.conns, conn)
}

func (s *Server) logDebug(msg string, args ...any) {
	if s.logger != nil {
		s.logger.Debug(msg, args...)
	}
}

type serverConn struct {
	server     *Server
	conn       net.Conn
	clientName string

	writeMu sync.Mutex

	requestMu sync.Mutex
	requests  map[uint64]context.CancelFunc
}

func newServerConn(server *Server, conn net.Conn) *serverConn {
	return &serverConn{
		server:   server,
		conn:     conn,
		requests: make(map[uint64]context.CancelFunc),
	}
}

func (c *serverConn) startRequest(frame Frame) {
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

func (c *serverConn) cancel(requestID uint64) {
	c.requestMu.Lock()
	cancel := c.requests[requestID]
	c.requestMu.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (c *serverConn) writeError(request Frame, remoteErr RemoteError) error {
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

func (c *serverConn) write(frame Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return writeFrame(c.conn, c.server.maxFrameSize, c.server.codec, frame)
}

func (c *serverConn) close() error {
	c.requestMu.Lock()
	for _, cancel := range c.requests {
		cancel()
	}
	c.requests = make(map[uint64]context.CancelFunc)
	c.requestMu.Unlock()

	return c.conn.Close()
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
