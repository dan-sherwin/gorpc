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
	ServiceName      string
	Codec            Codec
	MaxFrameSize     int64
	HandshakeTimeout time.Duration
	Logger           *slog.Logger
}

// HandlerFunc is the typed function shape used by registered unary methods.
type HandlerFunc[Req, Resp any] func(context.Context, Req) (Resp, error)

// Server accepts GoRPC connections and dispatches registered methods.
type Server struct {
	serviceName      string
	codec            Codec
	maxFrameSize     int64
	handshakeTimeout time.Duration
	logger           *slog.Logger

	handlerMu sync.RWMutex
	handlers  map[route]handler

	listenerMu sync.Mutex
	listener   net.Listener

	connMu sync.Mutex
	conns  map[*serverConn]struct{}

	shuttingDown atomic.Bool
	wg           sync.WaitGroup
}

type route struct {
	service string
	method  string
}

type handler func(context.Context, []byte) ([]byte, error)

// NewServer creates a Server with default codec and limits where options are unset.
func NewServer(opts ServerOptions) *Server {
	return &Server{
		serviceName:      opts.ServiceName,
		codec:            defaultCodec(opts.Codec),
		maxFrameSize:     normalizeMaxFrameSize(opts.MaxFrameSize),
		handshakeTimeout: normalizeHandshakeTimeout(opts.HandshakeTimeout),
		logger:           opts.Logger,
		handlers:         make(map[route]handler),
		conns:            make(map[*serverConn]struct{}),
	}
}

// Register binds a typed unary handler to a service and method name.
func Register[Req, Resp any](s *Server, service, method string, fn HandlerFunc[Req, Resp]) error {
	if s == nil {
		return ErrClosed
	}
	if service == "" || method == "" || fn == nil {
		return ErrInvalidRoute
	}

	r := route{service: service, method: method}
	wrapped := func(ctx context.Context, payload []byte) ([]byte, error) {
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

	if _, exists := s.handlers[r]; exists {
		return ErrDuplicateRoute
	}
	s.handlers[r] = wrapped

	return nil
}

// MustRegister is Register that panics on error.
func MustRegister[Req, Resp any](s *Server, service, method string, fn HandlerFunc[Req, Resp]) {
	if err := Register(s, service, method, fn); err != nil {
		panic(err)
	}
}

// Serve accepts GoRPC connections from ln until ctx is canceled or Shutdown is called.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if ln == nil {
		return fmt.Errorf("%w: nil listener", ErrProtocol)
	}
	if !s.setListener(ln) {
		return ErrClosed
	}
	defer s.clearListener(ln)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.shuttingDown.Load() || ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serveConn(ctx, conn)
		}()
	}
}

// ListenAndServe listens on network/address and serves GoRPC connections.
func (s *Server) ListenAndServe(ctx context.Context, network, address string) error {
	if network == "" {
		network = "tcp"
	}

	ln, err := net.Listen(network, address)
	if err != nil {
		return err
	}

	return s.Serve(ctx, ln)
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

func (s *Server) serveConn(parent context.Context, conn net.Conn) {
	sc := newServerConn(s, conn)
	s.addConn(sc)
	defer func() {
		s.removeConn(sc)
		_ = sc.close()
	}()

	if err := s.serverHandshake(conn); err != nil {
		s.logDebug("gorpc handshake failed", "error", err)
		return
	}

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
			sc.startRequest(parent, frame)
		case FrameCancel:
			sc.cancel(frame.RequestID)
		case FramePing:
			_ = sc.write(Frame{Type: FramePong, RequestID: frame.RequestID})
		default:
			s.logDebug("gorpc ignored frame", "type", frame.Type.String(), "request_id", frame.RequestID)
		}
	}
}

func (s *Server) serverHandshake(conn net.Conn) error {
	if s.handshakeTimeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(s.handshakeTimeout))
		defer func() {
			_ = conn.SetDeadline(time.Time{})
		}()
	}

	frame, err := readFrame(conn, s.maxFrameSize, s.codec)
	if err != nil {
		return err
	}
	if frame.Type != FrameHello {
		return fmt.Errorf("%w: expected hello, got %s", ErrProtocol, frame.Type.String())
	}

	var hello hello
	if err := s.codec.Unmarshal(frame.Payload, &hello); err != nil {
		return fmt.Errorf("decode hello: %w", err)
	}
	if hello.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrProtocol, hello.ProtocolVersion)
	}
	if hello.Codec != s.codec.Name() {
		return fmt.Errorf("%w: unsupported codec %q", ErrProtocol, hello.Codec)
	}

	payload, err := s.codec.Marshal(helloAck{
		ProtocolVersion: ProtocolVersion,
		Codec:           s.codec.Name(),
		ServiceName:     s.serviceName,
	})
	if err != nil {
		return fmt.Errorf("encode hello ack: %w", err)
	}

	return writeFrame(conn, s.maxFrameSize, s.codec, Frame{
		Type:    FrameHelloAck,
		Payload: payload,
	})
}

func (s *Server) findHandler(service, method string) handler {
	s.handlerMu.RLock()
	defer s.handlerMu.RUnlock()

	return s.handlers[route{service: service, method: method}]
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
	server *Server
	conn   net.Conn

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

func (c *serverConn) startRequest(parent context.Context, frame Frame) {
	if frame.RequestID == 0 {
		_ = c.writeError(frame, RemoteError{
			Code:    ErrorCodeInvalidRequest,
			Message: "request_id is required",
		})
		return
	}

	h := c.server.findHandler(frame.Service, frame.Method)
	if h == nil {
		_ = c.writeError(frame, RemoteError{
			Code:    ErrorCodeNotFound,
			Message: fmt.Sprintf("method %s/%s is not registered", frame.Service, frame.Method),
		})
		return
	}

	var ctx context.Context
	var cancel context.CancelFunc
	if frame.DeadlineUnixNano > 0 {
		deadline := time.Unix(0, frame.DeadlineUnixNano)
		ctx, cancel = context.WithDeadline(parent, deadline)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}

	c.requestMu.Lock()
	c.requests[frame.RequestID] = cancel
	c.requestMu.Unlock()

	go func() {
		defer func() {
			cancel()
			c.requestMu.Lock()
			delete(c.requests, frame.RequestID)
			c.requestMu.Unlock()
		}()

		payload, err := h(ctx, frame.Payload)
		if err != nil {
			_ = c.writeError(frame, remoteErrorFromError(err))
			return
		}

		_ = c.write(Frame{
			Type:      FrameResponse,
			RequestID: frame.RequestID,
			Service:   frame.Service,
			Method:    frame.Method,
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
		Service:   request.Service,
		Method:    request.Method,
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
