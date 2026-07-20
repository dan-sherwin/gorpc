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
	Compression      Compressor
	MaxFrameSize     int64
	HandshakeTimeout time.Duration
	Auth             Auth
	WriteTimeout     time.Duration
	Logger           *slog.Logger
	OnConnect        func(*Conn)
	OnDisconnect     func(*Conn)
	PeerManager      *PeerManager

	Backpressure      BackpressureOptions
	StreamOptions     StreamOptions
	UnaryInterceptor  UnaryInterceptor
	NotifyInterceptor NotifyInterceptor
	StreamInterceptor StreamInterceptor
}

// HandlerFunc is the typed function shape used by registered unary functions.
type HandlerFunc[Req, Resp any] func(*Context, Req) (Resp, error)

// NotifyHandlerFunc is the typed function shape used by registered one-way
// notification handlers.
type NotifyHandlerFunc[Req any] func(*Context, Req) error

// ServerStreamHandlerFunc handles one request and sends zero or more response
// items before returning.
type ServerStreamHandlerFunc[Req, Item any] func(*Context, Req, *StreamWriter[Item]) error

// ClientStreamHandlerFunc receives zero or more request items and returns one
// final response.
type ClientStreamHandlerFunc[Item, Resp any] func(*Context, *StreamReader[Item]) (Resp, error)

// BidiStreamHandlerFunc receives and sends stream items until either side
// closes its sending direction.
type BidiStreamHandlerFunc[Recv, Send any] func(*Context, *BidiStreamHandle[Send, Recv]) error

// Server accepts GoRPC connections, dispatches registered functions, and exposes
// accepted connections that can initiate requests back to the dialing side.
type Server struct {
	codec            Codec
	compressor       Compressor
	maxFrameSize     int64
	handshakeTimeout time.Duration
	auth             Auth
	writeTimeout     time.Duration
	logger           *slog.Logger
	onConnect        func(*Conn)
	onDisconnect     func(*Conn)
	peerManager      *PeerManager

	backpressure  BackpressureOptions
	streamOptions StreamOptions

	unaryInterceptor  UnaryInterceptor
	notifyInterceptor NotifyInterceptor
	streamInterceptor StreamInterceptor

	handlerMu sync.RWMutex
	handlers  map[string]handler

	notifyHandlerMu sync.RWMutex
	notifyHandlers  map[string]notifyHandler

	serverStreamHandlerMu sync.RWMutex
	serverStreamHandlers  map[string]serverStreamHandler

	clientStreamHandlerMu sync.RWMutex
	clientStreamHandlers  map[string]clientStreamHandler

	bidiStreamHandlerMu sync.RWMutex
	bidiStreamHandlers  map[string]bidiStreamHandler

	listenerMu sync.Mutex
	listener   net.Listener

	connMu sync.Mutex
	conns  map[*Conn]struct{}

	shuttingDown atomic.Bool
	wg           sync.WaitGroup
}

type handler func(*Context, []byte) ([]byte, error)
type notifyHandler func(*Context, []byte) error
type serverStreamHandler func(*Context, []byte, *Stream) error
type clientStreamHandler func(*Context, *Stream) ([]byte, error)
type bidiStreamHandler func(*Context, *Stream) error

type handlerRegistrar interface {
	handlerCodec() Codec
	registerHandler(function string, h handler) error
}

type notifyHandlerRegistrar interface {
	handlerCodec() Codec
	registerNotifyHandler(function string, h notifyHandler) error
}

type streamHandlerRegistrar interface {
	handlerCodec() Codec
	registerServerStreamHandler(function string, h serverStreamHandler) error
	registerClientStreamHandler(function string, h clientStreamHandler) error
	registerBidiStreamHandler(function string, h bidiStreamHandler) error
}

// NewServer creates a Server with default codec and limits where options are unset.
func NewServer(opts ServerOptions) *Server {
	backpressure := normalizeBackpressureOptions(opts.Backpressure)

	return &Server{
		codec:                defaultCodec(opts.Codec),
		compressor:           normalizeCompressor(opts.Compression),
		maxFrameSize:         normalizeMaxFrameSize(opts.MaxFrameSize),
		handshakeTimeout:     normalizeHandshakeTimeout(opts.HandshakeTimeout),
		auth:                 opts.Auth,
		writeTimeout:         normalizeWriteTimeout(opts.WriteTimeout),
		logger:               opts.Logger,
		onConnect:            opts.OnConnect,
		onDisconnect:         opts.OnDisconnect,
		peerManager:          opts.PeerManager,
		backpressure:         backpressure,
		streamOptions:        normalizeStreamOptions(opts.StreamOptions),
		unaryInterceptor:     opts.UnaryInterceptor,
		notifyInterceptor:    opts.NotifyInterceptor,
		streamInterceptor:    opts.StreamInterceptor,
		handlers:             make(map[string]handler),
		notifyHandlers:       make(map[string]notifyHandler),
		serverStreamHandlers: make(map[string]serverStreamHandler),
		clientStreamHandlers: make(map[string]clientStreamHandler),
		bidiStreamHandlers:   make(map[string]bidiStreamHandler),
		conns:                make(map[*Conn]struct{}),
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

// RegisterNotify binds a typed one-way notification handler to a function name.
// The sender gets write success/failure only; handler errors are local to the
// receiver.
func RegisterNotify[Req any](target any, function string, fn NotifyHandlerFunc[Req]) error {
	if target == nil {
		return ErrClosed
	}
	if function == "" || fn == nil {
		return ErrInvalidFunction
	}

	registrar, ok := target.(notifyHandlerRegistrar)
	if !ok {
		return ErrInvalidHandler
	}

	codec := registrar.handlerCodec()
	if codec == nil {
		return ErrClosed
	}

	return registrar.registerNotifyHandler(function, wrapNotifyHandler(codec, fn))
}

// RegisterServerStream binds a typed server-streaming handler to a function
// name. The caller sends one request and receives zero or more response items.
func RegisterServerStream[Req, Item any](target any, function string, fn ServerStreamHandlerFunc[Req, Item]) error {
	if target == nil {
		return ErrClosed
	}
	if function == "" || fn == nil {
		return ErrInvalidFunction
	}

	registrar, ok := target.(streamHandlerRegistrar)
	if !ok {
		return ErrInvalidHandler
	}

	codec := registrar.handlerCodec()
	if codec == nil {
		return ErrClosed
	}

	return registrar.registerServerStreamHandler(function, wrapServerStreamHandler(codec, fn))
}

// RegisterClientStream binds a typed client-streaming handler to a function
// name. The caller sends zero or more request items and receives one response.
func RegisterClientStream[Item, Resp any](target any, function string, fn ClientStreamHandlerFunc[Item, Resp]) error {
	if target == nil {
		return ErrClosed
	}
	if function == "" || fn == nil {
		return ErrInvalidFunction
	}

	registrar, ok := target.(streamHandlerRegistrar)
	if !ok {
		return ErrInvalidHandler
	}

	codec := registrar.handlerCodec()
	if codec == nil {
		return ErrClosed
	}

	return registrar.registerClientStreamHandler(function, wrapClientStreamHandler(codec, fn))
}

// RegisterBidiStream binds a typed bidirectional-streaming handler to a
// function name. Both sides can send and receive stream items.
func RegisterBidiStream[Recv, Send any](target any, function string, fn BidiStreamHandlerFunc[Recv, Send]) error {
	if target == nil {
		return ErrClosed
	}
	if function == "" || fn == nil {
		return ErrInvalidFunction
	}

	registrar, ok := target.(streamHandlerRegistrar)
	if !ok {
		return ErrInvalidHandler
	}

	codec := registrar.handlerCodec()
	if codec == nil {
		return ErrClosed
	}

	return registrar.registerBidiStreamHandler(function, wrapBidiStreamHandler(codec, fn))
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

func wrapServerStreamHandler[Req, Item any](codec Codec, fn ServerStreamHandlerFunc[Req, Item]) serverStreamHandler {
	wrapped := func(ctx *Context, payload []byte, stream *Stream) error {
		var req Req
		if err := codec.Unmarshal(payload, &req); err != nil {
			return NewRemoteError(ErrorCodeInvalidRequest, fmt.Sprintf("decode stream request: %v", err), nil)
		}

		return fn(ctx, req, &StreamWriter[Item]{stream: stream})
	}

	return wrapped
}

func wrapClientStreamHandler[Item, Resp any](codec Codec, fn ClientStreamHandlerFunc[Item, Resp]) clientStreamHandler {
	wrapped := func(ctx *Context, stream *Stream) ([]byte, error) {
		resp, err := fn(ctx, &StreamReader[Item]{stream: stream})
		if err != nil {
			return nil, err
		}

		data, err := codec.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("encode stream response: %w", err)
		}

		return data, nil
	}

	return wrapped
}

func wrapBidiStreamHandler[Recv, Send any](_ Codec, fn BidiStreamHandlerFunc[Recv, Send]) bidiStreamHandler {
	wrapped := func(ctx *Context, stream *Stream) error {
		return fn(ctx, &BidiStreamHandle[Send, Recv]{stream: stream})
	}

	return wrapped
}

func wrapNotifyHandler[Req any](codec Codec, fn NotifyHandlerFunc[Req]) notifyHandler {
	wrapped := func(ctx *Context, payload []byte) error {
		var req Req
		if err := codec.Unmarshal(payload, &req); err != nil {
			return NewRemoteError(ErrorCodeInvalidRequest, fmt.Sprintf("decode notification: %v", err), nil)
		}

		return fn(ctx, req)
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

func (s *Server) registerNotifyHandler(function string, h notifyHandler) error {
	if s == nil {
		return ErrClosed
	}
	if function == "" || h == nil {
		return ErrInvalidFunction
	}

	s.notifyHandlerMu.Lock()
	defer s.notifyHandlerMu.Unlock()

	if s.notifyHandlers == nil {
		s.notifyHandlers = make(map[string]notifyHandler)
	}
	if _, exists := s.notifyHandlers[function]; exists {
		return ErrDuplicateFunction
	}
	s.notifyHandlers[function] = h

	return nil
}

func (s *Server) registerServerStreamHandler(function string, h serverStreamHandler) error {
	if s == nil {
		return ErrClosed
	}
	if function == "" || h == nil {
		return ErrInvalidFunction
	}

	s.serverStreamHandlerMu.Lock()
	defer s.serverStreamHandlerMu.Unlock()

	if s.serverStreamHandlers == nil {
		s.serverStreamHandlers = make(map[string]serverStreamHandler)
	}
	if _, exists := s.serverStreamHandlers[function]; exists {
		return ErrDuplicateFunction
	}
	s.serverStreamHandlers[function] = h

	return nil
}

func (s *Server) registerClientStreamHandler(function string, h clientStreamHandler) error {
	if s == nil {
		return ErrClosed
	}
	if function == "" || h == nil {
		return ErrInvalidFunction
	}

	s.clientStreamHandlerMu.Lock()
	defer s.clientStreamHandlerMu.Unlock()

	if s.clientStreamHandlers == nil {
		s.clientStreamHandlers = make(map[string]clientStreamHandler)
	}
	if _, exists := s.clientStreamHandlers[function]; exists {
		return ErrDuplicateFunction
	}
	s.clientStreamHandlers[function] = h

	return nil
}

func (s *Server) registerBidiStreamHandler(function string, h bidiStreamHandler) error {
	if s == nil {
		return ErrClosed
	}
	if function == "" || h == nil {
		return ErrInvalidFunction
	}

	s.bidiStreamHandlerMu.Lock()
	defer s.bidiStreamHandlerMu.Unlock()

	if s.bidiStreamHandlers == nil {
		s.bidiStreamHandlers = make(map[string]bidiStreamHandler)
	}
	if _, exists := s.bidiStreamHandlers[function]; exists {
		return ErrDuplicateFunction
	}
	s.bidiStreamHandlers[function] = h

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

// MustRegisterNotify is RegisterNotify that panics on error.
func MustRegisterNotify[Req any](target any, function string, fn NotifyHandlerFunc[Req]) {
	if err := RegisterNotify(target, function, fn); err != nil {
		panic(err)
	}
}

// MustRegisterServerStream is RegisterServerStream that panics on error.
func MustRegisterServerStream[Req, Item any](target any, function string, fn ServerStreamHandlerFunc[Req, Item]) {
	if err := RegisterServerStream(target, function, fn); err != nil {
		panic(err)
	}
}

// MustRegisterClientStream is RegisterClientStream that panics on error.
func MustRegisterClientStream[Item, Resp any](target any, function string, fn ClientStreamHandlerFunc[Item, Resp]) {
	if err := RegisterClientStream(target, function, fn); err != nil {
		panic(err)
	}
}

// MustRegisterBidiStream is RegisterBidiStream that panics on error.
func MustRegisterBidiStream[Recv, Send any](target any, function string, fn BidiStreamHandlerFunc[Recv, Send]) {
	if err := RegisterBidiStream(target, function, fn); err != nil {
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
	clientName, compressor, err := s.serverHandshake(conn, func(clientName string, compressor Compressor) error {
		sc.clientName = clientName
		sc.compressor = compressor
		if s.peerManager != nil {
			return s.peerManager.acceptInbound(sc)
		}
		return nil
	})
	if err != nil {
		s.logDebug("gorpc handshake failed", "error", err)
		if s.peerManager != nil {
			s.peerManager.disconnected(sc)
		}
		_ = sc.close()
		return
	}
	if s.peerManager != nil {
		if err := s.peerManager.connected(sc); err != nil {
			s.logDebug("gorpc peer activation failed", "error", err)
			s.peerManager.disconnected(sc)
			_ = sc.close()
			return
		}
	}
	sc.clientName = clientName
	sc.compressor = compressor
	s.addConn(sc)

	connectionDone := make(chan struct{})
	lifecycleDone := make(chan struct{})
	go func() {
		s.safeOnConnect(sc)
		<-connectionDone
		s.safeOnDisconnect(sc)
		close(lifecycleDone)
	}()
	defer func() {
		close(connectionDone)
		<-lifecycleDone
		if s.peerManager != nil {
			s.peerManager.disconnected(sc)
		}
		s.removeConn(sc)
		_ = sc.close()
	}()

	for {
		frame, err := readFrameWithCompression(conn, s.maxFrameSize, s.codec, sc.compressor)
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
		case FrameNotify:
			sc.startNotify(frame)
		case FrameStreamStart:
			sc.startStream(frame)
		case FrameStreamItem, FrameStreamEnd:
			if !sc.deliverStreamFrame(frame) {
				s.logDebug("gorpc discarded stream frame for unknown request", "type", frame.Type.String(), "request_id", frame.RequestID)
			}
		case FrameResponse:
			if !sc.complete(frame.RequestID, clientResponse{frame: frame}) {
				s.logDebug("gorpc discarded response for unknown request", "request_id", frame.RequestID)
			}
		case FrameError:
			if !sc.complete(frame.RequestID, clientResponse{frame: frame}) && !sc.deliverStreamFrame(frame) {
				s.logDebug("gorpc discarded error for unknown request", "request_id", frame.RequestID)
			}
		case FrameCancel:
			sc.cancel(frame.RequestID)
		case FramePing:
			if err := sc.write(Frame{Type: FramePong, RequestID: frame.RequestID}); err != nil {
				sc.handleWriteError(err)
				if !errors.Is(err, ErrBackpressure) {
					return
				}
			}
		default:
			s.logDebug("gorpc ignored frame", "type", frame.Type.String(), "request_id", frame.RequestID)
		}
	}
}

func (s *Server) serverHandshake(conn net.Conn, accept func(string, Compressor) error) (string, Compressor, error) {
	if s.handshakeTimeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(s.handshakeTimeout))
		defer func() {
			_ = conn.SetDeadline(time.Time{})
		}()
	}

	frame, err := readFrame(conn, s.maxFrameSize, s.codec)
	if err != nil {
		return "", nil, err
	}
	if frame.Type != FrameHello {
		return "", nil, fmt.Errorf("%w: expected hello, got %s", ErrProtocol, frame.Type.String())
	}

	var hello hello
	if err := s.codec.Unmarshal(frame.Payload, &hello); err != nil {
		return "", nil, fmt.Errorf("decode hello: %w", err)
	}
	if hello.ProtocolVersion != ProtocolVersion {
		return "", nil, fmt.Errorf("%w: unsupported version %d", ErrProtocol, hello.ProtocolVersion)
	}
	if hello.Codec != s.codec.Name() {
		return "", nil, fmt.Errorf("%w: unsupported codec %q", ErrProtocol, hello.Codec)
	}
	if err := ensureCompressor(hello.Compression, s.compressor); err != nil {
		return "", nil, err
	}
	var compressor Compressor
	if hello.Compression != "" {
		compressor = s.compressor
	}

	ack := helloAck{
		ProtocolVersion: ProtocolVersion,
		Codec:           s.codec.Name(),
		Compression:     hello.Compression,
	}
	var authChallenge []byte
	if s.auth.enabledAuth() {
		authChallenge, err = s.auth.challenge()
		if err != nil {
			return "", nil, err
		}
		ack.AuthRequired = true
		ack.AuthMethod = s.auth.method()
		ack.AuthChallenge = authChallenge
	}

	if s.auth.enabledAuth() {
		if err := s.writeHelloAck(conn, ack); err != nil {
			return "", nil, err
		}
		if err := s.readAuth(conn, hello, authChallenge); err != nil {
			return "", nil, err
		}
		if accept != nil {
			if err := accept(hello.ClientName, compressor); err != nil {
				_ = s.writePeerHandshakeError(conn, err)
				return "", nil, err
			}
		}
		if err := s.writeAuthAck(conn); err != nil {
			return "", nil, err
		}
		return hello.ClientName, compressor, nil
	}
	if accept != nil {
		if err := accept(hello.ClientName, compressor); err != nil {
			_ = s.writePeerHandshakeError(conn, err)
			return "", nil, err
		}
	}
	if err := s.writeHelloAck(conn, ack); err != nil {
		return "", nil, err
	}

	return hello.ClientName, compressor, nil
}

func (s *Server) writeHelloAck(conn net.Conn, ack helloAck) error {
	payload, err := s.codec.Marshal(ack)
	if err != nil {
		return fmt.Errorf("encode hello ack: %w", err)
	}
	return writeFrame(conn, s.maxFrameSize, s.codec, Frame{
		Type:    FrameHelloAck,
		Payload: payload,
	})
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

	return nil
}

func (s *Server) writeAuthAck(conn net.Conn) error {
	payload, err := s.codec.Marshal(authAck{OK: true})
	if err != nil {
		return fmt.Errorf("encode auth ack: %w", err)
	}

	return writeFrame(conn, s.maxFrameSize, s.codec, Frame{
		Type:    FrameAuthAck,
		Payload: payload,
	})
}

func (s *Server) writePeerHandshakeError(conn net.Conn, err error) error {
	code := ErrorCodeUnavailable
	switch {
	case errors.Is(err, ErrPeerConnected):
		code = ErrorCodePeerConnected
	case errors.Is(err, ErrPeerIdentityRequired), errors.Is(err, ErrPeerSelfConnection):
		code = ErrorCodeUnauthorized
	}
	return s.writeHandshakeError(conn, RemoteError{Code: code, Message: err.Error()})
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

func (s *Server) findNotifyHandler(function string) notifyHandler {
	s.notifyHandlerMu.RLock()
	defer s.notifyHandlerMu.RUnlock()

	return s.notifyHandlers[function]
}

func (s *Server) findServerStreamHandler(function string) serverStreamHandler {
	s.serverStreamHandlerMu.RLock()
	defer s.serverStreamHandlerMu.RUnlock()

	return s.serverStreamHandlers[function]
}

func (s *Server) findClientStreamHandler(function string) clientStreamHandler {
	s.clientStreamHandlerMu.RLock()
	defer s.clientStreamHandlerMu.RUnlock()

	return s.clientStreamHandlers[function]
}

func (s *Server) findBidiStreamHandler(function string) bidiStreamHandler {
	s.bidiStreamHandlerMu.RLock()
	defer s.bidiStreamHandlerMu.RUnlock()

	return s.bidiStreamHandlers[function]
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
	compressor Compressor

	nextID       atomic.Uint64
	writeMu      sync.Mutex
	writeLimiter *writeLimiter

	pendingMu sync.Mutex
	pending   map[uint64]pendingCall

	requestMu sync.Mutex
	requests  map[uint64]context.CancelFunc

	streamMu sync.Mutex
	streams  map[uint64]*Stream

	closeOnce  sync.Once
	closed     chan struct{}
	closeErrMu sync.Mutex
	closeErr   error

	singleflight singleflightGroup
}

func newConn(server *Server, conn net.Conn) *Conn {
	return &Conn{
		server:       server,
		conn:         conn,
		writeLimiter: newWriteLimiter(server.backpressure.MaxConcurrentWrites),
		pending:      make(map[uint64]pendingCall),
		requests:     make(map[uint64]context.CancelFunc),
		streams:      make(map[uint64]*Stream),
		closed:       make(chan struct{}),
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
	if err := validateResponseTarget(resp); err != nil {
		return err
	}

	frame, err := c.callFrameContext(ctx, function, req)
	if err != nil {
		return err
	}

	return decodeResponse(c.server.codec, frame, resp)
}

// CallSingleflight performs a unary request/response call to the connected
// client and collapses concurrent calls with the same function and key into one
// remote request.
func (c *Conn) CallSingleflight(function string, key string, req any, resp any) error {
	return c.CallSingleflightContext(context.Background(), function, key, req, resp)
}

// CallSingleflightWithTimeout performs a singleflight call with a timeout.
func (c *Conn) CallSingleflightWithTimeout(function string, key string, req any, resp any, timeout time.Duration) error {
	if timeout <= 0 {
		return c.CallSingleflight(function, key, req, resp)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return c.CallSingleflightContext(ctx, function, key, req, resp)
}

// CallSingleflightContext performs a unary call to the connected client and
// shares one in-flight remote request with concurrent callers using the same
// function and key. If key is empty, GoRPC builds a key from the encoded request
// payload.
func (c *Conn) CallSingleflightContext(ctx context.Context, function string, key string, req any, resp any) error {
	if c == nil || c.server == nil {
		return ErrClosed
	}
	if function == "" {
		return ErrInvalidFunction
	}
	if err := validateResponseTarget(resp); err != nil {
		return err
	}

	sfKey, err := singleflightKey(c.server.codec, function, key, req)
	if err != nil {
		return fmt.Errorf("encode singleflight key: %w", err)
	}

	frame, err := c.singleflight.do(ctx, sfKey, func() (Frame, error) {
		return c.callFrameContext(ctx, function, req)
	})
	if err != nil {
		return err
	}

	return decodeResponse(c.server.codec, frame, resp)
}

func (c *Conn) callFrameContext(ctx context.Context, function string, req any) (Frame, error) {
	if c == nil || c.server == nil {
		return Frame{}, ErrClosed
	}
	ctx = normalizeContext(ctx)

	responseCh := make(chan clientResponse, 1)
	requestID, err := c.sendRequest(ctx, function, req, syncPendingCall{ch: responseCh})
	if err != nil {
		return Frame{}, err
	}

	select {
	case response := <-responseCh:
		if response.err != nil {
			return Frame{}, response.err
		}

		return response.frame, nil
	case <-ctx.Done():
		c.removePending(requestID)
		if err := c.write(Frame{Type: FrameCancel, RequestID: requestID}); err != nil {
			c.handleWriteError(err)
		}
		return Frame{}, ctx.Err()
	case <-c.closed:
		c.removePending(requestID)
		return Frame{}, c.closedError()
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

// Notify sends a one-way typed notification to the connected client.
func (c *Conn) Notify(function string, req any) error {
	return c.NotifyContext(context.Background(), function, req)
}

// NotifyWithTimeout sends a one-way typed notification to the connected client
// with a timeout while writing the notification frame.
func (c *Conn) NotifyWithTimeout(function string, req any, timeout time.Duration) error {
	if timeout <= 0 {
		return c.Notify(function, req)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return c.NotifyContext(ctx, function, req)
}

// NotifyContext sends a one-way typed notification to the connected client.
// Success means the frame was written locally; GoRPC does not wait for remote
// handler completion or remote errors.
func (c *Conn) NotifyContext(ctx context.Context, function string, req any) error {
	_, err := c.sendNotify(ctx, function, req)
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

	requestID := c.nextRequestID()
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
		c.handleWriteError(err)
		return 0, err
	}

	return requestID, nil
}

func (c *Conn) sendNotify(ctx context.Context, function string, req any) (uint64, error) {
	ctx = normalizeContext(ctx)
	if c == nil || c.server == nil {
		return 0, ErrClosed
	}
	if function == "" {
		return 0, ErrInvalidFunction
	}

	payload, err := c.server.codec.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("encode notification: %w", err)
	}

	requestID := c.nextRequestID()
	frame := Frame{
		Type:      FrameNotify,
		RequestID: requestID,
		Function:  function,
		Payload:   payload,
	}
	if deadline, ok := ctx.Deadline(); ok {
		frame.DeadlineUnixNano = deadline.UnixNano()
	}

	if err := c.write(frame); err != nil {
		c.handleWriteError(err)
		return 0, err
	}

	return requestID, nil
}

func (c *Conn) openServerStream(ctx context.Context, function string, req any, opts StreamOptions) (*Stream, error) {
	ctx = normalizeContext(ctx)
	if c == nil || c.server == nil {
		return nil, ErrClosed
	}
	if function == "" {
		return nil, ErrInvalidFunction
	}

	payload, err := c.server.codec.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode stream request: %w", err)
	}

	requestID := c.nextRequestID()
	streamOpts := mergeStreamOptions(c.server.streamOptions, opts)
	stream := newStreamWithOptions(ctx, requestID, function, c.server.codec, func(frame Frame) error {
		if err := c.write(frame); err != nil {
			c.handleWriteError(err)
			return err
		}
		return nil
	}, c.removeStream, streamOpts)
	if err := c.addStream(stream); err != nil {
		return nil, err
	}

	frame := Frame{
		Type:       FrameStreamStart,
		StreamKind: StreamKindServer,
		RequestID:  requestID,
		Function:   function,
		Payload:    payload,
	}
	if deadline, ok := ctx.Deadline(); ok {
		frame.DeadlineUnixNano = deadline.UnixNano()
	}

	if err := c.write(frame); err != nil {
		c.removeStream(requestID)
		stream.deliverError(fmt.Errorf("%w: %v", ErrUnavailable, err))
		c.handleWriteError(err)
		return nil, err
	}

	return stream, nil
}

func (c *Conn) openClientStream(ctx context.Context, function string, opts StreamOptions) (*Stream, chan clientResponse, func(uint64), Codec, error) {
	ctx = normalizeContext(ctx)
	if c == nil || c.server == nil {
		return nil, nil, nil, nil, ErrClosed
	}
	if function == "" {
		return nil, nil, nil, nil, ErrInvalidFunction
	}

	requestID := c.nextRequestID()
	responseCh := make(chan clientResponse, 1)
	if err := c.addPending(requestID, syncPendingCall{ch: responseCh}); err != nil {
		return nil, nil, nil, nil, err
	}

	streamOpts := mergeStreamOptions(c.server.streamOptions, opts)
	stream := newStreamWithOptions(ctx, requestID, function, c.server.codec, func(frame Frame) error {
		if err := c.write(frame); err != nil {
			c.handleWriteError(err)
			return err
		}
		return nil
	}, c.removeStream, streamOpts)
	stream.receivesItems = false
	if err := c.addStream(stream); err != nil {
		c.removePending(requestID)
		return nil, nil, nil, nil, err
	}
	frame := Frame{
		Type:       FrameStreamStart,
		StreamKind: StreamKindClient,
		RequestID:  requestID,
		Function:   function,
	}
	if deadline, ok := ctx.Deadline(); ok {
		frame.DeadlineUnixNano = deadline.UnixNano()
	}

	if err := c.write(frame); err != nil {
		c.removePending(requestID)
		c.removeStream(requestID)
		stream.finish(fmt.Errorf("%w: %v", ErrUnavailable, err))
		c.handleWriteError(err)
		return nil, nil, nil, nil, err
	}

	return stream, responseCh, c.removePending, c.server.codec, nil
}

func (c *Conn) openBidiStream(ctx context.Context, function string, opts StreamOptions) (*Stream, error) {
	ctx = normalizeContext(ctx)
	if c == nil || c.server == nil {
		return nil, ErrClosed
	}
	if function == "" {
		return nil, ErrInvalidFunction
	}

	requestID := c.nextRequestID()
	streamOpts := mergeStreamOptions(c.server.streamOptions, opts)
	stream := newStreamWithOptions(ctx, requestID, function, c.server.codec, func(frame Frame) error {
		if err := c.write(frame); err != nil {
			c.handleWriteError(err)
			return err
		}
		return nil
	}, c.removeStream, streamOpts)
	if err := c.addStream(stream); err != nil {
		return nil, err
	}

	frame := Frame{
		Type:       FrameStreamStart,
		StreamKind: StreamKindBidi,
		RequestID:  requestID,
		Function:   function,
	}
	if deadline, ok := ctx.Deadline(); ok {
		frame.DeadlineUnixNano = deadline.UnixNano()
	}

	if err := c.write(frame); err != nil {
		c.removeStream(requestID)
		stream.deliverError(fmt.Errorf("%w: %v", ErrUnavailable, err))
		c.handleWriteError(err)
		return nil, err
	}

	return stream, nil
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

		payload, err := invokeUnary(rpcCtx, frame.Payload, h, c.server.unaryInterceptor)
		if err != nil {
			_ = c.writeError(frame, remoteErrorFromError(err))
			return
		}

		if err := c.write(Frame{
			Type:      FrameResponse,
			RequestID: frame.RequestID,
			Function:  frame.Function,
			Payload:   payload,
		}); err != nil {
			c.handleWriteError(err)
		}
	}()
}

func (c *Conn) startNotify(frame Frame) {
	if frame.RequestID == 0 {
		c.server.logDebug("gorpc discarded notification without request_id", "function", frame.Function)
		return
	}
	if frame.Function == "" {
		c.server.logDebug("gorpc discarded notification without function", "request_id", frame.RequestID)
		return
	}

	h := c.server.findNotifyHandler(frame.Function)
	if h == nil {
		c.server.logDebug("gorpc discarded notification for unregistered function", "function", frame.Function, "request_id", frame.RequestID)
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
		notify:     true,
	}

	c.requestMu.Lock()
	c.requests[frame.RequestID] = cancel
	c.requestMu.Unlock()

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				c.server.logDebug("gorpc notify handler panic", "function", frame.Function, "request_id", frame.RequestID, "panic", recovered)
			}
			cancel()
			c.requestMu.Lock()
			delete(c.requests, frame.RequestID)
			c.requestMu.Unlock()
		}()

		if err := invokeNotify(rpcCtx, frame.Payload, h, c.server.notifyInterceptor); err != nil {
			c.server.logDebug("gorpc notify handler failed", "function", frame.Function, "request_id", frame.RequestID, "error", err)
		}
	}()
}

func (c *Conn) startStream(frame Frame) {
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

	switch frame.StreamKind {
	case StreamKindServer:
		c.startServerStream(frame)
	case StreamKindClient:
		c.startClientStream(frame)
	case StreamKindBidi:
		c.startBidiStream(frame)
	default:
		_ = c.writeError(frame, RemoteError{
			Code:    ErrorCodeInvalidRequest,
			Message: fmt.Sprintf("unsupported stream kind %q", frame.StreamKind.String()),
		})
	}
}

func (c *Conn) startServerStream(frame Frame) {
	h := c.server.findServerStreamHandler(frame.Function)
	if h == nil {
		_ = c.writeError(frame, RemoteError{
			Code:    ErrorCodeNotFound,
			Message: fmt.Sprintf("stream function %q is not registered", frame.Function),
		})
		return
	}

	ctx, cancel := contextFromFrame(frame)
	rpcCtx := &Context{
		Context:    ctx,
		clientName: c.clientName,
		requestID:  frame.RequestID,
		function:   frame.Function,
		remoteAddr: c.conn.RemoteAddr(),
		localAddr:  c.conn.LocalAddr(),
		conn:       c,
		stream:     true,
		streamKind: StreamKindServer,
	}
	stream := newStreamWithOptions(ctx, frame.RequestID, frame.Function, c.server.codec, func(writeFrame Frame) error {
		if err := c.write(writeFrame); err != nil {
			c.handleWriteError(err)
			return err
		}
		return nil
	}, c.removeStream, c.server.streamOptions)
	stream.receivesItems = false
	if err := c.addStream(stream); err != nil {
		_ = c.writeError(frame, remoteErrorFromError(err))
		cancel()
		return
	}

	c.requestMu.Lock()
	c.requests[frame.RequestID] = cancel
	c.requestMu.Unlock()

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				_ = c.writeError(frame, RemoteError{
					Code:    ErrorCodeInternal,
					Message: fmt.Sprintf("stream handler panic: %v", recovered),
				})
			}
			c.removeStream(frame.RequestID)
			stream.finish(ErrClosed)
			cancel()
			c.requestMu.Lock()
			delete(c.requests, frame.RequestID)
			c.requestMu.Unlock()
		}()

		if err := invokeServerStream(rpcCtx, frame.Payload, stream, h, c.server.streamInterceptor); err != nil {
			_ = c.writeError(frame, remoteErrorFromError(err))
			stream.finish(err)
			return
		}

		_ = stream.CloseSend()
	}()
}

func (c *Conn) startClientStream(frame Frame) {
	h := c.server.findClientStreamHandler(frame.Function)
	if h == nil {
		_ = c.writeError(frame, RemoteError{
			Code:    ErrorCodeNotFound,
			Message: fmt.Sprintf("stream function %q is not registered", frame.Function),
		})
		return
	}

	ctx, cancel := contextFromFrame(frame)
	rpcCtx := &Context{
		Context:    ctx,
		clientName: c.clientName,
		requestID:  frame.RequestID,
		function:   frame.Function,
		remoteAddr: c.conn.RemoteAddr(),
		localAddr:  c.conn.LocalAddr(),
		conn:       c,
		stream:     true,
		streamKind: StreamKindClient,
	}
	stream := newStreamWithOptions(ctx, frame.RequestID, frame.Function, c.server.codec, func(writeFrame Frame) error {
		if err := c.write(writeFrame); err != nil {
			c.handleWriteError(err)
			return err
		}
		return nil
	}, c.removeStream, c.server.streamOptions)
	if err := c.addStream(stream); err != nil {
		_ = c.writeError(frame, remoteErrorFromError(err))
		cancel()
		return
	}

	c.requestMu.Lock()
	c.requests[frame.RequestID] = cancel
	c.requestMu.Unlock()

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				_ = c.writeError(frame, RemoteError{
					Code:    ErrorCodeInternal,
					Message: fmt.Sprintf("stream handler panic: %v", recovered),
				})
			}
			c.removeStream(frame.RequestID)
			stream.finish(ErrClosed)
			cancel()
			c.requestMu.Lock()
			delete(c.requests, frame.RequestID)
			c.requestMu.Unlock()
		}()

		payload, err := invokeClientStream(rpcCtx, stream, h, c.server.streamInterceptor)
		if err != nil {
			_ = c.writeError(frame, remoteErrorFromError(err))
			return
		}

		if err := c.write(Frame{
			Type:      FrameResponse,
			RequestID: frame.RequestID,
			Function:  frame.Function,
			Payload:   payload,
		}); err != nil {
			c.handleWriteError(err)
		}
	}()
}

func (c *Conn) startBidiStream(frame Frame) {
	h := c.server.findBidiStreamHandler(frame.Function)
	if h == nil {
		_ = c.writeError(frame, RemoteError{
			Code:    ErrorCodeNotFound,
			Message: fmt.Sprintf("stream function %q is not registered", frame.Function),
		})
		return
	}

	ctx, cancel := contextFromFrame(frame)
	rpcCtx := &Context{
		Context:    ctx,
		clientName: c.clientName,
		requestID:  frame.RequestID,
		function:   frame.Function,
		remoteAddr: c.conn.RemoteAddr(),
		localAddr:  c.conn.LocalAddr(),
		conn:       c,
		stream:     true,
		streamKind: StreamKindBidi,
	}
	stream := newStreamWithOptions(ctx, frame.RequestID, frame.Function, c.server.codec, func(writeFrame Frame) error {
		if err := c.write(writeFrame); err != nil {
			c.handleWriteError(err)
			return err
		}
		return nil
	}, c.removeStream, c.server.streamOptions)
	if err := c.addStream(stream); err != nil {
		_ = c.writeError(frame, remoteErrorFromError(err))
		cancel()
		return
	}

	c.requestMu.Lock()
	c.requests[frame.RequestID] = cancel
	c.requestMu.Unlock()

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				_ = c.writeError(frame, RemoteError{
					Code:    ErrorCodeInternal,
					Message: fmt.Sprintf("stream handler panic: %v", recovered),
				})
			}
			c.removeStream(frame.RequestID)
			stream.finish(ErrClosed)
			cancel()
			c.requestMu.Lock()
			delete(c.requests, frame.RequestID)
			c.requestMu.Unlock()
		}()

		if err := invokeBidiStream(rpcCtx, stream, h, c.server.streamInterceptor); err != nil {
			_ = c.writeError(frame, remoteErrorFromError(err))
			return
		}

		_ = stream.CloseSend()
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

	err = c.write(Frame{
		Type:      FrameError,
		RequestID: request.RequestID,
		Function:  request.Function,
		Payload:   payload,
	})
	if err != nil {
		c.handleWriteError(err)
	}

	return err
}

func (c *Conn) write(frame Frame) error {
	select {
	case <-c.closed:
		return c.closedError()
	default:
	}
	if !c.writeLimiter.acquire() {
		c.reportBackpressure(BackpressureInfo{
			Side:      BackpressureSideServer,
			Reason:    BackpressureReasonConcurrentWrites,
			Limit:     c.server.backpressure.MaxConcurrentWrites,
			RequestID: frame.RequestID,
			Function:  frame.Function,
			FrameType: frame.Type,
		})
		return ErrBackpressure
	}
	defer c.writeLimiter.release()

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

	return writeFrameWithCompression(c.conn, c.server.maxFrameSize, c.server.codec, c.compressor, frame)
}

func (c *Conn) handleWriteError(err error) {
	if err == nil || errors.Is(err, ErrBackpressure) {
		return
	}

	c.closeWithError(err)
}

func (c *Conn) nextRequestID() uint64 {
	return c.nextID.Add(2)
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
		if c.server.backpressure.MaxPendingCalls > 0 && len(c.pending) >= c.server.backpressure.MaxPendingCalls {
			c.reportBackpressure(BackpressureInfo{
				Side:      BackpressureSideServer,
				Reason:    BackpressureReasonPendingCalls,
				Limit:     c.server.backpressure.MaxPendingCalls,
				RequestID: requestID,
			})
			return ErrBackpressure
		}
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

func (c *Conn) addStream(stream *Stream) error {
	if stream == nil {
		return ErrClosed
	}

	select {
	case <-c.closed:
		return c.closedError()
	default:
	}

	c.streamMu.Lock()
	defer c.streamMu.Unlock()

	select {
	case <-c.closed:
		return c.closedError()
	default:
		if c.server.backpressure.MaxActiveStreams > 0 && len(c.streams) >= c.server.backpressure.MaxActiveStreams {
			c.reportBackpressure(BackpressureInfo{
				Side:      BackpressureSideServer,
				Reason:    BackpressureReasonActiveStreams,
				Limit:     c.server.backpressure.MaxActiveStreams,
				RequestID: stream.RequestID(),
				Function:  stream.Function(),
			})
			return ErrBackpressure
		}
		c.streams[stream.RequestID()] = stream
		return nil
	}
}

func (c *Conn) removeStream(requestID uint64) {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()

	delete(c.streams, requestID)
}

func (c *Conn) findStream(requestID uint64) *Stream {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()

	return c.streams[requestID]
}

func (c *Conn) deliverStreamFrame(frame Frame) bool {
	stream := c.findStream(frame.RequestID)
	if stream == nil {
		return false
	}

	switch frame.Type {
	case FrameStreamItem:
		if !stream.receivesItems {
			return false
		}
		stream.deliverItem(frame)
	case FrameStreamEnd:
		if !stream.receivesItems {
			return false
		}
		stream.deliverEnd()
	case FrameError:
		stream.deliverError(remoteErrorFromFrame(c.server.codec, frame))
	default:
		return false
	}

	return true
}

func (c *Conn) failStreams(err error) {
	if err == nil {
		err = ErrUnavailable
	}

	c.streamMu.Lock()
	streams := make([]*Stream, 0, len(c.streams))
	for requestID, stream := range c.streams {
		delete(c.streams, requestID)
		streams = append(streams, stream)
	}
	c.streamMu.Unlock()

	for _, stream := range streams {
		stream.deliverError(err)
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

		_ = c.conn.Close()
		c.failStreams(err)
		c.requestMu.Lock()
		for _, cancel := range c.requests {
			cancel()
		}
		c.requests = make(map[uint64]context.CancelFunc)
		c.requestMu.Unlock()
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

func (c *Conn) reportBackpressure(info BackpressureInfo) {
	if c == nil || c.server == nil {
		return
	}

	reportBackpressure(c.server.backpressure, info)
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
