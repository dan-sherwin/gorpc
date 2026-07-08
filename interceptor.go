package gorpc

// UnaryRequest is passed to unary interceptors.
type UnaryRequest struct {
	Payload []byte
}

// NotifyRequest is passed to notification interceptors.
type NotifyRequest struct {
	Payload []byte
}

// StreamRequest is passed to stream interceptors.
type StreamRequest struct {
	Kind    StreamKind
	Payload []byte
}

// UnaryHandler is the raw handler shape used by unary interceptors.
type UnaryHandler func(*Context, UnaryRequest) ([]byte, error)

// UnaryInterceptor wraps an inbound unary handler.
type UnaryInterceptor func(*Context, UnaryRequest, UnaryHandler) ([]byte, error)

// NotifyHandler is the raw handler shape used by notification interceptors.
type NotifyHandler func(*Context, NotifyRequest) error

// NotifyInterceptor wraps an inbound notification handler.
type NotifyInterceptor func(*Context, NotifyRequest, NotifyHandler) error

// StreamHandler is the raw handler shape used by stream interceptors. Client
// streaming handlers return the final response payload; server and bidi stream
// handlers return nil payloads.
type StreamHandler func(*Context, StreamRequest, *Stream) ([]byte, error)

// StreamInterceptor wraps an inbound stream handler.
type StreamInterceptor func(*Context, StreamRequest, *Stream, StreamHandler) ([]byte, error)

func invokeUnary(ctx *Context, payload []byte, h handler, interceptor UnaryInterceptor) ([]byte, error) {
	req := UnaryRequest{Payload: payload}
	next := func(nextCtx *Context, nextReq UnaryRequest) ([]byte, error) {
		return h(nextCtx, nextReq.Payload)
	}
	if interceptor == nil {
		return next(ctx, req)
	}

	return interceptor(ctx, req, next)
}

func invokeNotify(ctx *Context, payload []byte, h notifyHandler, interceptor NotifyInterceptor) error {
	req := NotifyRequest{Payload: payload}
	next := func(nextCtx *Context, nextReq NotifyRequest) error {
		return h(nextCtx, nextReq.Payload)
	}
	if interceptor == nil {
		return next(ctx, req)
	}

	return interceptor(ctx, req, next)
}

func invokeServerStream(ctx *Context, payload []byte, stream *Stream, h serverStreamHandler, interceptor StreamInterceptor) error {
	req := StreamRequest{Kind: StreamKindServer, Payload: payload}
	next := func(nextCtx *Context, nextReq StreamRequest, nextStream *Stream) ([]byte, error) {
		return nil, h(nextCtx, nextReq.Payload, nextStream)
	}
	if interceptor == nil {
		_, err := next(ctx, req, stream)
		return err
	}

	_, err := interceptor(ctx, req, stream, next)
	return err
}

func invokeClientStream(ctx *Context, stream *Stream, h clientStreamHandler, interceptor StreamInterceptor) ([]byte, error) {
	req := StreamRequest{Kind: StreamKindClient}
	next := func(nextCtx *Context, _ StreamRequest, nextStream *Stream) ([]byte, error) {
		return h(nextCtx, nextStream)
	}
	if interceptor == nil {
		return next(ctx, req, stream)
	}

	return interceptor(ctx, req, stream, next)
}

func invokeBidiStream(ctx *Context, stream *Stream, h bidiStreamHandler, interceptor StreamInterceptor) error {
	req := StreamRequest{Kind: StreamKindBidi}
	next := func(nextCtx *Context, _ StreamRequest, nextStream *Stream) ([]byte, error) {
		return nil, h(nextCtx, nextStream)
	}
	if interceptor == nil {
		_, err := next(ctx, req, stream)
		return err
	}

	_, err := interceptor(ctx, req, stream, next)
	return err
}
