package gorpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

const streamRecvBuffer = 16

// StreamOptions configures newly opened streams. Zero values keep GoRPC defaults.
type StreamOptions struct {
	// RecvBuffer limits queued items. The default is 16.
	RecvBuffer int
	// RecvBytes limits queued, uncompressed item payloads. The default is 64 MiB.
	RecvBytes int64
}

func normalizeStreamOptions(opts StreamOptions) StreamOptions {
	if opts.RecvBuffer <= 0 {
		opts.RecvBuffer = streamRecvBuffer
	}
	if opts.RecvBytes <= 0 {
		opts.RecvBytes = DefaultMaxFrameSize
	}
	return opts
}

func mergeStreamOptions(base, override StreamOptions) StreamOptions {
	if override.RecvBuffer > 0 {
		base.RecvBuffer = override.RecvBuffer
	}
	if override.RecvBytes > 0 {
		base.RecvBytes = override.RecvBytes
	}
	return normalizeStreamOptions(base)
}

// Stream is the raw bidirectional item stream used by the typed streaming
// helpers. Most callers should prefer ServerStream, ClientStream, BidiStream,
// and the typed handler registration functions.
type Stream struct {
	requestID uint64
	function  string
	codec     Codec
	write     func(Frame) error
	onDone    func(*Stream)

	ctx    context.Context
	cancel context.CancelCauseFunc

	recvCh        chan []byte
	recvDone      chan struct{}
	sendDone      chan struct{}
	receivesItems bool
	sendsItems    bool
	recvByteLimit uint64

	// sendMu orders items before the send-side end frame. It is never held by
	// the connection reader, which must remain free to deliver credit.
	sendMu sync.Mutex
	mu     sync.Mutex
	flow   *streamFlow

	bufferedBytes uint64
	sendClosed    bool
	sendEnded     bool
	recvClosed    bool
	done          bool
	recvErr       error
	closeSendErr  error

	closeSendOnce sync.Once
}

func newStreamWithOptions(ctx context.Context, requestID uint64, function string, codec Codec, write func(Frame) error, onDone func(*Stream), opts StreamOptions) *Stream {
	streamCtx, cancel := context.WithCancelCause(normalizeContext(ctx))
	opts = normalizeStreamOptions(opts)
	return &Stream{
		requestID:     requestID,
		function:      function,
		codec:         defaultCodec(codec),
		write:         write,
		onDone:        onDone,
		ctx:           streamCtx,
		cancel:        cancel,
		recvCh:        make(chan []byte, opts.RecvBuffer),
		recvDone:      make(chan struct{}),
		sendDone:      make(chan struct{}),
		receivesItems: true,
		sendsItems:    true,
		recvByteLimit: uint64(opts.RecvBytes),
	}
}

// configure runs before the stream is published to the connection reader.
func (s *Stream) configure(kind StreamKind, caller, flowControl bool) {
	switch kind {
	case StreamKindServer:
		s.receivesItems = caller
		s.sendsItems = !caller
		if caller {
			s.sendClosed = true
			s.sendEnded = true
			close(s.sendDone)
		} else {
			s.recvClosed = true
			s.recvErr = io.EOF
			close(s.recvDone)
		}
	case StreamKindClient:
		s.receivesItems = !caller
		s.sendsItems = caller
		// The response keeps this stream alive after its item side closes.
	}
	if flowControl {
		s.flow = &streamFlow{changed: make(chan struct{}, 1)}
	}
}

// watchContext is installed after a stream start is sent or accepted, so a
// cancel frame cannot overtake the start frame.
func (s *Stream) watchContext() {
	context.AfterFunc(s.ctx, func() {
		err := context.Cause(s.ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			// A bare cancel can reach the peer before its own deadline fires,
			// replacing the timeout with context.Canceled.
			s.abort(err)
			return
		}
		if s.finish(err) {
			_ = s.write(Frame{Type: FrameCancel, RequestID: s.requestID, Function: s.function})
		}
	})
}

// RequestID returns the stream request ID.
func (s *Stream) RequestID() uint64 {
	if s == nil {
		return 0
	}
	return s.requestID
}

// Function returns the remote function name for the stream.
func (s *Stream) Function() string {
	if s == nil {
		return ""
	}
	return s.function
}

// Context returns the stream context. It is canceled when the stream ends,
// the connection closes, or either peer cancels the stream.
func (s *Stream) Context() context.Context {
	if s == nil || s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

// Send encodes and writes one item. With negotiated flow control, it waits for
// receiver credit or cancellation. Calls to Send are serialized.
func (s *Stream) Send(item any) error {
	if s == nil || !s.sendsItems {
		return ErrClosed
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	if err := s.sendError(); err != nil {
		return err
	}
	payload, err := s.codec.Marshal(item)
	if err != nil {
		return fmt.Errorf("encode stream item: %w", err)
	}
	if err := s.takeCredit(uint64(len(payload))); err != nil {
		return err
	}
	if err := s.write(Frame{
		Type: FrameStreamItem, RequestID: s.requestID, Function: s.function, Payload: payload,
	}); err != nil {
		s.abort(err)
		return err
	}
	return nil
}

func (s *Stream) sendError() error {
	if err := context.Cause(s.ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendClosed {
		return ErrClosed
	}
	return nil
}

// Recv reads one stream item into item. It returns io.EOF after the remote side
// closes its send side and all buffered items have been consumed.
func (s *Stream) Recv(item any) error {
	if s == nil || !s.receivesItems {
		return ErrClosed
	}
	if item == nil {
		return fmt.Errorf("%w: stream item must be a non-nil pointer", ErrInvalidResponse)
	}
	if err := validateResponseTarget(item); err != nil {
		return err
	}
	// Drain items before reporting a terminal error or EOF.
	select {
	case payload := <-s.recvCh:
		return s.receivePayload(payload, item)
	default:
	}
	select {
	case payload := <-s.recvCh:
		return s.receivePayload(payload, item)
	case <-s.recvDone:
	case <-s.ctx.Done():
	}
	select {
	case payload := <-s.recvCh:
		return s.receivePayload(payload, item)
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recvErr != nil {
		return s.recvErr
	}
	return context.Cause(s.ctx)
}

func (s *Stream) receivePayload(payload []byte, item any) error {
	size := uint64(len(payload))
	s.mu.Lock()
	s.bufferedBytes -= size
	s.mu.Unlock()

	err := s.codec.Unmarshal(payload, item)
	if writeErr := s.returnCredit(size); writeErr != nil {
		s.abort(writeErr)
	}
	if err != nil {
		return fmt.Errorf("decode stream item: %w", err)
	}
	return nil
}

// CloseSend closes the local sending side. Receiving can continue. A blocked
// Send is interrupted; an item already being written precedes the end frame.
func (s *Stream) CloseSend() error {
	if s == nil || !s.sendsItems {
		return ErrClosed
	}
	s.closeSendOnce.Do(func() {
		s.mu.Lock()
		s.sendClosed = true
		close(s.sendDone)
		s.mu.Unlock()

		s.sendMu.Lock()
		defer s.sendMu.Unlock()
		err := context.Cause(s.ctx)
		if err == nil {
			err = s.write(Frame{Type: FrameStreamEnd, RequestID: s.requestID, Function: s.function})
		}
		s.mu.Lock()
		s.closeSendErr = err
		s.sendEnded = err == nil
		finished := s.recvClosed
		s.mu.Unlock()
		if err != nil {
			s.abort(err)
		} else if finished {
			s.finish(ErrClosed)
		}
	})
	return s.closeSendErr
}

// Cancel cancels the whole stream locally before sending a best-effort cancel
// frame to the remote side.
func (s *Stream) Cancel() error {
	if s == nil {
		return ErrClosed
	}
	if !s.finish(context.Canceled) {
		return nil
	}
	return s.write(Frame{Type: FrameCancel, RequestID: s.requestID, Function: s.function})
}

// abort must not write on the connection reader's goroutine.
func (s *Stream) abort(err error) {
	if !s.finish(err) {
		return
	}
	go func() {
		payload, marshalErr := s.codec.Marshal(remoteErrorFromError(err))
		if marshalErr == nil {
			_ = s.write(Frame{Type: FrameError, RequestID: s.requestID, Function: s.function, Payload: payload})
		}
	}()
}

func (s *Stream) deliverItem(frame Frame) {
	s.mu.Lock()
	if s.recvClosed || s.done {
		s.mu.Unlock()
		return
	}
	size := uint64(len(frame.Payload))
	if size <= s.recvByteLimit-s.bufferedBytes {
		select {
		case s.recvCh <- frame.Payload:
			s.bufferedBytes += size
			s.mu.Unlock()
			return
		default:
		}
	}
	s.mu.Unlock()
	s.abort(fmt.Errorf("%w: stream receive buffer exhausted", ErrBackpressure))
}

func (s *Stream) deliverEnd() {
	s.mu.Lock()
	s.closeRecvLocked(io.EOF)
	finished := s.sendEnded
	s.mu.Unlock()
	if finished {
		s.finish(ErrClosed)
	}
}

func (s *Stream) deliverError(err error) {
	if err == nil {
		err = ErrUnavailable
	}
	s.finish(err)
}

func (s *Stream) finish(err error) bool {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return false
	}
	s.done = true
	s.closeRecvLocked(err)
	s.cancel(err)
	s.mu.Unlock()
	if s.onDone != nil {
		s.onDone(s)
	}
	return true
}

func (s *Stream) closeRecvLocked(err error) {
	if s.recvClosed {
		return
	}
	s.recvClosed = true
	s.recvErr = err
	close(s.recvDone)
}

// StreamReader is a typed receive-only stream wrapper.
type StreamReader[T any] struct {
	stream *Stream
}

// Recv receives one typed stream item. It returns io.EOF when the remote side
// cleanly closes its send side.
func (r *StreamReader[T]) Recv() (T, error) {
	var item T
	if r == nil || r.stream == nil {
		return item, ErrClosed
	}

	err := r.stream.Recv(&item)
	return item, err
}

// Cancel cancels the stream and sends a best-effort cancel frame.
func (r *StreamReader[T]) Cancel() error {
	if r == nil || r.stream == nil {
		return ErrClosed
	}

	return r.stream.Cancel()
}

// Stream returns the raw stream.
func (r *StreamReader[T]) Stream() *Stream {
	if r == nil {
		return nil
	}

	return r.stream
}

// StreamWriter is a typed send-only stream wrapper.
type StreamWriter[T any] struct {
	stream *Stream
}

// Send sends one typed stream item.
func (w *StreamWriter[T]) Send(item T) error {
	if w == nil || w.stream == nil {
		return ErrClosed
	}

	return w.stream.Send(item)
}

// Close closes the local sending side of the stream.
func (w *StreamWriter[T]) Close() error {
	if w == nil || w.stream == nil {
		return ErrClosed
	}

	return w.stream.CloseSend()
}

// Stream returns the raw stream.
func (w *StreamWriter[T]) Stream() *Stream {
	if w == nil {
		return nil
	}

	return w.stream
}

// ClientStreamHandle is returned by ClientStream. It lets the caller send many
// request items and then receive one final response.
type ClientStreamHandle[Item, Resp any] struct {
	stream        *Stream
	responseCh    chan clientResponse
	codec         Codec
	removePending func(uint64)
}

// Send sends one typed request item.
func (s *ClientStreamHandle[Item, Resp]) Send(item Item) error {
	if s == nil || s.stream == nil {
		return ErrClosed
	}

	return s.stream.Send(item)
}

// CloseAndRecv closes the local sending side and waits for the final typed
// response.
func (s *ClientStreamHandle[Item, Resp]) CloseAndRecv() (Resp, error) {
	var resp Resp
	if s == nil || s.stream == nil {
		return resp, ErrClosed
	}
	defer s.stream.finish(ErrClosed)

	// A handler may return before the caller closes its send side.
	select {
	case response := <-s.responseCh:
		return s.decodeResponse(response)
	default:
	}
	if err := s.stream.CloseSend(); err != nil {
		select {
		case response := <-s.responseCh:
			return s.decodeResponse(response)
		default:
			return resp, err
		}
	}
	select {
	case response := <-s.responseCh:
		return s.decodeResponse(response)
	case <-s.stream.Context().Done():
		// Response delivery cancels the stream to wake any blocked sender.
		select {
		case response := <-s.responseCh:
			return s.decodeResponse(response)
		default:
			return resp, context.Cause(s.stream.Context())
		}
	}
}

func (s *ClientStreamHandle[Item, Resp]) decodeResponse(response clientResponse) (Resp, error) {
	var resp Resp
	if response.err != nil {
		return resp, response.err
	}
	err := decodeResponse(s.codec, response.frame, &resp)
	return resp, err
}

// Cancel cancels the stream and sends a best-effort cancel frame.
func (s *ClientStreamHandle[Item, Resp]) Cancel() error {
	if s == nil || s.stream == nil {
		return ErrClosed
	}
	if s.removePending != nil {
		s.removePending(s.stream.RequestID())
	}

	return s.stream.Cancel()
}

// Stream returns the raw stream.
func (s *ClientStreamHandle[Item, Resp]) Stream() *Stream {
	if s == nil {
		return nil
	}

	return s.stream
}

// BidiStreamHandle is a typed bidirectional stream wrapper. Send and Recv can
// be used concurrently by different goroutines.
type BidiStreamHandle[Send, Recv any] struct {
	stream *Stream
}

// Send sends one typed stream item.
func (s *BidiStreamHandle[Send, Recv]) Send(item Send) error {
	if s == nil || s.stream == nil {
		return ErrClosed
	}

	return s.stream.Send(item)
}

// Recv receives one typed stream item. It returns io.EOF when the remote side
// cleanly closes its send side.
func (s *BidiStreamHandle[Send, Recv]) Recv() (Recv, error) {
	var item Recv
	if s == nil || s.stream == nil {
		return item, ErrClosed
	}

	err := s.stream.Recv(&item)
	return item, err
}

// CloseSend closes the local sending side of the stream.
func (s *BidiStreamHandle[Send, Recv]) CloseSend() error {
	if s == nil || s.stream == nil {
		return ErrClosed
	}

	return s.stream.CloseSend()
}

// Cancel cancels the stream and sends a best-effort cancel frame.
func (s *BidiStreamHandle[Send, Recv]) Cancel() error {
	if s == nil || s.stream == nil {
		return ErrClosed
	}

	return s.stream.Cancel()
}

// Stream returns the raw stream.
func (s *BidiStreamHandle[Send, Recv]) Stream() *Stream {
	if s == nil {
		return nil
	}

	return s.stream
}

type streamTarget interface {
	openServerStream(ctx context.Context, function string, req any, opts StreamOptions) (*Stream, error)
	openClientStream(ctx context.Context, function string, opts StreamOptions) (*Stream, chan clientResponse, func(uint64), Codec, error)
	openBidiStream(ctx context.Context, function string, opts StreamOptions) (*Stream, error)
}

// ServerStream opens a server-streaming call. The caller sends one request and
// receives zero or more typed items until Recv returns io.EOF or an error.
//
// The target can be either *Client or an accepted *Conn, so either connected
// side can open a stream to the other side.
func ServerStream[Req, Item any](ctx context.Context, target any, function string, req Req) (*StreamReader[Item], error) {
	return ServerStreamWithOptions[Req, Item](ctx, target, function, req, StreamOptions{})
}

// ServerStreamWithOptions opens a server-streaming call with stream options.
func ServerStreamWithOptions[Req, Item any](ctx context.Context, target any, function string, req Req, opts StreamOptions) (*StreamReader[Item], error) {
	endpoint, err := asStreamTarget(target)
	if err != nil {
		return nil, err
	}

	stream, err := endpoint.openServerStream(ctx, function, req, opts)
	if err != nil {
		return nil, err
	}

	return &StreamReader[Item]{stream: stream}, nil
}

// ClientStream opens a client-streaming call. The caller sends zero or more
// typed items and then calls CloseAndRecv for the final typed response.
//
// The target can be either *Client or an accepted *Conn, so either connected
// side can open a stream to the other side.
func ClientStream[Item, Resp any](ctx context.Context, target any, function string) (*ClientStreamHandle[Item, Resp], error) {
	return ClientStreamWithOptions[Item, Resp](ctx, target, function, StreamOptions{})
}

// ClientStreamWithOptions opens a client-streaming call with stream options.
func ClientStreamWithOptions[Item, Resp any](ctx context.Context, target any, function string, opts StreamOptions) (*ClientStreamHandle[Item, Resp], error) {
	endpoint, err := asStreamTarget(target)
	if err != nil {
		return nil, err
	}

	stream, responseCh, removePending, codec, err := endpoint.openClientStream(ctx, function, opts)
	if err != nil {
		return nil, err
	}

	return &ClientStreamHandle[Item, Resp]{
		stream:        stream,
		responseCh:    responseCh,
		codec:         codec,
		removePending: removePending,
	}, nil
}

// BidiStream opens a bidirectional stream. Send and Recv can be used
// concurrently by different goroutines. Recv returns io.EOF when the remote
// send side closes cleanly.
//
// The target can be either *Client or an accepted *Conn, so either connected
// side can open a stream to the other side.
func BidiStream[Send, Recv any](ctx context.Context, target any, function string) (*BidiStreamHandle[Send, Recv], error) {
	return BidiStreamWithOptions[Send, Recv](ctx, target, function, StreamOptions{})
}

// BidiStreamWithOptions opens a bidirectional stream with stream options.
func BidiStreamWithOptions[Send, Recv any](ctx context.Context, target any, function string, opts StreamOptions) (*BidiStreamHandle[Send, Recv], error) {
	endpoint, err := asStreamTarget(target)
	if err != nil {
		return nil, err
	}

	stream, err := endpoint.openBidiStream(ctx, function, opts)
	if err != nil {
		return nil, err
	}

	return &BidiStreamHandle[Send, Recv]{stream: stream}, nil
}

func asStreamTarget(target any) (streamTarget, error) {
	if target == nil {
		return nil, ErrClosed
	}

	endpoint, ok := target.(streamTarget)
	if !ok {
		return nil, ErrInvalidHandler
	}

	return endpoint, nil
}

func remoteErrorFromFrame(codec Codec, frame Frame) error {
	var remoteErr RemoteError
	if err := defaultCodec(codec).Unmarshal(frame.Payload, &remoteErr); err != nil {
		return fmt.Errorf("decode remote error: %w", err)
	}

	return &remoteErr
}
