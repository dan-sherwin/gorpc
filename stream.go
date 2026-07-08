package gorpc

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

const streamRecvBuffer = 16

// StreamOptions configures newly opened streams. Zero values keep GoRPC
// defaults.
type StreamOptions struct {
	RecvBuffer int
}

func normalizeStreamOptions(opts StreamOptions) StreamOptions {
	if opts.RecvBuffer <= 0 {
		opts.RecvBuffer = streamRecvBuffer
	}

	return opts
}

func mergeStreamOptions(base, override StreamOptions) StreamOptions {
	if override.RecvBuffer > 0 {
		base.RecvBuffer = override.RecvBuffer
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
	onDone    func(uint64)

	ctx    context.Context
	cancel context.CancelFunc

	recvCh   chan streamDelivery
	recvDone chan struct{}

	receivesItems bool
	sendClosed    atomic.Bool
	recvClosed    atomic.Bool
	done          atomic.Bool

	closeSendOnce sync.Once
	cancelOnce    sync.Once
	removeOnce    sync.Once

	recvErrMu sync.Mutex
	recvErr   error
}

type streamDelivery struct {
	payload []byte
	err     error
	end     bool
}

func newStreamWithOptions(ctx context.Context, requestID uint64, function string, codec Codec, write func(Frame) error, onDone func(uint64), opts StreamOptions) *Stream {
	ctx = normalizeContext(ctx)
	streamCtx, cancel := context.WithCancel(ctx)
	opts = normalizeStreamOptions(opts)

	return &Stream{
		requestID:     requestID,
		function:      function,
		codec:         defaultCodec(codec),
		write:         write,
		onDone:        onDone,
		ctx:           streamCtx,
		cancel:        cancel,
		recvCh:        make(chan streamDelivery, opts.RecvBuffer),
		recvDone:      make(chan struct{}),
		receivesItems: true,
	}
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

// Context returns the stream context. It is canceled when the stream is locally
// canceled, the connection closes, or a remote stream error is received.
func (s *Stream) Context() context.Context {
	if s == nil || s.ctx == nil {
		return context.Background()
	}

	return s.ctx
}

// Send writes one stream item. The item is MessagePack-encoded into a
// FrameStreamItem payload.
func (s *Stream) Send(item any) error {
	if s == nil {
		return ErrClosed
	}
	if s.sendClosed.Load() {
		return ErrClosed
	}

	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}

	payload, err := s.codec.Marshal(item)
	if err != nil {
		return fmt.Errorf("encode stream item: %w", err)
	}

	if err := s.write(Frame{
		Type:      FrameStreamItem,
		RequestID: s.requestID,
		Function:  s.function,
		Payload:   payload,
	}); err != nil {
		s.finish(err)
		return err
	}

	return nil
}

// Recv reads one stream item into item. It returns io.EOF after the remote side
// closes its send side with a stream_end frame.
func (s *Stream) Recv(item any) error {
	if s == nil {
		return ErrClosed
	}
	if item == nil {
		return fmt.Errorf("%w: stream item must be a non-nil pointer", ErrInvalidResponse)
	}

	select {
	case delivery, ok := <-s.recvCh:
		return s.receiveDelivery(delivery, ok, item)
	default:
	}
	select {
	case <-s.recvDone:
		select {
		case delivery, ok := <-s.recvCh:
			return s.receiveDelivery(delivery, ok, item)
		default:
			return s.receiveError()
		}
	default:
	}

	select {
	case delivery, ok := <-s.recvCh:
		return s.receiveDelivery(delivery, ok, item)
	case <-s.recvDone:
		select {
		case delivery, ok := <-s.recvCh:
			return s.receiveDelivery(delivery, ok, item)
		default:
			return s.receiveError()
		}
	case <-s.ctx.Done():
		select {
		case delivery, ok := <-s.recvCh:
			return s.receiveDelivery(delivery, ok, item)
		default:
			select {
			case <-s.recvDone:
				return s.receiveError()
			default:
			}
			return s.ctx.Err()
		}
	}
}

func (s *Stream) receiveDelivery(delivery streamDelivery, ok bool, item any) error {
	if !ok {
		return io.EOF
	}
	if delivery.err != nil {
		return delivery.err
	}
	if delivery.end {
		return io.EOF
	}
	if err := s.codec.Unmarshal(delivery.payload, item); err != nil {
		return fmt.Errorf("decode stream item: %w", err)
	}

	return nil
}

func (s *Stream) receiveError() error {
	s.recvErrMu.Lock()
	defer s.recvErrMu.Unlock()

	if s.recvErr == nil {
		return io.EOF
	}

	return s.recvErr
}

// CloseSend closes the local sending side of the stream with a stream_end
// frame. It does not cancel receiving items from the remote side.
func (s *Stream) CloseSend() error {
	if s == nil {
		return ErrClosed
	}

	var err error
	s.closeSendOnce.Do(func() {
		s.sendClosed.Store(true)
		select {
		case <-s.ctx.Done():
			err = s.ctx.Err()
			return
		default:
		}

		err = s.write(Frame{
			Type:      FrameStreamEnd,
			RequestID: s.requestID,
			Function:  s.function,
		})
		if err != nil {
			s.finish(err)
		}
	})

	return err
}

// Cancel cancels the whole stream and sends a best-effort cancel frame to the
// remote side.
func (s *Stream) Cancel() error {
	if s == nil {
		return ErrClosed
	}

	var err error
	s.cancelOnce.Do(func() {
		err = s.write(Frame{
			Type:      FrameCancel,
			RequestID: s.requestID,
			Function:  s.function,
		})
		s.finish(context.Canceled)
	})

	return err
}

func (s *Stream) deliverItem(frame Frame) {
	if s == nil || s.recvClosed.Load() {
		return
	}

	s.deliver(streamDelivery{payload: frame.Payload})
}

func (s *Stream) deliverEnd() {
	if s == nil {
		return
	}
	s.closeRecv(io.EOF)
	s.remove()
}

func (s *Stream) deliverError(err error) {
	if s == nil {
		return
	}
	if err == nil {
		err = ErrUnavailable
	}

	s.closeRecv(err)
	s.finish(err)
}

func (s *Stream) deliver(delivery streamDelivery) {
	select {
	case s.recvCh <- delivery:
	case <-s.ctx.Done():
	}
}

func (s *Stream) finish(err error) {
	if s == nil {
		return
	}
	if !s.done.CompareAndSwap(false, true) {
		return
	}
	if err == nil {
		err = ErrClosed
	}

	s.closeRecv(err)
	s.cancel()
	s.remove()
}

func (s *Stream) closeRecv(err error) {
	if s == nil {
		return
	}
	if err == nil {
		err = io.EOF
	}
	if !s.recvClosed.CompareAndSwap(false, true) {
		return
	}

	s.recvErrMu.Lock()
	s.recvErr = err
	s.recvErrMu.Unlock()

	close(s.recvDone)
}

func (s *Stream) remove() {
	if s.onDone != nil {
		s.removeOnce.Do(func() {
			s.onDone(s.requestID)
		})
	}
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

	if err := s.stream.CloseSend(); err != nil {
		return resp, err
	}

	select {
	case response := <-s.responseCh:
		if response.err != nil {
			return resp, response.err
		}
		err := decodeResponse(s.codec, response.frame, &resp)
		return resp, err
	case <-s.stream.Context().Done():
		if s.removePending != nil {
			s.removePending(s.stream.RequestID())
		}
		return resp, s.stream.Context().Err()
	}
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
