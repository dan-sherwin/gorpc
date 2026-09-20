package gorpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

func TestSlowServerStreamDoesNotBlockConnection(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, limit := range []string{"items", "bytes"} {
			t.Run(fmt.Sprintf("reverse=%t/%s", reverse, limit), func(t *testing.T) {
				opts := StreamOptions{RecvBuffer: 1}
				if limit == "bytes" {
					opts = StreamOptions{RecvBuffer: 16, RecvBytes: 10}
				}
				sent := make(chan int, 32)
				client, conn := streamTestPair(t, opts, func(target any) {
					MustRegisterServerStream(target, "items", func(_ *Context, _ struct{}, writer *StreamWriter[[]byte]) error {
						for i := range 32 {
							if err := writer.Send(bytes.Repeat([]byte{byte(i)}, 8)); err != nil {
								return err
							}
							sent <- i
						}
						return nil
					})
				})
				var target any = client
				if reverse {
					target = conn
				}
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()
				reader, err := ServerStream[struct{}, []byte](ctx, target, "items", struct{}{})
				if err != nil {
					t.Fatal(err)
				}
				if got := receiveTestValue(t, sent); got != 0 {
					t.Fatalf("first item = %d", got)
				}
				assertTestBlocked(t, sent)
				probeStreamConnection(t, target)
				for i := range 32 {
					item, err := reader.Recv()
					if err != nil || !bytes.Equal(item, bytes.Repeat([]byte{byte(i)}, 8)) {
						t.Fatalf("item %d = %v, %v", i, item, err)
					}
				}
				if _, err := reader.Recv(); !errors.Is(err, io.EOF) {
					t.Fatalf("end = %v", err)
				}
			})
		}
	}
}

func TestSlowClientStreamDoesNotBlockConnection(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			release := make(chan struct{})
			client, conn := streamTestPair(t, StreamOptions{RecvBuffer: 1}, func(target any) {
				MustRegisterClientStream(target, "upload", func(ctx *Context, reader *StreamReader[int]) (int, error) {
					select {
					case <-release:
					case <-ctx.Done():
						return 0, ctx.Err()
					}
					total := 0
					for {
						item, err := reader.Recv()
						if errors.Is(err, io.EOF) {
							return total, nil
						}
						if err != nil {
							return 0, err
						}
						total += item
					}
				})
			})
			var target any = client
			if reverse {
				target = conn
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			writer, err := ClientStream[int, int](ctx, target, "upload")
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.Send(1); err != nil {
				t.Fatal(err)
			}
			sent := make(chan error, 1)
			go func() { sent <- writer.Send(2) }()
			assertTestBlocked(t, sent)
			probeStreamConnection(t, target)
			close(release)
			if err := receiveTestValue(t, sent); err != nil {
				t.Fatal(err)
			}
			if total, err := writer.CloseAndRecv(); err != nil || total != 3 {
				t.Fatalf("response = %d, %v", total, err)
			}
		})
	}
}

func TestBidiStreamKeepsSendingAfterRemoteHalfClose(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			received := make(chan int, 1)
			client, conn := streamTestPair(t, StreamOptions{RecvBuffer: 1}, func(target any) {
				MustRegisterBidiStream(target, "half_close", func(_ *Context, stream *BidiStreamHandle[int, int]) error {
					if err := stream.CloseSend(); err != nil {
						return err
					}
					for i := 0; ; i++ {
						item, err := stream.Recv()
						if errors.Is(err, io.EOF) {
							received <- i
							return nil
						}
						if err != nil {
							return err
						}
						if item != i {
							return fmt.Errorf("item = %d, want %d", item, i)
						}
					}
				})
			})
			var target any = client
			if reverse {
				target = conn
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			stream, err := BidiStream[int, int](ctx, target, "half_close")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("half close = %v", err)
			}
			for i := range 100 {
				if err := stream.Send(i); err != nil {
					t.Fatalf("send %d: %v", i, err)
				}
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			if count := receiveTestValue(t, received); count != 100 {
				t.Fatalf("received %d items", count)
			}
		})
	}
}

func TestBlockedStreamSendTerminates(t *testing.T) {
	for _, stop := range []string{"cancel", "deadline", "disconnect", "remote_cancel", "remote_error", "early_response"} {
		t.Run(stop, func(t *testing.T) {
			release := make(chan struct{})
			client, conn := streamTestPair(t, StreamOptions{RecvBuffer: 1}, func(target any) {
				MustRegisterClientStream(target, "blocked", func(ctx *Context, reader *StreamReader[int]) (int, error) {
					select {
					case <-release:
						if stop == "remote_cancel" {
							return 0, reader.Cancel()
						}
						if stop == "early_response" {
							return 42, nil
						}
						return 0, NewRemoteError(ErrorCodeInvalidRequest, "rejected", nil)
					case <-ctx.Done():
						return 0, ctx.Err()
					}
				})
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if stop == "deadline" {
				var deadlineCancel context.CancelFunc
				ctx, deadlineCancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer deadlineCancel()
			}
			stream, err := ClientStream[int, int](ctx, client, "blocked")
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(1); err != nil {
				t.Fatal(err)
			}
			sent := make(chan error, 1)
			go func() { sent <- stream.Send(2) }()
			assertTestBlocked(t, sent)
			switch stop {
			case "cancel":
				cancel()
			case "disconnect":
				_ = conn.Close()
			case "remote_cancel", "remote_error", "early_response":
				close(release)
			}
			err = receiveTestValue(t, sent)
			if err == nil {
				t.Fatal("blocked send succeeded after termination")
			}
			switch stop {
			case "cancel", "remote_cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("send = %v, want canceled", err)
				}
			case "deadline":
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("send = %v, want deadline exceeded", err)
				}
			case "disconnect":
				if !errors.Is(err, ErrUnavailable) {
					t.Fatalf("send = %v, want unavailable", err)
				}
			case "remote_error":
				var remote *RemoteError
				if !errors.As(err, &remote) || remote.Code != ErrorCodeInvalidRequest {
					t.Fatalf("send = %v, want remote rejection", err)
				}
				if _, err := stream.CloseAndRecv(); !errors.As(err, &remote) {
					t.Fatalf("final response lost remote error: %v", err)
				}
			case "early_response":
				if value, err := stream.CloseAndRecv(); err != nil || value != 42 {
					t.Fatalf("early response = %d, %v", value, err)
				}
			}
		})
	}
}

func TestStreamItemLargerThanByteWindow(t *testing.T) {
	client, _ := streamTestPair(t, StreamOptions{RecvBytes: 8}, func(target any) {
		MustRegisterClientStream(target, "upload", func(_ *Context, reader *StreamReader[[]byte]) (int, error) {
			item, err := reader.Recv()
			return len(item), err
		})
	})
	stream, err := ClientStream[[]byte, int](t.Context(), client, "upload")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(make([]byte, 9)); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("send = %v, want size rejection", err)
	}
	if err := stream.Send([]byte{1, 2}); err != nil {
		t.Fatalf("send after size rejection: %v", err)
	}
	if size, err := stream.CloseAndRecv(); err != nil || size != 2 {
		t.Fatalf("response = %d, %v", size, err)
	}
}

func TestBidiStreamConcurrentHalfClose(t *testing.T) {
	for range 100 {
		var s *Stream
		ended := make(chan struct{}, 1)
		s = newStreamWithOptions(t.Context(), 1, "close", MessagePackCodec{}, func(frame Frame) error {
			if frame.Type == FrameStreamEnd {
				ended <- struct{}{}
			}
			return nil
		}, nil, StreamOptions{})
		s.configure(StreamKindBidi, true, true)
		var wg sync.WaitGroup
		wg.Go(func() { s.deliverEnd() })
		wg.Go(func() { _ = s.CloseSend() })
		wg.Wait()
		select {
		case <-ended:
		default:
			t.Fatal("remote half-close prevented local end frame")
		}
	}
}

func TestBidiStreamConcurrentTraffic(t *testing.T) {
	const items = 128
	exchange := func(stream *BidiStreamHandle[[]byte, []byte]) error {
		defer func() { _ = stream.Cancel() }()
		sent := make(chan error, 1)
		go func() {
			for i := range items {
				if err := stream.Send(bytes.Repeat([]byte{byte(i)}, 32*1024)); err != nil {
					sent <- err
					return
				}
			}
			sent <- stream.CloseSend()
		}()
		for i := range items {
			item, err := stream.Recv()
			if err != nil {
				return fmt.Errorf("receive item %d: %w", i, err)
			}
			if !bytes.Equal(item, bytes.Repeat([]byte{byte(i)}, 32*1024)) {
				return fmt.Errorf("item %d: wrong payload", i)
			}
		}
		if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
			return fmt.Errorf("end: %v", err)
		}
		return <-sent
	}
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			client, conn := streamTestPair(t, StreamOptions{RecvBuffer: 2, RecvBytes: 70 * 1024}, func(target any) {
				MustRegisterBidiStream(target, "duplex", func(_ *Context, stream *BidiStreamHandle[[]byte, []byte]) error {
					return exchange(stream)
				})
			})
			var target any = client
			if reverse {
				target = conn
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			results := make(chan error, 4)
			for range 4 {
				go func() {
					stream, err := BidiStream[[]byte, []byte](ctx, target, "duplex")
					if err == nil {
						err = exchange(stream)
					}
					results <- err
				}()
			}
			probeStreamConnection(t, target)
			for range 4 {
				if err := receiveTestValue(t, results); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestStreamCreditValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		grants []Frame
	}{
		{"no_items", []Frame{{WindowBytes: 8}}},
		{"no_bytes", []Frame{{WindowItems: 2}}},
		{"extra_items", []Frame{{WindowItems: 2, WindowBytes: 8}, {WindowItems: 1}}},
		{"extra_bytes", []Frame{{WindowItems: 2, WindowBytes: 8}, {WindowItems: 1, WindowBytes: 1}}},
		{"overflow", []Frame{{WindowItems: ^uint64(0), WindowBytes: ^uint64(0)}, {WindowItems: 1, WindowBytes: 1}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream := newStreamWithOptions(t.Context(), 1, "credit", MessagePackCodec{}, nil, nil, StreamOptions{})
			stream.configure(StreamKindBidi, true, true)
			for i, grant := range test.grants {
				err := stream.acceptCredit(grant)
				if i == len(test.grants)-1 {
					if !errors.Is(err, ErrProtocol) {
						t.Fatalf("invalid grant = %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				} else if test.name == "extra_bytes" {
					if err := stream.takeCredit(0); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
	stream := newStreamWithOptions(t.Context(), 1, "credit", MessagePackCodec{}, nil, nil, StreamOptions{})
	stream.configure(StreamKindBidi, true, true)
	grant := Frame{WindowItems: 2, WindowBytes: 8}
	if err := stream.acceptCredit(grant); err != nil {
		t.Fatal(err)
	}
	if err := stream.takeCredit(8); err != nil {
		t.Fatal(err)
	}
	if err := stream.takeCredit(0); err != nil {
		t.Fatal(err)
	}
	if err := stream.acceptCredit(Frame{WindowItems: 1}); err != nil {
		t.Fatalf("credit for a zero-byte item: %v", err)
	}
	if err := stream.acceptCredit(Frame{WindowItems: 1, WindowBytes: 8}); err != nil {
		t.Fatalf("return consumed credit: %v", err)
	}
}

func TestStreamReturnsBatchedCredit(t *testing.T) {
	for _, test := range []struct {
		name       string
		opts       StreamOptions
		queued     int
		batch      int
		payloadLen int
	}{
		{"items", StreamOptions{RecvBuffer: 8, RecvBytes: 1024}, 8, 4, 10},
		{"bytes", StreamOptions{RecvBuffer: 16, RecvBytes: 80}, 4, 2, 20},
		{"odd_window", StreamOptions{RecvBuffer: 3, RecvBytes: 1024}, 3, 2, 10},
		{"single_item", StreamOptions{RecvBuffer: 1, RecvBytes: 1024}, 1, 1, 10},
		{"empty_queue", StreamOptions{RecvBuffer: 16, RecvBytes: 1024}, 1, 1, 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			var grants []Frame
			stream := newStreamWithOptions(t.Context(), 1, "credit", MessagePackCodec{}, func(frame Frame) error {
				grants = append(grants, frame)
				return nil
			}, nil, test.opts)
			stream.configure(StreamKindServer, true, true)
			payload, err := stream.codec.Marshal(make([]byte, test.payloadLen-2))
			if err != nil || len(payload) != test.payloadLen {
				t.Fatalf("payload = %d bytes, %v", len(payload), err)
			}
			for range test.queued {
				stream.deliverItem(Frame{Payload: payload})
			}
			for i := 1; i <= test.queued; i++ {
				var item []byte
				if err := stream.Recv(&item); err != nil {
					t.Fatal(err)
				}
				wantGrants := i / test.batch
				if i == test.queued && i%test.batch != 0 {
					wantGrants++
				}
				if len(grants) != wantGrants {
					t.Fatalf("after %d items: %d grants, want %d", i, len(grants), wantGrants)
				}
			}
			var items, size uint64
			for _, grant := range grants {
				if grant.Type != FrameStreamWindow || grant.RequestID != 1 || grant.WindowItems == 0 {
					t.Fatalf("invalid grant: %+v", grant)
				}
				items += grant.WindowItems
				size += grant.WindowBytes
			}
			if items != uint64(test.queued) || size != uint64(test.queued*test.payloadLen) {
				t.Fatalf("returned %d items, %d bytes", items, size)
			}
		})
	}
}

func TestStreamCreditUpdateDoesNotAllocate(t *testing.T) {
	stream := newStreamWithOptions(t.Context(), 1, "credit", MessagePackCodec{}, nil, nil, StreamOptions{})
	stream.configure(StreamKindClient, true, true)
	if err := stream.acceptCredit(Frame{WindowItems: 16, WindowBytes: 1024}); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := stream.takeCredit(1); err != nil {
			t.Fatal(err)
		}
		if err := stream.acceptCredit(Frame{WindowItems: 1, WindowBytes: 1}); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("credit update allocated %.0f times", allocs)
	}
}

func TestStreamDiscardsPendingCreditAfterEnd(t *testing.T) {
	stream := newStreamWithOptions(t.Context(), 1, "credit", MessagePackCodec{}, func(Frame) error {
		t.Error("returned credit after the receive direction ended")
		return nil
	}, nil, StreamOptions{RecvBuffer: 8})
	stream.configure(StreamKindServer, true, true)
	for range 4 {
		stream.deliverItem(Frame{Payload: []byte{1}})
	}
	var item int
	if err := stream.Recv(&item); err != nil {
		t.Fatal(err)
	}
	stream.deliverEnd()
	for range 3 {
		if err := stream.Recv(&item); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.Recv(&item); !errors.Is(err, io.EOF) {
		t.Fatalf("end = %v", err)
	}
}

func TestStreamContextTerminationFrame(t *testing.T) {
	for _, test := range []struct {
		name  string
		cause error
		frame FrameType
	}{
		{"cancel", context.Canceled, FrameCancel},
		{"deadline", context.DeadlineExceeded, FrameError},
		{"wrapped_deadline", fmt.Errorf("parent: %w", context.DeadlineExceeded), FrameError},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			frames := make(chan Frame, 1)
			stream := newStreamWithOptions(ctx, 1, "deadline", MessagePackCodec{}, func(frame Frame) error {
				frames <- frame
				return nil
			}, nil, StreamOptions{})
			stream.configure(StreamKindBidi, true, true)
			stream.watchContext()
			cancel(test.cause)
			frame := receiveTestValue(t, frames)
			if frame.Type != test.frame {
				t.Fatalf("termination = %v, want %v", frame.Type, test.frame)
			}
			if test.frame == FrameError && !errors.Is(remoteErrorFromFrame(stream.codec, frame), context.DeadlineExceeded) {
				t.Fatal("remote termination lost the deadline cause")
			}
		})
	}
}

func TestStreamCreditFlushesBeforeLargerItem(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			client, conn := streamTestPair(t, StreamOptions{RecvBuffer: 16, RecvBytes: 32}, func(target any) {
				MustRegisterServerStream(target, "varying", func(_ *Context, _ struct{}, writer *StreamWriter[[]byte]) error {
					for range 16 {
						for _, size := range []int{1, 30} {
							if err := writer.Send(make([]byte, size)); err != nil {
								return err
							}
						}
					}
					return nil
				})
			})
			var target any = client
			if reverse {
				target = conn
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			reader, err := ServerStream[struct{}, []byte](ctx, target, "varying", struct{}{})
			if err != nil {
				t.Fatal(err)
			}
			for range 16 {
				for _, size := range []int{1, 30} {
					item, err := reader.Recv()
					if err != nil || len(item) != size {
						t.Fatalf("receive %d bytes: got %d, %v", size, len(item), err)
					}
				}
			}
			if _, err := reader.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("end = %v", err)
			}
		})
	}
}

func streamTestPair(t *testing.T, opts StreamOptions, register func(any)) (*Client, *Conn) {
	t.Helper()
	accepted := make(chan *Conn, 1)
	server, address, shutdown := startTestServerWithOptions(t, ServerOptions{
		StreamOptions: opts,
		OnConnect:     func(conn *Conn) { accepted <- conn },
	})
	t.Cleanup(shutdown)
	client := NewTCPClient(address, "flow-control-test", ClientOptions{StreamOptions: opts, PingInterval: -1})
	t.Cleanup(func() { _ = client.Close() })
	for _, target := range []any{client, server} {
		MustRegister(target, "probe", func(_ *Context, _ struct{}) (int, error) { return 7, nil })
		register(target)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	conn := receiveTestValue(t, accepted)
	if !client.SupportsStreamFlowControl() || !conn.SupportsStreamFlowControl() {
		t.Fatal("stream credit was not negotiated")
	}
	return client, conn
}

func probeStreamConnection(t *testing.T, target any) {
	t.Helper()
	caller := target.(interface {
		CallContext(context.Context, string, any, any) error
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var response int
	if err := caller.CallContext(ctx, "probe", struct{}{}, &response); err != nil || response != 7 {
		t.Fatalf("unrelated call = %d, %v", response, err)
	}
}

func receiveTestValue[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for stream progress")
		var zero T
		return zero
	}
}

func assertTestBlocked[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case value := <-ch:
		t.Fatalf("operation completed without receive credit: %v", value)
	case <-time.After(20 * time.Millisecond):
	}
}
