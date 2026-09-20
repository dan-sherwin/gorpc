package gorpc

import (
	"errors"
	"net"
	"testing"
)

func TestControlFramesBypassWriteLimit(t *testing.T) {
	for _, side := range []string{"client", "server"} {
		t.Run(side, func(t *testing.T) {
			local, remote := net.Pipe()
			t.Cleanup(func() { _ = local.Close() })
			t.Cleanup(func() { _ = remote.Close() })
			limits := BackpressureOptions{MaxConcurrentWrites: 1}
			var write func(Frame) error
			var limiter *writeLimiter
			if side == "client" {
				client := NewTCPClient("", "controls", ClientOptions{Backpressure: limits})
				t.Cleanup(func() { _ = client.Close() })
				if err := client.setConn(local, 1, true); err != nil {
					t.Fatal(err)
				}
				limiter = client.writeLimiter
				write = func(frame Frame) error { return client.writeTo(local, frame) }
			} else {
				conn := newConn(NewServer(ServerOptions{Backpressure: limits}), local)
				t.Cleanup(func() { _ = conn.Close() })
				limiter = conn.writeLimiter
				write = conn.write
			}
			if !limiter.acquire() {
				t.Fatal("could not occupy write slot")
			}
			defer limiter.release()
			if err := write(Frame{Type: FrameStreamItem}); !errors.Is(err, ErrBackpressure) {
				t.Fatalf("data frame = %v", err)
			}
			for _, kind := range []FrameType{FrameStreamWindow, FrameStreamEnd, FrameCancel, FrameError, FramePing, FramePong} {
				read := make(chan error, 1)
				go func() {
					_, err := readFrame(remote, DefaultMaxFrameSize, MessagePackCodec{})
					read <- err
				}()
				if err := write(Frame{Type: kind}); err != nil {
					t.Fatalf("control frame %s: %v", kind, err)
				}
				if err := receiveTestValue(t, read); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestBackpressureCallbackCanInspectConnection(t *testing.T) {
	for _, side := range []string{"client", "server"} {
		t.Run(side, func(t *testing.T) {
			var callback func()
			limits := BackpressureOptions{
				MaxActiveStreams: 1,
				MaxPendingCalls:  1,
				OnBackpressure:   func(_ BackpressureInfo) { callback() },
			}
			var addStream func(*Stream) error
			var addPending func(uint64, pendingCall) error
			if side == "client" {
				client := NewTCPClient("", "callback", ClientOptions{Backpressure: limits})
				t.Cleanup(func() { _ = client.Close() })
				addStream, addPending = client.addStream, client.addPending
				callback = func() {
					client.removePending(99)
					_ = client.findStream(99)
				}
			} else {
				local, remote := net.Pipe()
				t.Cleanup(func() { _ = remote.Close() })
				conn := newConn(NewServer(ServerOptions{Backpressure: limits}), local)
				t.Cleanup(func() { _ = conn.Close() })
				addStream, addPending = conn.addStream, conn.addPending
				callback = func() {
					conn.removePending(99)
					_ = conn.findStream(99)
				}
			}
			for id := uint64(1); id <= 2; id++ {
				stream := newStreamWithOptions(t.Context(), id, "limited", MessagePackCodec{}, nil, nil, StreamOptions{})
				if err := addStream(stream); (id == 1 && err != nil) || (id == 2 && !errors.Is(err, ErrBackpressure)) {
					t.Fatalf("add stream %d: %v", id, err)
				}
				pending := syncPendingCall{ch: make(chan clientResponse, 1)}
				if err := addPending(id, pending); (id == 1 && err != nil) || (id == 2 && !errors.Is(err, ErrBackpressure)) {
					t.Fatalf("add pending %d: %v", id, err)
				}
			}
		})
	}
}
