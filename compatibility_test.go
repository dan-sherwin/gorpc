package gorpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

// legacyCodec models the pre-extension handshake and refuses any new frame
// fields on its sending side. The MessagePack decoder ignores unknown fields,
// as the released v1 peers do.
type legacyCodec struct{ MessagePackCodec }

func (c legacyCodec) Marshal(value any) ([]byte, error) {
	switch v := value.(type) {
	case hello:
		v.Capabilities = nil
		value = v
	case helloAck:
		v.Capabilities = nil
		value = v
	case Frame:
		if v.Type == FrameStreamWindow || v.WindowItems != 0 || v.WindowBytes != 0 {
			return nil, errors.New("legacy peer cannot send stream credit")
		}
	}
	return c.MessagePackCodec.Marshal(value)
}

func (c legacyCodec) Unmarshal(data []byte, value any) error {
	if err := c.MessagePackCodec.Unmarshal(data, value); err != nil {
		return err
	}
	switch v := value.(type) {
	case *hello:
		v.Capabilities = nil
	case *helloAck:
		v.Capabilities = nil
	case *Frame:
		if v.Type == FrameStreamWindow || v.WindowItems != 0 || v.WindowBytes != 0 {
			return errors.New("legacy peer received stream credit")
		}
	}
	return nil
}

func TestLegacyPeerCompatibility(t *testing.T) {
	for _, oldServer := range []bool{false, true} {
		for _, authenticated := range []bool{false, true} {
			t.Run(fmt.Sprintf("oldServer=%t/auth=%t", oldServer, authenticated), func(t *testing.T) {
				serverOpts := ServerOptions{Compression: GzipCompression()}
				clientOpts := ClientOptions{Compression: GzipCompression(), PingInterval: -1}
				if oldServer {
					serverOpts.Codec = legacyCodec{}
				} else {
					clientOpts.Codec = legacyCodec{}
				}
				if authenticated {
					serverOpts.Auth = SharedSecret("compatibility-test")
					clientOpts.Auth = serverOpts.Auth
				}
				accepted := make(chan *Conn, 1)
				serverOpts.OnConnect = func(conn *Conn) { accepted <- conn }
				server, address, shutdown := startTestServerWithOptions(t, serverOpts)
				defer shutdown()
				client := NewTCPClient(address, "legacy-test", clientOpts)
				defer func() { _ = client.Close() }()
				notified := make(chan int, 2)
				for _, target := range []any{server, client} {
					MustRegister(target, "probe", func(_ *Context, _ struct{}) (int, error) { return 7, nil })
					MustRegisterNotify(target, "notice", func(_ *Context, n int) error { notified <- n; return nil })
					MustRegisterServerStream(target, "list", func(_ *Context, _ struct{}, w *StreamWriter[int]) error { return w.Send(9) })
					MustRegisterClientStream(target, "upload", func(_ *Context, r *StreamReader[int]) (int, error) { return r.Recv() })
					MustRegisterBidiStream(target, "echo", func(_ *Context, s *BidiStreamHandle[int, int]) error {
						n, err := s.Recv()
						if err != nil {
							return err
						}
						return s.Send(n)
					})
				}
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()
				if err := client.Connect(ctx); err != nil {
					t.Fatal(err)
				}
				conn := receiveTestValue(t, accepted)
				if client.SupportsStreamFlowControl() || conn.SupportsStreamFlowControl() {
					t.Fatal("legacy connection negotiated stream credit")
				}
				for _, target := range []any{client, conn} {
					probeStreamConnection(t, target)
					if err := target.(interface{ Notify(string, any) error }).Notify("notice", 11); err != nil {
						t.Fatal(err)
					}
					if got := receiveTestValue(t, notified); got != 11 {
						t.Fatalf("notification = %d", got)
					}
					r, err := ServerStream[struct{}, int](ctx, target, "list", struct{}{})
					if err != nil {
						t.Fatal(err)
					}
					if n, err := r.Recv(); err != nil || n != 9 {
						t.Fatalf("list item = %d, %v", n, err)
					}
					if _, err := r.Recv(); !errors.Is(err, io.EOF) {
						t.Fatalf("list end = %v", err)
					}
					w, err := ClientStream[int, int](ctx, target, "upload")
					if err != nil {
						t.Fatal(err)
					}
					if err := w.Send(12); err != nil {
						t.Fatal(err)
					}
					if n, err := w.CloseAndRecv(); err != nil || n != 12 {
						t.Fatalf("upload = %d, %v", n, err)
					}
					s, err := BidiStream[int, int](ctx, target, "echo")
					if err != nil {
						t.Fatal(err)
					}
					if err := s.Send(13); err != nil {
						t.Fatal(err)
					}
					if err := s.CloseSend(); err != nil {
						t.Fatal(err)
					}
					if n, err := s.Recv(); err != nil || n != 13 {
						t.Fatalf("echo = %d, %v", n, err)
					}
					if _, err := s.Recv(); !errors.Is(err, io.EOF) {
						t.Fatalf("echo end = %v", err)
					}
				}
			})
		}
	}
}

func TestLegacyStreamOverflowIsIsolated(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()
	MustRegister(server, "probe", func(_ *Context, _ struct{}) (int, error) { return 7, nil })
	MustRegisterServerStream(server, "flood", func(_ *Context, _ struct{}, w *StreamWriter[int]) error {
		for i := range 100 {
			if err := w.Send(i); err != nil {
				return err
			}
		}
		return nil
	})
	client, err := TCPDial(address, "legacy-overflow", ClientOptions{Codec: legacyCodec{}, StreamOptions: StreamOptions{RecvBuffer: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	r, err := ServerStream[struct{}, int](ctx, client, "flood", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.Stream().Context().Done():
	case <-ctx.Done():
		t.Fatal("overflow did not terminate stream")
	}
	if n, err := r.Recv(); err != nil || n != 0 {
		t.Fatalf("buffered item = %d, %v", n, err)
	}
	if _, err := r.Recv(); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("overflow = %v", err)
	}
	probeStreamConnection(t, client)
}
