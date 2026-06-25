package gorpc

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type getChannelRequest struct {
	ID string
}

type getChannelResponse struct {
	ID   string
	Name string
}

func TestUnaryRoundTrip(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "ChannelTracker", "GetChannel", func(_ context.Context, req getChannelRequest) (getChannelResponse, error) {
		return getChannelResponse{ID: req.ID, Name: "Demand Orbit"}, nil
	})

	client, err := Dial(context.Background(), "tcp", address, ClientOptions{
		ClientName:          "manager",
		ExpectedServiceName: "channel-tracker",
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	resp, err := Call[getChannelRequest, getChannelResponse](context.Background(), client, "ChannelTracker", "GetChannel", getChannelRequest{ID: "abc123"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.ID != "abc123" || resp.Name != "Demand Orbit" {
		t.Fatalf("resp = %+v", resp)
	}
	if client.RemoteService() != "channel-tracker" {
		t.Fatalf("remote service = %q", client.RemoteService())
	}
}

func TestMethodHelper(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "ChannelTracker", "GetChannel", func(_ context.Context, req getChannelRequest) (getChannelResponse, error) {
		return getChannelResponse{ID: req.ID, Name: "via method helper"}, nil
	})

	client, err := Dial(context.Background(), "tcp", address, ClientOptions{ClientName: "manager"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	getChannel := Method[getChannelRequest, getChannelResponse](client, "ChannelTracker", "GetChannel")
	resp, err := getChannel(context.Background(), getChannelRequest{ID: "def456"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.Name != "via method helper" {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestConcurrentCallsOnSingleConnection(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "ChannelTracker", "GetChannel", func(_ context.Context, req getChannelRequest) (getChannelResponse, error) {
		time.Sleep(time.Duration(len(req.ID)%5) * time.Millisecond)
		return getChannelResponse{ID: req.ID, Name: "channel-" + req.ID}, nil
	})

	client, err := Dial(context.Background(), "tcp", address, ClientOptions{ClientName: "manager"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, 32)
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			id := string(rune('a' + i))
			resp, err := Call[getChannelRequest, getChannelResponse](context.Background(), client, "ChannelTracker", "GetChannel", getChannelRequest{ID: id})
			if err != nil {
				errCh <- err
				return
			}
			if resp.ID != id || resp.Name != "channel-"+id {
				errCh <- errors.New("response routed to the wrong caller")
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestRemoteError(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "ChannelTracker", "GetChannel", func(_ context.Context, req getChannelRequest) (getChannelResponse, error) {
		return getChannelResponse{}, NewRemoteError(ErrorCodeNotFound, "channel not found", map[string]any{
			"channel_id": req.ID,
		})
	})

	client, err := Dial(context.Background(), "tcp", address, ClientOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	_, err = Call[getChannelRequest, getChannelResponse](context.Background(), client, "ChannelTracker", "GetChannel", getChannelRequest{ID: "missing"})
	var remoteErr *RemoteError
	if !errors.As(err, &remoteErr) {
		t.Fatalf("err = %T %v, want RemoteError", err, err)
	}
	if remoteErr.Code != ErrorCodeNotFound || remoteErr.Message != "channel not found" {
		t.Fatalf("remote error = %+v", remoteErr)
	}
	if remoteErr.Details["channel_id"] != "missing" {
		t.Fatalf("details = %+v", remoteErr.Details)
	}
}

func TestContextDeadlineCancelsServerHandler(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	started := make(chan struct{})
	canceled := make(chan struct{})

	MustRegister(server, "ChannelTracker", "GetChannel", func(ctx context.Context, _ getChannelRequest) (getChannelResponse, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return getChannelResponse{}, ctx.Err()
	})

	client, err := Dial(context.Background(), "tcp", address, ClientOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	_, err = Call[getChannelRequest, getChannelResponse](ctx, client, "ChannelTracker", "GetChannel", getChannelRequest{ID: "slow"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context deadline exceeded", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("handler was not canceled")
	}
}

func TestClientMaxFrameSize(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "ChannelTracker", "GetChannel", func(_ context.Context, _ getChannelRequest) (getChannelResponse, error) {
		return getChannelResponse{}, nil
	})

	client, err := Dial(context.Background(), "tcp", address, ClientOptions{MaxFrameSize: 128})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	_, err = Call[getChannelRequest, getChannelResponse](context.Background(), client, "ChannelTracker", "GetChannel", getChannelRequest{
		ID: string(make([]byte, 1024)),
	})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func startTestServer(t *testing.T) (*Server, string, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := NewServer(ServerOptions{ServiceName: "channel-tracker"})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- server.Serve(ctx, ln)
	}()

	shutdown := func() {
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("serve: %v", err)
		}
	}

	return server, ln.Addr().String(), shutdown
}
