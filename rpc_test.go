package gorpc

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type getItemRequest struct {
	ID string
}

type getItemResponse struct {
	ID   string
	Name string
}

func TestUnaryRoundTrip(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "get_an_item", func(_ *Context, req getItemRequest) (getItemResponse, error) {
		return getItemResponse{ID: req.ID, Name: "Widget Pack"}, nil
	})

	client, err := Dial(context.Background(), "tcp", address, ClientOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	var resp getItemResponse
	if err := client.CallWithTimeout("get_an_item", getItemRequest{ID: "abc123"}, &resp, time.Second); err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.ID != "abc123" || resp.Name != "Widget Pack" {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestRequestContextMetadata(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	type requestInfo struct {
		clientName string
		requestID  uint64
		function   string
		remoteAddr net.Addr
		localAddr  net.Addr
	}

	infoCh := make(chan requestInfo, 1)
	MustRegister(server, "get_an_item", func(ctx *Context, req getItemRequest) (getItemResponse, error) {
		infoCh <- requestInfo{
			clientName: ctx.ClientName(),
			requestID:  ctx.RequestID(),
			function:   ctx.Function(),
			remoteAddr: ctx.RemoteAddr(),
			localAddr:  ctx.LocalAddr(),
		}

		return getItemResponse{ID: req.ID, Name: "with metadata"}, nil
	})

	client, err := TCPDial(address, "metadata-test-client", ClientOptions{
		PingInterval: -1,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	var resp getItemResponse
	if err := client.Call("get_an_item", getItemRequest{ID: "abc123"}, &resp); err != nil {
		t.Fatalf("call: %v", err)
	}

	select {
	case info := <-infoCh:
		if info.clientName != "metadata-test-client" {
			t.Fatalf("client name = %q", info.clientName)
		}
		if info.requestID == 0 {
			t.Fatal("request ID was not set")
		}
		if info.function != "get_an_item" {
			t.Fatalf("function = %q", info.function)
		}
		if info.remoteAddr == nil {
			t.Fatal("remote addr was not set")
		}
		if info.localAddr == nil {
			t.Fatal("local addr was not set")
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not send request info")
	}
}

func TestSharedSecretAuth(t *testing.T) {
	server, address, shutdown := startTestServerWithOptions(t, ServerOptions{
		Auth: SharedSecret("correct horse battery staple"),
	})
	defer shutdown()

	MustRegister(server, "get_an_item", func(_ *Context, req getItemRequest) (getItemResponse, error) {
		return getItemResponse{ID: req.ID, Name: "authenticated"}, nil
	})

	client, err := TCPDial(address, "auth-test-client", ClientOptions{
		Auth:         SharedSecret("correct horse battery staple"),
		PingInterval: -1,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	var resp getItemResponse
	if err := client.Call("get_an_item", getItemRequest{ID: "abc123"}, &resp); err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.Name != "authenticated" {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestSharedSecretAuthRejectsMissingClientAuth(t *testing.T) {
	_, address, shutdown := startTestServerWithOptions(t, ServerOptions{
		Auth: SharedSecret("server-secret"),
	})
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := Dial(ctx, "tcp", address, ClientOptions{
		PingInterval: -1,
	})
	if !errors.Is(err, ErrAuthentication) {
		t.Fatalf("err = %v, want ErrAuthentication", err)
	}
}

func TestSharedSecretAuthRejectsWrongSecret(t *testing.T) {
	_, address, shutdown := startTestServerWithOptions(t, ServerOptions{
		Auth: SharedSecret("server-secret"),
	})
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := Dial(ctx, "tcp", address, ClientOptions{
		Auth:         SharedSecret("client-secret"),
		PingInterval: -1,
	})
	if !errors.Is(err, ErrAuthentication) {
		t.Fatalf("err = %v, want ErrAuthentication", err)
	}
}

func TestFunctionHelper(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "get_an_item", func(_ *Context, req getItemRequest) (getItemResponse, error) {
		return getItemResponse{ID: req.ID, Name: "via function helper"}, nil
	})

	client, err := Dial(context.Background(), "tcp", address, ClientOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	getItem := Function[getItemRequest, getItemResponse](client, "get_an_item")
	resp, err := getItem(context.Background(), getItemRequest{ID: "def456"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.Name != "via function helper" {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestDialRetriesUntilInitialConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	clientCh := make(chan *Client, 1)
	errCh := make(chan error, 1)
	go func() {
		client, err := Dial(ctx, "tcp", address, reconnectTestOptions())
		if err != nil {
			errCh <- err
			return
		}
		clientCh <- client
	}()

	time.Sleep(50 * time.Millisecond)

	_, shutdown := startInventoryServerAt(t, address, "late server")
	defer shutdown()

	var client *Client
	select {
	case client = <-clientCh:
	case err := <-errCh:
		t.Fatalf("dial: %v", err)
	case <-ctx.Done():
		t.Fatalf("dial timed out: %v", ctx.Err())
	}
	defer func() {
		_ = client.Close()
	}()

	var resp getItemResponse
	if err := client.Call("get_an_item", getItemRequest{ID: "abc123"}, &resp); err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.Name != "late server" {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestClientReconnectsAfterServerRestart(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := ln.Addr().String()

	_, shutdownFirst := startInventoryServerOnListener(t, ln, "before restart")

	client, err := Dial(context.Background(), "tcp", address, reconnectTestOptions())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	var resp getItemResponse
	if err := client.Call("get_an_item", getItemRequest{ID: "abc123"}, &resp); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if resp.Name != "before restart" {
		t.Fatalf("first resp = %+v", resp)
	}

	shutdownFirst()

	waitForUnavailable(t, client)

	_, shutdownSecond := startInventoryServerAt(t, address, "after restart")
	defer shutdownSecond()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp = getItemResponse{}
	if err := client.CallContext(ctx, "get_an_item", getItemRequest{ID: "def456"}, &resp); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if resp.ID != "def456" || resp.Name != "after restart" {
		t.Fatalf("second resp = %+v", resp)
	}
}

func TestConcurrentCallsOnSingleConnection(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "get_an_item", func(_ *Context, req getItemRequest) (getItemResponse, error) {
		time.Sleep(time.Duration(len(req.ID)%5) * time.Millisecond)
		return getItemResponse{ID: req.ID, Name: "item-" + req.ID}, nil
	})

	client, err := Dial(context.Background(), "tcp", address, ClientOptions{})
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
			var resp getItemResponse
			if err := client.Call("get_an_item", getItemRequest{ID: id}, &resp); err != nil {
				errCh <- err
				return
			}
			if resp.ID != id || resp.Name != "item-"+id {
				errCh <- errors.New("response delivered to the wrong caller")
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

	MustRegister(server, "get_an_item", func(_ *Context, req getItemRequest) (getItemResponse, error) {
		return getItemResponse{}, NewRemoteError(ErrorCodeNotFound, "item not found", map[string]any{
			"item_id": req.ID,
		})
	})

	client, err := Dial(context.Background(), "tcp", address, ClientOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	var resp getItemResponse
	err = client.Call("get_an_item", getItemRequest{ID: "missing"}, &resp)
	var remoteErr *RemoteError
	if !errors.As(err, &remoteErr) {
		t.Fatalf("err = %T %v, want RemoteError", err, err)
	}
	if remoteErr.Code != ErrorCodeNotFound || remoteErr.Message != "item not found" {
		t.Fatalf("remote error = %+v", remoteErr)
	}
	if remoteErr.Details["item_id"] != "missing" {
		t.Fatalf("details = %+v", remoteErr.Details)
	}
}

func TestAsyncCall(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "get_an_item", func(_ *Context, req getItemRequest) (getItemResponse, error) {
		return getItemResponse{ID: req.ID, Name: "async item"}, nil
	})

	client, err := TCPDial(address, "async-test-client", ClientOptions{
		PingInterval: -1,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	type asyncResult struct {
		correlationID string
		requestID     uint64
		function      string
		resp          getItemResponse
		err           error
	}

	results := make(chan asyncResult, 1)
	handler := func(ctx ClientContext, resp *getItemResponse) {
		result := asyncResult{
			correlationID: ctx.CorrelationID(),
			requestID:     ctx.RequestID(),
			function:      ctx.Function(),
			err:           ctx.Error(),
		}
		if resp != nil {
			result.resp = *resp
		}
		results <- result
	}

	if err := client.AsyncCall("get_an_item", getItemRequest{ID: "abc123"}, handler, "corr-123"); err != nil {
		t.Fatalf("async call: %v", err)
	}

	select {
	case result := <-results:
		if result.err != nil {
			t.Fatalf("async err: %v", result.err)
		}
		if result.correlationID != "corr-123" {
			t.Fatalf("correlation ID = %q", result.correlationID)
		}
		if result.requestID == 0 {
			t.Fatal("request ID was not set")
		}
		if result.function != "get_an_item" {
			t.Fatalf("function = %q", result.function)
		}
		if result.resp.ID != "abc123" || result.resp.Name != "async item" {
			t.Fatalf("resp = %+v", result.resp)
		}
	case <-time.After(time.Second):
		t.Fatal("async handler was not called")
	}
}

func TestAsyncCallRemoteError(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "get_an_item", func(_ *Context, req getItemRequest) (getItemResponse, error) {
		return getItemResponse{}, NewRemoteError(ErrorCodeNotFound, "item not found", map[string]any{
			"item_id": req.ID,
		})
	})

	client, err := TCPDial(address, "async-error-test-client", ClientOptions{
		PingInterval: -1,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	errs := make(chan error, 1)
	handler := func(ctx ClientContext, _ *getItemResponse) {
		errs <- ctx.Error()
	}

	if err := client.AsyncCall("get_an_item", getItemRequest{ID: "missing"}, handler, "missing-corr"); err != nil {
		t.Fatalf("async call: %v", err)
	}

	select {
	case err := <-errs:
		var remoteErr *RemoteError
		if !errors.As(err, &remoteErr) {
			t.Fatalf("err = %T %v, want RemoteError", err, err)
		}
		if remoteErr.Code != ErrorCodeNotFound || remoteErr.Details["item_id"] != "missing" {
			t.Fatalf("remote error = %+v", remoteErr)
		}
	case <-time.After(time.Second):
		t.Fatal("async handler was not called")
	}
}

func TestAsyncCallRejectsInvalidHandler(t *testing.T) {
	client := &Client{}

	err := client.AsyncCall("get_an_item", getItemRequest{ID: "abc123"}, func(*getItemResponse) {}, "corr-123")
	if !errors.Is(err, ErrInvalidHandler) {
		t.Fatalf("err = %v, want ErrInvalidHandler", err)
	}
}

func TestAsyncCallRecoversHandlerPanic(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "get_an_item", func(_ *Context, req getItemRequest) (getItemResponse, error) {
		return getItemResponse{ID: req.ID, Name: "panic callback"}, nil
	})

	client, err := TCPDial(address, "async-panic-test-client", ClientOptions{
		PingInterval: -1,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	called := make(chan struct{})
	handler := func(_ ClientContext, _ *getItemResponse) {
		close(called)
		panic("callback exploded")
	}

	if err := client.AsyncCall("get_an_item", getItemRequest{ID: "abc123"}, handler, "panic-corr"); err != nil {
		t.Fatalf("async call: %v", err)
	}

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("async handler was not called")
	}
}

func TestCallRejectsInvalidResponse(t *testing.T) {
	client := &Client{}

	err := client.Call("get_an_item", getItemRequest{ID: "abc123"}, getItemResponse{})
	if !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("err = %v, want ErrInvalidResponse", err)
	}
}

func TestHandlerPanicReturnsRemoteError(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "get_an_item", func(_ *Context, _ getItemRequest) (getItemResponse, error) {
		panic("handler exploded")
	})

	client, err := TCPDial(address, "handler-panic-test-client", ClientOptions{
		PingInterval: -1,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	var resp getItemResponse
	err = client.Call("get_an_item", getItemRequest{ID: "abc123"}, &resp)
	var remoteErr *RemoteError
	if !errors.As(err, &remoteErr) {
		t.Fatalf("err = %T %v, want RemoteError", err, err)
	}
	if remoteErr.Code != ErrorCodeInternal {
		t.Fatalf("code = %q, want %q", remoteErr.Code, ErrorCodeInternal)
	}
	if !strings.Contains(remoteErr.Message, "handler panic") {
		t.Fatalf("message = %q", remoteErr.Message)
	}
}

func TestContextDeadlineCancelsServerHandler(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	started := make(chan struct{})
	canceled := make(chan struct{})

	MustRegister(server, "get_an_item", func(ctx *Context, _ getItemRequest) (getItemResponse, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return getItemResponse{}, ctx.Err()
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

	var resp getItemResponse
	err = client.CallContext(ctx, "get_an_item", getItemRequest{ID: "slow"}, &resp)
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

	MustRegister(server, "get_an_item", func(_ *Context, _ getItemRequest) (getItemResponse, error) {
		return getItemResponse{}, nil
	})

	client, err := Dial(context.Background(), "tcp", address, ClientOptions{MaxFrameSize: 128})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	var resp getItemResponse
	err = client.Call("get_an_item", getItemRequest{
		ID: string(make([]byte, 1024)),
	}, &resp)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func startTestServer(t *testing.T) (*Server, string, func()) {
	t.Helper()

	return startTestServerWithOptions(t, ServerOptions{})
}

func startTestServerWithOptions(t *testing.T, opts ServerOptions) (*Server, string, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := NewServer(opts)
	errCh := make(chan error, 1)

	go func() {
		errCh <- server.ServeListener(ln)
	}()

	shutdown := func() {
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

func reconnectTestOptions() ClientOptions {
	return ClientOptions{
		ReconnectMinDelay: 10 * time.Millisecond,
		ReconnectMaxDelay: 50 * time.Millisecond,
		ReconnectJitter:   -1,
		PingInterval:      -1,
	}
}

func startInventoryServerAt(t *testing.T, address, name string) (*Server, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("listen %s: %v", address, err)
	}

	return startInventoryServerOnListener(t, ln, name)
}

func startInventoryServerOnListener(t *testing.T, ln net.Listener, name string) (*Server, func()) {
	t.Helper()

	server := NewServer(ServerOptions{})
	MustRegister(server, "get_an_item", func(_ *Context, req getItemRequest) (getItemResponse, error) {
		return getItemResponse{ID: req.ID, Name: name}, nil
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeListener(ln)
	}()

	shutdown := func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("serve: %v", err)
		}
	}

	return server, shutdown
}

func waitForUnavailable(t *testing.T, client *Client) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		var resp getItemResponse
		err := client.CallContext(ctx, "get_an_item", getItemRequest{ID: "probe"}, &resp)
		cancel()
		if err != nil {
			if errors.Is(err, ErrUnavailable) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("client did not observe unavailable connection")
}
