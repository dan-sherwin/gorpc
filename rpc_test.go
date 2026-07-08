package gorpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
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

type clientInfoRequest struct {
	Prefix string
}

type clientInfoResponse struct {
	Message string
}

type itemChangedNotification struct {
	ID   string
	Name string
}

type streamListRequest struct {
	Prefix string
	Count  int
}

type streamItem struct {
	Value string
}

type streamSummary struct {
	Count  int
	Joined string
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

func TestNotifyClientToServer(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	notifications := make(chan itemChangedNotification, 1)
	MustRegisterNotify(server, "item_changed", func(ctx *Context, notification itemChangedNotification) error {
		if !ctx.IsNotify() {
			return errors.New("context did not mark notification")
		}
		if ctx.RequestID() == 0 {
			return errors.New("notification request ID was not set")
		}
		if ctx.Function() != "item_changed" {
			return errors.New("notification function was not set")
		}

		notifications <- notification
		return nil
	})

	client, err := TCPDial(address, "notify-test-client", ClientOptions{
		PingInterval: -1,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	if err := client.Notify("item_changed", itemChangedNotification{ID: "abc123", Name: "Widget Pack"}); err != nil {
		t.Fatalf("notify: %v", err)
	}

	select {
	case notification := <-notifications:
		if notification.ID != "abc123" || notification.Name != "Widget Pack" {
			t.Fatalf("notification = %+v", notification)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive notification")
	}
}

func TestNotifyServerToClientFromHandler(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "get_an_item", func(ctx *Context, req getItemRequest) (getItemResponse, error) {
		if err := ctx.Notify("item_changed", itemChangedNotification{ID: req.ID, Name: "server push"}); err != nil {
			return getItemResponse{}, err
		}

		return getItemResponse{ID: req.ID, Name: "Widget Pack"}, nil
	})

	notifications := make(chan itemChangedNotification, 1)
	client := NewTCPClient(address, "server-notify-handler-client", ClientOptions{
		PingInterval: -1,
	})
	MustRegisterNotify(client, "item_changed", func(ctx *Context, notification itemChangedNotification) error {
		if !ctx.IsNotify() {
			return errors.New("context did not mark notification")
		}
		notifications <- notification
		return nil
	})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	var resp getItemResponse
	if err := client.Call("get_an_item", getItemRequest{ID: "abc123"}, &resp); err != nil {
		t.Fatalf("call: %v", err)
	}

	select {
	case notification := <-notifications:
		if notification.ID != "abc123" || notification.Name != "server push" {
			t.Fatalf("notification = %+v", notification)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not receive notification")
	}
}

func TestNotifyServerToClientOnConnect(t *testing.T) {
	_, address, shutdown := startTestServerWithOptions(t, ServerOptions{
		OnConnect: func(conn *Conn) {
			_ = conn.NotifyWithTimeout("item_changed", itemChangedNotification{ID: "hello", Name: "from on connect"}, time.Second)
		},
	})
	defer shutdown()

	notifications := make(chan itemChangedNotification, 1)
	client := NewTCPClient(address, "server-notify-connect-client", ClientOptions{
		PingInterval: -1,
	})
	MustRegisterNotify(client, "item_changed", func(_ *Context, notification itemChangedNotification) error {
		notifications <- notification
		return nil
	})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	select {
	case notification := <-notifications:
		if notification.ID != "hello" || notification.Name != "from on connect" {
			t.Fatalf("notification = %+v", notification)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not receive on-connect notification")
	}
}

func TestBidirectionalRequestFromServerHandler(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegister(server, "get_an_item", func(ctx *Context, req getItemRequest) (getItemResponse, error) {
		if ctx.Conn() == nil {
			return getItemResponse{}, errors.New("server request context did not expose the connection")
		}

		var clientResp clientInfoResponse
		if err := ctx.CallWithTimeout("client_info", clientInfoRequest{Prefix: req.ID}, &clientResp, time.Second); err != nil {
			return getItemResponse{}, err
		}

		return getItemResponse{ID: req.ID, Name: clientResp.Message}, nil
	})

	client := NewTCPClient(address, "bidirectional-handler-client", ClientOptions{
		PingInterval: -1,
	})
	MustRegister(client, "client_info", func(ctx *Context, req clientInfoRequest) (clientInfoResponse, error) {
		if ctx.RequestID() == 0 {
			return clientInfoResponse{}, errors.New("client handler request ID was not set")
		}
		if ctx.Function() != "client_info" {
			return clientInfoResponse{}, errors.New("client handler function was not set")
		}
		if ctx.RemoteAddr() == nil {
			return clientInfoResponse{}, errors.New("client handler remote addr was not set")
		}

		return clientInfoResponse{Message: req.Prefix + "-from-client"}, nil
	})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	var resp getItemResponse
	if err := client.Call("get_an_item", getItemRequest{ID: "abc123"}, &resp); err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.ID != "abc123" || resp.Name != "abc123-from-client" {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestServerCanInitiateRequestAfterConnect(t *testing.T) {
	results := make(chan clientInfoResponse, 1)
	errs := make(chan error, 1)

	_, address, shutdown := startTestServerWithOptions(t, ServerOptions{
		OnConnect: func(conn *Conn) {
			var resp clientInfoResponse
			if err := conn.CallWithTimeout("client_info", clientInfoRequest{Prefix: "hello"}, &resp, time.Second); err != nil {
				errs <- err
				return
			}
			results <- resp
		},
	})
	defer shutdown()

	client := NewTCPClient(address, "on-connect-client", ClientOptions{
		PingInterval: -1,
	})
	MustRegister(client, "client_info", func(_ *Context, req clientInfoRequest) (clientInfoResponse, error) {
		return clientInfoResponse{Message: req.Prefix + "-from-on-connect-client"}, nil
	})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	select {
	case err := <-errs:
		t.Fatalf("server initiated call: %v", err)
	case resp := <-results:
		if resp.Message != "hello-from-on-connect-client" {
			t.Fatalf("resp = %+v", resp)
		}
	case <-time.After(time.Second):
		t.Fatal("server initiated call did not complete")
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

func TestServerStreamClientToServer(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegisterServerStream(server, "list_items", func(ctx *Context, req streamListRequest, stream *StreamWriter[streamItem]) error {
		if !ctx.IsStream() {
			return errors.New("context did not mark stream")
		}
		if ctx.StreamKind() != StreamKindServer {
			return fmt.Errorf("stream kind = %s", ctx.StreamKind().String())
		}

		for i := 1; i <= req.Count; i++ {
			if err := stream.Send(streamItem{Value: fmt.Sprintf("%s-%d", req.Prefix, i)}); err != nil {
				return err
			}
		}

		return nil
	})

	client, err := TCPDial(address, "server-stream-test-client", ClientOptions{
		PingInterval: -1,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	reader, err := ServerStream[streamListRequest, streamItem](context.Background(), client, "list_items", streamListRequest{
		Prefix: "widget",
		Count:  3,
	})
	if err != nil {
		t.Fatalf("server stream: %v", err)
	}

	var values []string
	for {
		item, err := reader.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		values = append(values, item.Value)
	}

	if strings.Join(values, ",") != "widget-1,widget-2,widget-3" {
		t.Fatalf("values = %v", values)
	}
}

func TestClientStreamClientToServer(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegisterClientStream(server, "upload_items", func(ctx *Context, reader *StreamReader[streamItem]) (streamSummary, error) {
		if !ctx.IsStream() || ctx.StreamKind() != StreamKindClient {
			return streamSummary{}, errors.New("context did not mark client stream")
		}

		var values []string
		for {
			item, err := reader.Recv()
			if errors.Is(err, io.EOF) {
				return streamSummary{
					Count:  len(values),
					Joined: strings.Join(values, ","),
				}, nil
			}
			if err != nil {
				return streamSummary{}, err
			}
			values = append(values, item.Value)
		}
	})

	client, err := TCPDial(address, "client-stream-test-client", ClientOptions{
		PingInterval: -1,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	stream, err := ClientStream[streamItem, streamSummary](context.Background(), client, "upload_items")
	if err != nil {
		t.Fatalf("client stream: %v", err)
	}
	for _, value := range []string{"alpha", "bravo", "charlie"} {
		if err := stream.Send(streamItem{Value: value}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("close and recv: %v", err)
	}
	if resp.Count != 3 || resp.Joined != "alpha,bravo,charlie" {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestBidiStreamClientToServer(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	MustRegisterBidiStream(server, "echo_items", func(ctx *Context, stream *BidiStreamHandle[streamItem, streamItem]) error {
		if !ctx.IsStream() || ctx.StreamKind() != StreamKindBidi {
			return errors.New("context did not mark bidi stream")
		}

		for {
			item, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := stream.Send(streamItem{Value: strings.ToUpper(item.Value)}); err != nil {
				return err
			}
		}
	})

	client, err := TCPDial(address, "bidi-stream-test-client", ClientOptions{
		PingInterval: -1,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	stream, err := BidiStream[streamItem, streamItem](context.Background(), client, "echo_items")
	if err != nil {
		t.Fatalf("bidi stream: %v", err)
	}
	for _, value := range []string{"alpha", "bravo"} {
		if err := stream.Send(streamItem{Value: value}); err != nil {
			t.Fatalf("send: %v", err)
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if resp.Value != strings.ToUpper(value) {
			t.Fatalf("resp = %+v", resp)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("final recv err = %v, want EOF", err)
	}
}

func TestServerCanOpenServerStreamToClient(t *testing.T) {
	results := make(chan []string, 1)
	errs := make(chan error, 1)

	_, address, shutdown := startTestServerWithOptions(t, ServerOptions{
		OnConnect: func(conn *Conn) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			reader, err := ServerStream[streamListRequest, streamItem](ctx, conn, "client_list_items", streamListRequest{
				Prefix: "client",
				Count:  2,
			})
			if err != nil {
				errs <- err
				return
			}

			var values []string
			for {
				item, err := reader.Recv()
				if errors.Is(err, io.EOF) {
					results <- values
					return
				}
				if err != nil {
					errs <- err
					return
				}
				values = append(values, item.Value)
			}
		},
	})
	defer shutdown()

	client := NewTCPClient(address, "server-open-server-stream-client", ClientOptions{
		PingInterval: -1,
	})
	MustRegisterServerStream(client, "client_list_items", func(_ *Context, req streamListRequest, stream *StreamWriter[streamItem]) error {
		for i := 1; i <= req.Count; i++ {
			if err := stream.Send(streamItem{Value: fmt.Sprintf("%s-%d", req.Prefix, i)}); err != nil {
				return err
			}
		}
		return nil
	})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	select {
	case err := <-errs:
		t.Fatalf("server opened stream: %v", err)
	case values := <-results:
		if strings.Join(values, ",") != "client-1,client-2" {
			t.Fatalf("values = %v", values)
		}
	case <-time.After(time.Second):
		t.Fatal("server initiated stream did not complete")
	}
}

func TestServerCanOpenClientStreamToClient(t *testing.T) {
	results := make(chan streamSummary, 1)
	errs := make(chan error, 1)

	_, address, shutdown := startTestServerWithOptions(t, ServerOptions{
		OnConnect: func(conn *Conn) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			stream, err := ClientStream[streamItem, streamSummary](ctx, conn, "client_upload_items")
			if err != nil {
				errs <- err
				return
			}
			for _, value := range []string{"one", "two"} {
				if err := stream.Send(streamItem{Value: value}); err != nil {
					errs <- err
					return
				}
			}
			resp, err := stream.CloseAndRecv()
			if err != nil {
				errs <- err
				return
			}
			results <- resp
		},
	})
	defer shutdown()

	client := NewTCPClient(address, "server-open-client-stream-client", ClientOptions{
		PingInterval: -1,
	})
	MustRegisterClientStream(client, "client_upload_items", func(_ *Context, reader *StreamReader[streamItem]) (streamSummary, error) {
		var values []string
		for {
			item, err := reader.Recv()
			if errors.Is(err, io.EOF) {
				return streamSummary{Count: len(values), Joined: strings.Join(values, ",")}, nil
			}
			if err != nil {
				return streamSummary{}, err
			}
			values = append(values, item.Value)
		}
	})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	select {
	case err := <-errs:
		t.Fatalf("server opened client stream: %v", err)
	case resp := <-results:
		if resp.Count != 2 || resp.Joined != "one,two" {
			t.Fatalf("resp = %+v", resp)
		}
	case <-time.After(time.Second):
		t.Fatal("server initiated client stream did not complete")
	}
}

func TestServerCanOpenBidiStreamToClient(t *testing.T) {
	results := make(chan []string, 1)
	errs := make(chan error, 1)

	_, address, shutdown := startTestServerWithOptions(t, ServerOptions{
		OnConnect: func(conn *Conn) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			stream, err := BidiStream[streamItem, streamItem](ctx, conn, "client_echo_items")
			if err != nil {
				errs <- err
				return
			}

			var values []string
			for _, value := range []string{"red", "blue"} {
				if err := stream.Send(streamItem{Value: value}); err != nil {
					errs <- err
					return
				}
				item, err := stream.Recv()
				if err != nil {
					errs <- err
					return
				}
				values = append(values, item.Value)
			}
			if err := stream.CloseSend(); err != nil {
				errs <- err
				return
			}
			if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
				errs <- err
				return
			}
			results <- values
		},
	})
	defer shutdown()

	client := NewTCPClient(address, "server-open-bidi-stream-client", ClientOptions{
		PingInterval: -1,
	})
	MustRegisterBidiStream(client, "client_echo_items", func(_ *Context, stream *BidiStreamHandle[streamItem, streamItem]) error {
		for {
			item, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := stream.Send(streamItem{Value: item.Value + "-client"}); err != nil {
				return err
			}
		}
	})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	select {
	case err := <-errs:
		t.Fatalf("server opened bidi stream: %v", err)
	case values := <-results:
		if strings.Join(values, ",") != "red-client,blue-client" {
			t.Fatalf("values = %v", values)
		}
	case <-time.After(time.Second):
		t.Fatal("server initiated bidi stream did not complete")
	}
}

func TestStreamFailsOnConnectionLossAndClientReconnects(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := ln.Addr().String()

	server := NewServer(ServerOptions{})
	MustRegisterServerStream(server, "slow_items", func(ctx *Context, _ streamListRequest, stream *StreamWriter[streamItem]) error {
		if err := stream.Send(streamItem{Value: "first"}); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeListener(ln)
	}()
	shutdownFirst := func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			t.Fatalf("shutdown first: %v", err)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("serve first: %v", err)
		}
	}

	client, err := Dial(context.Background(), "tcp", address, reconnectTestOptions())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	reader, err := ServerStream[streamListRequest, streamItem](context.Background(), client, "slow_items", streamListRequest{})
	if err != nil {
		t.Fatalf("server stream: %v", err)
	}
	item, err := reader.Recv()
	if err != nil {
		t.Fatalf("first recv: %v", err)
	}
	if item.Value != "first" {
		t.Fatalf("item = %+v", item)
	}

	shutdownFirst()

	streamErr := make(chan error, 1)
	go func() {
		_, err := reader.Recv()
		streamErr <- err
	}()

	select {
	case err := <-streamErr:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("stream err = %v, want ErrUnavailable", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not fail after connection loss")
	}

	_, shutdownSecond := startInventoryServerAt(t, address, "after stream reconnect")
	defer shutdownSecond()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var resp getItemResponse
	if err := client.CallContext(ctx, "get_an_item", getItemRequest{ID: "reconnected"}, &resp); err != nil {
		t.Fatalf("call after reconnect: %v", err)
	}
	if resp.Name != "after stream reconnect" {
		t.Fatalf("resp = %+v", resp)
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

func TestCompressionRoundTrip(t *testing.T) {
	server, address, shutdown := startTestServerWithOptions(t, ServerOptions{
		Compression: GzipCompression(),
	})
	defer shutdown()

	MustRegister(server, "get_an_item", func(_ *Context, req getItemRequest) (getItemResponse, error) {
		return getItemResponse{
			ID:   req.ID,
			Name: strings.Repeat("compressed-widget-", 256),
		}, nil
	})

	client, err := TCPDial(address, "compression-test-client", ClientOptions{
		Compression:  GzipCompression(),
		PingInterval: -1,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	var resp getItemResponse
	if err := client.Call("get_an_item", getItemRequest{ID: strings.Repeat("abc123", 128)}, &resp); err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.ID == "" || !strings.Contains(resp.Name, "compressed-widget") {
		t.Fatalf("resp = %+v", resp)
	}

	plainClient, err := TCPDial(address, "plain-test-client", ClientOptions{
		PingInterval: -1,
	})
	if err != nil {
		t.Fatalf("plain dial: %v", err)
	}
	defer func() {
		_ = plainClient.Close()
	}()

	var plainResp getItemResponse
	if err := plainClient.Call("get_an_item", getItemRequest{ID: "plain"}, &plainResp); err != nil {
		t.Fatalf("plain call: %v", err)
	}
	if plainResp.ID != "plain" {
		t.Fatalf("plain resp = %+v", plainResp)
	}
}

func TestInterceptorsWrapInboundHandlers(t *testing.T) {
	events := make(chan string, 3)
	server, address, shutdown := startTestServerWithOptions(t, ServerOptions{
		UnaryInterceptor: func(ctx *Context, req UnaryRequest, next UnaryHandler) ([]byte, error) {
			if len(req.Payload) == 0 {
				return nil, errors.New("unary interceptor saw empty payload")
			}
			events <- "unary:" + ctx.Function()
			return next(ctx, req)
		},
		NotifyInterceptor: func(ctx *Context, req NotifyRequest, next NotifyHandler) error {
			if len(req.Payload) == 0 {
				return errors.New("notify interceptor saw empty payload")
			}
			events <- "notify:" + ctx.Function()
			return next(ctx, req)
		},
		StreamInterceptor: func(ctx *Context, req StreamRequest, stream *Stream, next StreamHandler) ([]byte, error) {
			if req.Kind != StreamKindServer {
				return nil, fmt.Errorf("stream kind = %s", req.Kind.String())
			}
			if len(req.Payload) == 0 {
				return nil, errors.New("stream interceptor saw empty payload")
			}
			events <- "stream:" + ctx.Function()
			return next(ctx, req, stream)
		},
	})
	defer shutdown()

	MustRegister(server, "get_an_item", func(_ *Context, req getItemRequest) (getItemResponse, error) {
		return getItemResponse{ID: req.ID, Name: "intercepted"}, nil
	})
	MustRegisterNotify(server, "item_changed", func(_ *Context, _ itemChangedNotification) error {
		return nil
	})
	MustRegisterServerStream(server, "stream_items", func(_ *Context, req streamListRequest, stream *StreamWriter[streamItem]) error {
		return stream.Send(streamItem{Value: req.Prefix + "-0"})
	})

	client, err := TCPDial(address, "interceptor-test-client", ClientOptions{
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
	if err := client.Notify("item_changed", itemChangedNotification{ID: "abc123"}); err != nil {
		t.Fatalf("notify: %v", err)
	}
	reader, err := ServerStream[streamListRequest, streamItem](context.Background(), client, "stream_items", streamListRequest{Prefix: "x", Count: 1})
	if err != nil {
		t.Fatalf("server stream: %v", err)
	}
	item, err := reader.Recv()
	if err != nil {
		t.Fatalf("stream recv: %v", err)
	}
	if item.Value != "x-0" {
		t.Fatalf("stream item = %+v", item)
	}
	if _, err := reader.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("stream end err = %v, want io.EOF", err)
	}

	want := map[string]bool{
		"unary:get_an_item":   true,
		"notify:item_changed": true,
		"stream:stream_items": true,
	}
	for i := 0; i < 3; i++ {
		select {
		case event := <-events:
			if !want[event] {
				t.Fatalf("unexpected interceptor event %q", event)
			}
			delete(want, event)
		case <-time.After(time.Second):
			t.Fatalf("missing interceptor events: %+v", want)
		}
	}
}

func TestCallSingleflightCollapsesConcurrentCalls(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	MustRegister(server, "get_an_item", func(_ *Context, req getItemRequest) (getItemResponse, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return getItemResponse{ID: req.ID, Name: "singleflight"}, nil
	})

	client, err := TCPDial(address, "singleflight-test-client", ClientOptions{
		PingInterval: -1,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	var first getItemResponse
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- client.CallSingleflight("get_an_item", "same-key", getItemRequest{ID: "abc123"}, &first)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first singleflight call did not start")
	}

	var second getItemResponse
	secondErr := make(chan error, 1)
	go func() {
		secondErr <- client.CallSingleflight("get_an_item", "same-key", getItemRequest{ID: "abc123"}, &second)
	}()

	time.Sleep(25 * time.Millisecond)
	close(release)

	for name, errCh := range map[string]chan error{
		"first":  firstErr,
		"second": secondErr,
	} {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("%s call: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s call did not finish", name)
		}
	}
	if first.Name != "singleflight" || second.Name != "singleflight" {
		t.Fatalf("responses = %+v %+v", first, second)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestBackpressureRejectsTooManyPendingCalls(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	started := make(chan struct{})
	release := make(chan struct{})
	MustRegister(server, "get_an_item", func(_ *Context, req getItemRequest) (getItemResponse, error) {
		if req.ID == "hold" {
			close(started)
			<-release
		}
		return getItemResponse{ID: req.ID, Name: "ok"}, nil
	})

	backpressureEvents := make(chan BackpressureInfo, 1)
	client, err := TCPDial(address, "backpressure-test-client", ClientOptions{
		PingInterval: -1,
		Backpressure: BackpressureOptions{
			MaxPendingCalls: 1,
			OnBackpressure: func(info BackpressureInfo) {
				backpressureEvents <- info
			},
		},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	firstErr := make(chan error, 1)
	go func() {
		var resp getItemResponse
		firstErr <- client.Call("get_an_item", getItemRequest{ID: "hold"}, &resp)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first call did not start")
	}

	var resp getItemResponse
	err = client.Call("get_an_item", getItemRequest{ID: "rejected"}, &resp)
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("err = %v, want ErrBackpressure", err)
	}

	select {
	case event := <-backpressureEvents:
		if event.Side != BackpressureSideClient || event.Reason != BackpressureReasonPendingCalls {
			t.Fatalf("backpressure event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("backpressure callback was not called")
	}

	close(release)
	select {
	case err := <-firstErr:
		if err != nil {
			t.Fatalf("first call: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first call did not finish")
	}
}

func TestServerNotifyAllBroadcastsToConnections(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	defer shutdown()

	received := make(chan string, 2)
	newClient := func(name string) *Client {
		t.Helper()

		client := NewTCPClient(address, name, ClientOptions{
			PingInterval: -1,
		})
		MustRegisterNotify(client, "item_changed", func(_ *Context, notification itemChangedNotification) error {
			received <- notification.ID
			return nil
		})
		if err := client.Connect(context.Background()); err != nil {
			t.Fatalf("connect %s: %v", name, err)
		}

		return client
	}

	first := newClient("broadcast-client-1")
	defer func() {
		_ = first.Close()
	}()
	second := newClient("broadcast-client-2")
	defer func() {
		_ = second.Close()
	}()

	result := server.NotifyAll("item_changed", itemChangedNotification{ID: "broadcast-1"})
	if !result.OK() || result.Total != 2 || result.Sent != 2 || result.Failed != 0 {
		t.Fatalf("broadcast result = %+v", result)
	}

	for i := 0; i < 2; i++ {
		select {
		case id := <-received:
			if id != "broadcast-1" {
				t.Fatalf("notification id = %q", id)
			}
		case <-time.After(time.Second):
			t.Fatal("broadcast notification was not received")
		}
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
