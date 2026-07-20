package gorpc

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type peerEchoRequest struct {
	Value string
}

type peerEchoResponse struct {
	Value string
}

func TestPeerManagerReusesEstablishedFullDuplexConnection(t *testing.T) {
	managerA := NewPeerManager("a")
	managerB := NewPeerManager("b")
	t.Cleanup(func() { _ = managerA.Close() })
	t.Cleanup(func() { _ = managerB.Close() })

	serverA, addressA := startPeerTestServer(t, managerA, "a.echo")
	serverB, addressB := startPeerTestServer(t, managerB, "b.echo")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	clientA, err := managerA.Dial(ctx, peerTestDialOptions("b", addressB, "a.echo"))
	if err != nil {
		t.Fatalf("dial a to b: %v", err)
	}
	t.Cleanup(func() { _ = clientA.Close() })

	clientB, err := managerB.Dial(ctx, peerTestDialOptions("a", addressA, "b.echo"))
	if err != nil {
		t.Fatalf("reuse b to a: %v", err)
	}
	t.Cleanup(func() { _ = clientB.Close() })

	assertPeerCall(t, clientA, "b.echo", "from-a")
	assertPeerCall(t, clientB, "a.echo", "from-b")

	if got := len(serverA.Connections()); got != 0 {
		t.Fatalf("server a accepted %d connections, want 0", got)
	}
	if got := len(serverB.Connections()); got != 1 {
		t.Fatalf("server b accepted %d connections, want 1", got)
	}
}

func TestPeerManagerCollapsesConcurrentDials(t *testing.T) {
	managerA := NewPeerManager("a")
	managerB := NewPeerManager("b")
	t.Cleanup(func() { _ = managerA.Close() })
	t.Cleanup(func() { _ = managerB.Close() })

	serverA, addressA := startPeerTestServer(t, managerA, "a.echo")
	serverB, addressB := startPeerTestServer(t, managerB, "b.echo")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var clientA, clientB *PeerClient
	var errA, errB error
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		clientA, errA = managerA.Dial(ctx, peerTestDialOptions("b", addressB, "a.echo"))
	}()
	go func() {
		defer wg.Done()
		<-start
		clientB, errB = managerB.Dial(ctx, peerTestDialOptions("a", addressA, "b.echo"))
	}()
	close(start)
	wg.Wait()
	if errA != nil {
		t.Fatalf("dial a to b: %v", errA)
	}
	if errB != nil {
		t.Fatalf("dial b to a: %v", errB)
	}
	t.Cleanup(func() { _ = clientA.Close() })
	t.Cleanup(func() { _ = clientB.Close() })

	waitForPeerTest(t, func() bool {
		return len(serverA.Connections())+len(serverB.Connections()) == 1
	})
	assertPeerCall(t, clientA, "b.echo", "from-a")
	assertPeerCall(t, clientB, "a.echo", "from-b")

	statusA := clientA.Status()
	statusB := clientB.Status()
	if !statusA.Active || !statusB.Active {
		t.Fatalf("both logical peers must be active: a=%+v b=%+v", statusA, statusB)
	}
	if statusA.Direction == statusB.Direction {
		t.Fatalf("physical connection directions must be opposite: a=%s b=%s", statusA.Direction, statusB.Direction)
	}
}

func TestPeerManagerCollapsesAuthenticatedConcurrentDials(t *testing.T) {
	managerA := NewPeerManager("a")
	managerB := NewPeerManager("b")
	t.Cleanup(func() { _ = managerA.Close() })
	t.Cleanup(func() { _ = managerB.Close() })
	auth := SharedSecret("peer-manager-test-secret")

	serverA, addressA := startPeerTestServerWithOptions(t, managerA, "a.echo", ServerOptions{Auth: auth})
	serverB, addressB := startPeerTestServerWithOptions(t, managerB, "b.echo", ServerOptions{Auth: auth})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	optionsA := peerTestDialOptions("b", addressB, "a.echo")
	optionsA.ClientOptions.Auth = auth
	optionsB := peerTestDialOptions("a", addressA, "b.echo")
	optionsB.ClientOptions.Auth = auth
	var clientA, clientB *PeerClient
	var errA, errB error
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		clientA, errA = managerA.Dial(ctx, optionsA)
	}()
	go func() {
		defer wg.Done()
		<-start
		clientB, errB = managerB.Dial(ctx, optionsB)
	}()
	close(start)
	wg.Wait()
	if errA != nil || errB != nil {
		t.Fatalf("authenticated simultaneous dial errors: a=%v b=%v", errA, errB)
	}
	t.Cleanup(func() { _ = clientA.Close() })
	t.Cleanup(func() { _ = clientB.Close() })
	waitForPeerTest(t, func() bool {
		return len(serverA.Connections())+len(serverB.Connections()) == 1
	})
	assertPeerCall(t, clientA, "b.echo", "authenticated-a")
	assertPeerCall(t, clientB, "a.echo", "authenticated-b")
}

func TestPeerManagerSharesConcurrentSameSideDials(t *testing.T) {
	managerA := NewPeerManager("a")
	managerB := NewPeerManager("b")
	t.Cleanup(func() { _ = managerA.Close() })
	t.Cleanup(func() { _ = managerB.Close() })

	_, addressA := startPeerTestServer(t, managerA, "a.echo")
	_ = addressA
	serverB, addressB := startPeerTestServer(t, managerB, "b.echo")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const callers = 20
	clients := make([]*PeerClient, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			clients[index], errs[index] = managerA.Dial(ctx, peerTestDialOptions("b", addressB, "a.echo"))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
	}
	t.Cleanup(func() {
		for _, client := range clients {
			_ = client.Close()
		}
	})
	if got := len(serverB.Connections()); got != 1 {
		t.Fatalf("server b accepted %d connections, want 1", got)
	}
}

func TestPeerManagerRejectsLateDuplicate(t *testing.T) {
	managerA := NewPeerManager("a")
	managerB := NewPeerManager("b")
	t.Cleanup(func() { _ = managerA.Close() })
	t.Cleanup(func() { _ = managerB.Close() })

	serverA, addressA := startPeerTestServer(t, managerA, "a.echo")
	_, addressB := startPeerTestServer(t, managerB, "b.echo")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	clientA, err := managerA.Dial(ctx, peerTestDialOptions("b", addressB, "a.echo"))
	if err != nil {
		t.Fatalf("dial a to b: %v", err)
	}
	t.Cleanup(func() { _ = clientA.Close() })

	duplicate, err := Dial(ctx, "tcp", addressA, ClientOptions{ClientName: "b"})
	if duplicate != nil {
		_ = duplicate.Close()
	}
	if !errors.Is(err, ErrPeerConnected) {
		t.Fatalf("late duplicate error = %v, want ErrPeerConnected", err)
	}
	if got := len(serverA.Connections()); got != 0 {
		t.Fatalf("server a accepted %d duplicate connections, want 0", got)
	}
}

func TestPeerManagerDoesNotExposeInboundBeforeHandshakeCompletes(t *testing.T) {
	manager := NewPeerManager("a")
	t.Cleanup(func() { _ = manager.Close() })

	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { _ = clientSide.Close() })
	server := NewServer(ServerOptions{PeerManager: manager})
	conn := newConn(server, serverSide)
	conn.clientName = "b"
	if err := manager.acceptInbound(conn); err != nil {
		t.Fatalf("reserve inbound: %v", err)
	}
	peer, ok := manager.Peer("b")
	if !ok {
		t.Fatal("reserved peer was not recorded")
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancelWait()
	if err := peer.WaitReady(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("peer became ready before handshake completion: %v", err)
	}
	if peer.Status().Active {
		t.Fatal("reserved inbound peer reported active before handshake completion")
	}

	if err := manager.connected(conn); err != nil {
		t.Fatalf("activate inbound: %v", err)
	}
	readyCtx, cancelReady := context.WithTimeout(context.Background(), time.Second)
	defer cancelReady()
	if err := peer.WaitReady(readyCtx); err != nil {
		t.Fatalf("wait for activated peer: %v", err)
	}
	if !peer.Status().Active {
		t.Fatal("activated inbound peer did not report active")
	}
}

func startPeerTestServer(t *testing.T, manager *PeerManager, function string) (*Server, string) {
	return startPeerTestServerWithOptions(t, manager, function, ServerOptions{})
}

func startPeerTestServerWithOptions(t *testing.T, manager *PeerManager, function string, options ServerOptions) (*Server, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	options.PeerManager = manager
	server := NewServer(options)
	MustRegister(server, function, func(_ *Context, req peerEchoRequest) (peerEchoResponse, error) {
		return peerEchoResponse(req), nil
	})
	go func() { _ = server.ServeListener(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	return server, listener.Addr().String()
}

func peerTestDialOptions(peerName string, address string, localFunction string) PeerDialOptions {
	return PeerDialOptions{
		PeerName: peerName,
		Network:  "tcp",
		Address:  address,
		RegisterHandlers: func(client *Client) error {
			return Register(client, localFunction, func(_ *Context, req peerEchoRequest) (peerEchoResponse, error) {
				return peerEchoResponse(req), nil
			})
		},
	}
}

func assertPeerCall(t *testing.T, caller interface {
	CallContext(context.Context, string, any, any) error
}, function string, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var response peerEchoResponse
	if err := caller.CallContext(ctx, function, peerEchoRequest{Value: value}, &response); err != nil {
		t.Fatalf("call %s: %v", function, err)
	}
	if response.Value != value {
		t.Fatalf("response value = %q, want %q", response.Value, value)
	}
}

func waitForPeerTest(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
