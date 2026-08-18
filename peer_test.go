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

type peerConnectionGenerationRequest struct{}

type peerConnectionGenerationResponse struct {
	ContextGeneration uint64
	ConnGeneration    uint64
	HasConn           bool
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

func TestPeerConnectionGenerationTracksAutomaticReconnect(t *testing.T) {
	managerA := NewPeerManager("a")
	managerB := NewPeerManager("b")
	t.Cleanup(func() { _ = managerA.Close() })
	t.Cleanup(func() { _ = managerB.Close() })

	serverB, addressB := startPeerTestServer(t, managerB, "b.echo")
	MustRegister(serverB, "b.connection_generation", func(ctx *Context, _ peerConnectionGenerationRequest) (peerConnectionGenerationResponse, error) {
		response := peerConnectionGenerationResponse{
			ContextGeneration: ctx.ConnectionGeneration(),
			HasConn:           ctx.Conn() != nil,
		}
		if ctx.Conn() != nil {
			response.ConnGeneration = ctx.Conn().ConnectionGeneration()
		}
		return response, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientA, err := managerA.Dial(ctx, PeerDialOptions{
		PeerName: "b",
		Network:  "tcp",
		Address:  addressB,
		RegisterHandlers: func(client *Client) error {
			if err := Register(client, "a.connection_generation", func(ctx *Context, _ peerConnectionGenerationRequest) (peerConnectionGenerationResponse, error) {
				return peerConnectionGenerationResponse{
					ContextGeneration: ctx.ConnectionGeneration(),
					HasConn:           ctx.Conn() != nil,
				}, nil
			}); err != nil {
				return err
			}
			return Register(client, "a.echo", func(_ *Context, req peerEchoRequest) (peerEchoResponse, error) {
				return peerEchoResponse(req), nil
			})
		},
	})
	if err != nil {
		t.Fatalf("dial a to b: %v", err)
	}
	t.Cleanup(func() { _ = clientA.Close() })

	logicalPeer := clientA.Peer()
	if logicalPeer == nil {
		t.Fatal("managed client did not expose its logical peer")
	}
	statusBefore := clientA.Status()
	if !statusBefore.Active || statusBefore.Direction != PeerDirectionOutbound || statusBefore.ConnectionGeneration == 0 {
		t.Fatalf("initial peer status = %+v, want active outbound connection with a generation", statusBefore)
	}

	acceptedBefore := waitForAcceptedPeerConnection(t, serverB, nil)
	if acceptedBefore.ConnectionGeneration() == statusBefore.ConnectionGeneration {
		t.Fatalf("accepted and dialing connections shared process generation %d", acceptedBefore.ConnectionGeneration())
	}
	inboundLogicalPeer, ok := managerB.Peer("a")
	if !ok {
		t.Fatal("accepting manager did not expose its logical peer")
	}
	statusInboundBefore := inboundLogicalPeer.Status()
	if !statusInboundBefore.Active || statusInboundBefore.Direction != PeerDirectionInbound || statusInboundBefore.ConnectionGeneration != acceptedBefore.ConnectionGeneration() {
		t.Fatalf("initial inbound peer status = %+v, want accepted generation %d", statusInboundBefore, acceptedBefore.ConnectionGeneration())
	}
	outboundEndpointBefore := requirePeerEndpoint(t, logicalPeer, statusBefore.ConnectionGeneration)
	inboundEndpointBefore := requirePeerEndpoint(t, inboundLogicalPeer, statusInboundBefore.ConnectionGeneration)
	if _, ok := logicalPeer.EndpointForGeneration(0); ok {
		t.Fatal("peer returned an endpoint for zero generation")
	}
	if _, ok := logicalPeer.EndpointForGeneration(statusInboundBefore.ConnectionGeneration); ok {
		t.Fatal("outbound peer returned an endpoint for the accepting side's generation")
	}
	assertPeerCall(t, outboundEndpointBefore, "b.echo", "outbound-g1")
	assertPeerCall(t, inboundEndpointBefore, "a.echo", "inbound-g1")
	assertAcceptedConnectionGeneration(t, clientA, acceptedBefore)
	assertReverseConnectionGeneration(t, acceptedBefore, statusBefore.ConnectionGeneration)

	if err := acceptedBefore.Close(); err != nil {
		t.Fatalf("close first physical connection: %v", err)
	}

	var acceptedAfter *Conn
	waitForPeerTest(t, func() bool {
		status := clientA.Status()
		if !status.Active || status.ConnectionGeneration == 0 || status.ConnectionGeneration == statusBefore.ConnectionGeneration {
			return false
		}
		for _, conn := range serverB.Connections() {
			if conn != acceptedBefore {
				acceptedAfter = conn
				return true
			}
		}
		return false
	})

	if clientA.Peer() != logicalPeer {
		t.Fatal("automatic reconnect replaced the logical peer")
	}
	statusAfter := clientA.Status()
	if statusAfter.Direction != PeerDirectionOutbound {
		t.Fatalf("reconnected peer direction = %q, want outbound", statusAfter.Direction)
	}
	if statusAfter.ConnectionGeneration == statusBefore.ConnectionGeneration {
		t.Fatalf("connection generation remained %d across physical reconnect", statusAfter.ConnectionGeneration)
	}
	if acceptedAfter.ConnectionGeneration() == acceptedBefore.ConnectionGeneration() {
		t.Fatalf("accepted connection generation remained %d across physical reconnect", acceptedAfter.ConnectionGeneration())
	}
	if acceptedAfter.ConnectionGeneration() == statusAfter.ConnectionGeneration {
		t.Fatalf("reconnected accepted and dialing connections shared process generation %d", acceptedAfter.ConnectionGeneration())
	}
	if currentInboundPeer, ok := managerB.Peer("a"); !ok || currentInboundPeer != inboundLogicalPeer {
		t.Fatal("automatic reconnect replaced the accepting side's logical peer")
	}
	statusInboundAfter := inboundLogicalPeer.Status()
	if !statusInboundAfter.Active || statusInboundAfter.Direction != PeerDirectionInbound || statusInboundAfter.ConnectionGeneration != acceptedAfter.ConnectionGeneration() {
		t.Fatalf("reconnected inbound peer status = %+v, want accepted generation %d", statusInboundAfter, acceptedAfter.ConnectionGeneration())
	}
	if _, ok := logicalPeer.EndpointForGeneration(statusBefore.ConnectionGeneration); ok {
		t.Fatal("outbound peer rebound the old generation to the replacement connection")
	}
	if _, ok := inboundLogicalPeer.EndpointForGeneration(statusInboundBefore.ConnectionGeneration); ok {
		t.Fatal("inbound peer rebound the old generation to the replacement connection")
	}
	assertPeerEndpointUnavailable(t, outboundEndpointBefore, "b.echo")
	assertPeerEndpointUnavailable(t, inboundEndpointBefore, "a.echo")
	assertPeerEndpointNotifyUnavailable(t, outboundEndpointBefore, "b.stale")
	assertPeerEndpointNotifyUnavailable(t, inboundEndpointBefore, "a.stale")

	outboundEndpointAfter := requirePeerEndpoint(t, logicalPeer, statusAfter.ConnectionGeneration)
	inboundEndpointAfter := requirePeerEndpoint(t, inboundLogicalPeer, statusInboundAfter.ConnectionGeneration)
	assertPeerCall(t, outboundEndpointAfter, "b.echo", "outbound-g2")
	assertPeerCall(t, inboundEndpointAfter, "a.echo", "inbound-g2")
	assertAcceptedConnectionGeneration(t, clientA, acceptedAfter)
	assertReverseConnectionGeneration(t, acceptedAfter, statusAfter.ConnectionGeneration)
}

func requirePeerEndpoint(t *testing.T, peer *Peer, generation uint64) *PeerEndpoint {
	t.Helper()
	endpoint, ok := peer.EndpointForGeneration(generation)
	if !ok {
		t.Fatalf("peer did not return endpoint for current generation %d", generation)
	}
	if endpoint.ConnectionGeneration() != generation {
		t.Fatalf("endpoint generation = %d, want %d", endpoint.ConnectionGeneration(), generation)
	}
	return endpoint
}

func assertPeerEndpointUnavailable(t *testing.T, endpoint *PeerEndpoint, function string) {
	t.Helper()
	var response peerEchoResponse
	err := endpoint.CallWithTimeout(function, peerEchoRequest{Value: "stale"}, &response, time.Second)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("stale endpoint call error = %v, want ErrUnavailable", err)
	}
}

func assertPeerEndpointNotifyUnavailable(t *testing.T, endpoint *PeerEndpoint, function string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := endpoint.NotifyContext(ctx, function, peerEchoRequest{Value: "stale"})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("stale endpoint notify error = %v, want ErrUnavailable", err)
	}
}

func waitForAcceptedPeerConnection(t *testing.T, server *Server, previous *Conn) *Conn {
	t.Helper()
	var accepted *Conn
	waitForPeerTest(t, func() bool {
		for _, conn := range server.Connections() {
			if conn != previous {
				accepted = conn
				return true
			}
		}
		return false
	})
	return accepted
}

func assertAcceptedConnectionGeneration(t *testing.T, client *PeerClient, accepted *Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var response peerConnectionGenerationResponse
	if err := client.CallContext(ctx, "b.connection_generation", peerConnectionGenerationRequest{}, &response); err != nil {
		t.Fatalf("call accepted connection generation: %v", err)
	}
	if !response.HasConn {
		t.Fatal("server-side handler context did not expose its accepted connection")
	}
	if response.ContextGeneration == 0 || response.ContextGeneration != accepted.ConnectionGeneration() || response.ConnGeneration != accepted.ConnectionGeneration() {
		t.Fatalf("accepted generations: response=%+v conn=%d", response, accepted.ConnectionGeneration())
	}
}

func assertReverseConnectionGeneration(t *testing.T, accepted *Conn, want uint64) {
	t.Helper()
	var response peerConnectionGenerationResponse
	if err := accepted.CallWithTimeout("a.connection_generation", peerConnectionGenerationRequest{}, &response, time.Second); err != nil {
		t.Fatalf("call reverse connection generation: %v", err)
	}
	if response.HasConn {
		t.Fatal("client-side reverse handler unexpectedly exposed an accepted connection")
	}
	if response.ContextGeneration == 0 || response.ContextGeneration != want {
		t.Fatalf("reverse context generation = %d, want peer generation %d", response.ContextGeneration, want)
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
