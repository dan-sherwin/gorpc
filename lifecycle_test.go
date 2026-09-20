package gorpc

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectCanceledDuringHandshake(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		t.Run(map[bool]string{false: "context", true: "close"}[closeClient], func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ln.Close() })
			helloRead := make(chan struct{})
			peerDone := make(chan error, 1)
			go func() {
				conn, err := ln.Accept()
				if err != nil {
					peerDone <- err
					return
				}
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				_, err = readFrame(conn, DefaultMaxFrameSize, MessagePackCodec{})
				close(helloRead)
				if err == nil {
					_, err = readFrame(conn, DefaultMaxFrameSize, MessagePackCodec{})
				}
				peerDone <- err
			}()
			client := NewTCPClient(ln.Addr().String(), "closing", ClientOptions{HandshakeTimeout: -1, DialTimeout: -1})
			t.Cleanup(func() { _ = client.Close() })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			connected := make(chan error, 1)
			go func() { connected <- client.Connect(ctx) }()
			receiveTestValue(t, helloRead)
			if closeClient {
				_ = client.Close()
			} else {
				cancel()
			}
			if err := receiveTestValue(t, connected); !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
				t.Fatalf("Connect = %v", err)
			}
			if err := receiveTestValue(t, peerDone); err == nil {
				t.Fatal("handshake connection remained open")
			}
			if _, ok := client.currentConn(); ok {
				t.Fatal("canceled handshake published a connection")
			}
		})
	}
}

func TestDialTimeoutDoesNotLimitHandshake(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	peerDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			peerDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := readFrame(conn, DefaultMaxFrameSize, MessagePackCodec{}); err != nil {
			peerDone <- err
			return
		}
		time.Sleep(100 * time.Millisecond)
		server := NewServer(ServerOptions{})
		peerDone <- server.writeHelloAck(conn, helloAck{ProtocolVersion: ProtocolVersion, Codec: MessagePackCodec{}.Name()})
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	client := NewTCPClient(ln.Addr().String(), "slow-handshake", ClientOptions{
		DialTimeout: 50 * time.Millisecond, HandshakeTimeout: time.Second, PingInterval: -1,
	})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := receiveTestValue(t, peerDone); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentConnectUsesOneConnection(t *testing.T) {
	var accepted atomic.Int32
	_, address, shutdown := startTestServerWithOptions(t, ServerOptions{
		OnConnect: func(_ *Conn) { accepted.Add(1) },
	})
	defer shutdown()
	client := NewTCPClient(address, "concurrent-connect")
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if err := client.Connect(ctx); err != nil {
				t.Errorf("Connect: %v", err)
			}
		})
	}
	wg.Wait()
	waitForPeerTest(t, func() bool { return accepted.Load() > 0 })
	if got := accepted.Load(); got != 1 {
		t.Fatalf("accepted %d connections", got)
	}
}

func TestConcurrentConnectRespectsCallerContext(t *testing.T) {
	client := NewTCPClient("127.0.0.1:1", "waiting-connect")
	t.Cleanup(func() { _ = client.Close() })
	client.connectGate <- struct{}{}
	defer func() { <-client.connectGate }()
	ctx, cancel := context.WithCancel(t.Context())
	connected := make(chan error, 1)
	go func() { connected <- client.Connect(ctx) }()
	cancel()
	if err := receiveTestValue(t, connected); !errors.Is(err, context.Canceled) {
		t.Fatalf("Connect while another handshake is pending = %v", err)
	}
}

func TestShutdownClosesPendingHandshake(t *testing.T) {
	server, address, shutdown := startTestServerWithOptions(t, ServerOptions{HandshakeTimeout: -1})
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	waitForPeerTest(t, func() bool {
		server.connMu.Lock()
		defer server.connMu.Unlock()
		return len(server.conns) == 1
	})
	if len(server.Connections()) != 0 {
		t.Fatal("Connections exposed an unfinished handshake")
	}
	shutdown()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := conn.Read(b[:]); err == nil {
		t.Fatal("shutdown left handshake open")
	} else if e, ok := err.(net.Error); ok && e.Timeout() {
		t.Fatalf("handshake did not close: %v", err)
	}
}

func TestShutdownClosesAllListeners(t *testing.T) {
	server := NewServer(ServerOptions{})
	served := make(chan error, 2)
	for range 2 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		go func() { served <- server.ServeListener(ln) }()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		client, err := Dial(ctx, "tcp", ln.Addr().String(), ClientOptions{})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := receiveTestValue(t, served); err != nil {
			t.Fatal(err)
		}
	}
}

func TestServeListenerAfterShutdownClosesListener(t *testing.T) {
	server := NewServer(ServerOptions{})
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	if err := server.ServeListener(ln); !errors.Is(err, ErrClosed) {
		t.Fatalf("ServeListener = %v", err)
	}
	conn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("listener remained open after ServeListener returned")
	}
}

func TestShutdownWaitsForHandler(t *testing.T) {
	server, address, shutdown := startTestServer(t)
	t.Cleanup(shutdown)
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	MustRegister(server, "wait", func(ctx *Context, _ struct{}) (int, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return 0, ctx.Err()
	})
	client, err := TCPDial(address, "shutdown-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	called := make(chan error, 1)
	go func() {
		var response int
		called <- client.Call("wait", struct{}{}, &response)
	}()
	receiveTestValue(t, started)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- server.Shutdown(ctx) }()
	receiveTestValue(t, canceled)
	assertTestBlocked(t, stopped)
	close(release)
	if err := receiveTestValue(t, stopped); err != nil {
		t.Fatal(err)
	}
	if err := receiveTestValue(t, called); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("call = %v", err)
	}
}

func TestStreamCleanupDoesNotRemoveReplacement(t *testing.T) {
	client := NewTCPClient("", "cleanup")
	t.Cleanup(func() { _ = client.Close() })
	old := newStreamWithOptions(t.Context(), 2, "old", MessagePackCodec{}, nil, client.removeStream, StreamOptions{})
	current := newStreamWithOptions(t.Context(), 2, "new", MessagePackCodec{}, nil, client.removeStream, StreamOptions{})
	if err := client.addStream(current); err != nil {
		t.Fatal(err)
	}
	old.finish(ErrUnavailable)
	if client.findStream(2) != current {
		t.Fatal("old stream cleanup removed its replacement")
	}
	current.finish(ErrClosed)
	if client.findStream(2) != nil {
		t.Fatal("current stream was not removed")
	}
}

func TestRequestCleanupDoesNotCrossConnections(t *testing.T) {
	client := NewTCPClient("", "cleanup")
	t.Cleanup(func() { _ = client.Close() })
	old, oldRemote := net.Pipe()
	current, currentRemote := net.Pipe()
	t.Cleanup(func() { _ = oldRemote.Close() })
	t.Cleanup(func() { _ = currentRemote.Close() })
	if err := client.setConn(old, 1, true); err != nil {
		t.Fatal(err)
	}
	oldCtx, oldCancel := context.WithCancel(t.Context())
	defer oldCancel()
	if !client.trackRequest(old, 1, 2, oldCancel) {
		t.Fatal("old request was not tracked")
	}
	client.connectionLostGeneration(old, 1, ErrUnavailable)
	if oldCtx.Err() == nil {
		t.Fatal("connection loss did not cancel old request")
	}
	if err := client.setConn(current, 2, true); err != nil {
		t.Fatal(err)
	}
	currentCtx, currentCancel := context.WithCancel(t.Context())
	defer currentCancel()
	if !client.trackRequest(current, 2, 2, currentCancel) {
		t.Fatal("new request was not tracked")
	}
	client.untrackRequest(old, 1, 2)
	client.cancel(2)
	if currentCtx.Err() == nil {
		t.Fatal("old handler cleanup removed the new request's cancellation")
	}
}
