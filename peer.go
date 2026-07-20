package gorpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PeerDirection identifies which side created the active physical connection.
type PeerDirection string

const (
	// PeerDirectionInbound means the remote peer established the physical connection.
	PeerDirectionInbound  PeerDirection = "inbound"
	// PeerDirectionOutbound means the local peer established the physical connection.
	PeerDirectionOutbound PeerDirection = "outbound"
)

// PeerManager owns all GoRPC connections for one local application identity.
// A manager must be shared by every GoRPC server and dial path in the process.
type PeerManager struct {
	localName string

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	peers  map[string]*Peer
	closed bool
}

// PeerDialOptions configure a managed outbound connection. PeerName is the
// authenticated application identity expected at the remote endpoint.
type PeerDialOptions struct {
	PeerName         string
	Network          string
	Address          string
	ClientOptions    ClientOptions
	RegisterHandlers func(*Client) error
}

// PeerStatus is a point-in-time snapshot of one logical peer relationship.
type PeerStatus struct {
	PeerName      string
	Active        bool
	Direction     PeerDirection
	Network       string
	LocalAddress  string
	RemoteAddress string
	ConnectedAt   time.Time
	Dialing       bool
	LastError     string
}

type peerDialConfig struct {
	network          string
	address          string
	clientOptions    ClientOptions
	registerHandlers func(*Client) error
}

type peerEndpoint interface {
	CallContext(context.Context, string, any, any) error
	AsyncCallContext(context.Context, string, any, any, string) error
	NotifyContext(context.Context, string, any) error
	streamTarget
}

type peerLink struct {
	direction   PeerDirection
	endpoint    peerEndpoint
	client      *Client
	conn        *Conn
	ready       bool
	connectedAt time.Time
}

// Peer is one logical, full-duplex relationship. Calls are routed over the
// single active physical connection regardless of which side initiated it.
type Peer struct {
	manager *PeerManager
	name    string
	key     string

	mu          sync.Mutex
	active      *peerLink
	ready       chan struct{}
	dialConfig  *peerDialConfig
	dialing     bool
	dialCancel  context.CancelFunc
	dialDone    chan struct{}
	lastDialErr error
	leases      int
	closed      bool
}

// PeerClient is a caller-owned lease on a managed peer relationship. Closing
// one lease never interrupts another user of the same peer.
type PeerClient struct {
	peer   *Peer
	closed atomic.Bool
}

// NewPeerManager creates a connection manager for localName.
func NewPeerManager(localName string) *PeerManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &PeerManager{
		localName: strings.TrimSpace(localName),
		ctx:       ctx,
		cancel:    cancel,
		peers:     make(map[string]*Peer),
	}
}

// LocalName returns the identity placed in managed outbound handshakes.
func (m *PeerManager) LocalName() string {
	if m == nil {
		return ""
	}
	return m.localName
}

// Dial acquires one logical peer and waits for either an existing inbound
// connection or one shared outbound dial to become ready.
func (m *PeerManager) Dial(ctx context.Context, opts PeerDialOptions) (*PeerClient, error) {
	if m == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	peerName := strings.TrimSpace(opts.PeerName)
	if peerName == "" {
		return nil, ErrPeerIdentityRequired
	}
	network := strings.TrimSpace(opts.Network)
	if network == "" {
		network = "tcp"
	}
	address := strings.TrimSpace(opts.Address)
	if address == "" {
		return nil, fmt.Errorf("%w: peer address is required", ErrUnavailable)
	}

	peer, err := m.peer(peerName, true)
	if err != nil {
		return nil, err
	}
	if err := peer.acquire(peerDialConfig{
		network:          network,
		address:          address,
		clientOptions:    opts.ClientOptions,
		registerHandlers: opts.RegisterHandlers,
	}); err != nil {
		return nil, err
	}
	client := &PeerClient{peer: peer}
	if err := peer.WaitReady(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

// Peer returns a known logical peer, including accepted-only peers.
func (m *PeerManager) Peer(peerName string) (*Peer, bool) {
	peer, err := m.peer(peerName, false)
	return peer, err == nil && peer != nil
}

// Peers returns a snapshot of all known logical peers.
func (m *PeerManager) Peers() []*Peer {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	peers := make([]*Peer, 0, len(m.peers))
	for _, peer := range m.peers {
		peers = append(peers, peer)
	}
	return peers
}

// Close stops all pending dials and closes all managed connections.
func (m *PeerManager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	peers := make([]*Peer, 0, len(m.peers))
	for _, peer := range m.peers {
		peers = append(peers, peer)
	}
	m.mu.Unlock()
	m.cancel()
	for _, peer := range peers {
		peer.close()
	}
	return nil
}

func (m *PeerManager) peer(peerName string, create bool) (*Peer, error) {
	if m == nil {
		return nil, ErrClosed
	}
	name := strings.TrimSpace(peerName)
	key := normalizePeerName(name)
	if key == "" {
		return nil, ErrPeerIdentityRequired
	}
	if key == normalizePeerName(m.localName) {
		return nil, ErrPeerSelfConnection
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	peer := m.peers[key]
	if peer == nil && create {
		peer = &Peer{manager: m, name: name, key: key, ready: make(chan struct{})}
		m.peers[key] = peer
	}
	return peer, nil
}

func (m *PeerManager) acceptInbound(conn *Conn) error {
	if m == nil || conn == nil {
		return ErrClosed
	}
	peer, err := m.peer(conn.ClientName(), true)
	if err != nil {
		return err
	}
	return peer.acceptInbound(conn)
}

func (m *PeerManager) disconnected(conn *Conn) {
	if m == nil || conn == nil {
		return
	}
	peer, _ := m.peer(conn.ClientName(), false)
	if peer != nil {
		peer.disconnected(conn)
	}
}

func (m *PeerManager) connected(conn *Conn) error {
	if m == nil || conn == nil {
		return ErrClosed
	}
	peer, err := m.peer(conn.ClientName(), false)
	if err != nil {
		return err
	}
	if peer == nil {
		return ErrPeerConnected
	}
	return peer.connected(conn)
}

func (p *Peer) acquire(config peerDialConfig) error {
	if p == nil {
		return ErrClosed
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	if p.dialConfig != nil && (p.dialConfig.network != config.network || p.dialConfig.address != config.address) {
		if p.leases > 0 {
			p.mu.Unlock()
			return fmt.Errorf("%w: peer %s is already configured at %s %s", ErrPeerConfiguration, p.name, p.dialConfig.network, p.dialConfig.address)
		}
		copied := config
		p.dialConfig = &copied
	}
	if p.dialConfig == nil {
		copied := config
		p.dialConfig = &copied
	}
	p.leases++
	p.lastDialErr = nil
	p.mu.Unlock()
	p.ensureDial()
	return nil
}

func (p *Peer) release() {
	if p == nil {
		return
	}
	var cancel context.CancelFunc
	var closeClient *Client
	p.mu.Lock()
	if p.leases > 0 {
		p.leases--
	}
	if p.leases == 0 {
		cancel = p.dialCancel
		if p.active != nil && p.active.direction == PeerDirectionOutbound {
			closeClient = p.active.client
			p.active = nil
			p.ready = make(chan struct{})
		}
	}
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if closeClient != nil {
		_ = closeClient.Close()
	}
}

func (p *Peer) ensureDial() {
	if p == nil || p.manager == nil {
		return
	}
	p.mu.Lock()
	if p.closed || p.leases == 0 || p.dialConfig == nil || p.active != nil || p.dialing {
		p.mu.Unlock()
		return
	}
	config := *p.dialConfig
	dialCtx, cancel := context.WithCancel(p.manager.ctx)
	p.dialing = true
	p.dialCancel = cancel
	p.dialDone = make(chan struct{})
	p.lastDialErr = nil
	done := p.dialDone
	p.mu.Unlock()

	go p.runDial(dialCtx, config, done)
}

func (p *Peer) runDial(ctx context.Context, config peerDialConfig, done chan struct{}) {
	options := config.clientOptions
	options.ClientName = p.manager.localName
	client := NewClient(config.network, config.address, options)
	var err error
	if config.registerHandlers != nil {
		err = config.registerHandlers(client)
	}
	if err == nil {
		err = client.Connect(ctx)
	}

	keepClient := false
	p.mu.Lock()
	if p.dialDone == done {
		p.dialing = false
		p.dialCancel = nil
		p.dialDone = nil
		if err == nil && !p.closed && p.leases > 0 && p.active == nil {
			p.active = &peerLink{
				direction:   PeerDirectionOutbound,
				endpoint:    client,
				client:      client,
				ready:       true,
				connectedAt: time.Now(),
			}
			close(p.ready)
			keepClient = true
			p.lastDialErr = nil
		} else if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrPeerConnected) {
			p.lastDialErr = err
		}
		close(done)
	}
	p.mu.Unlock()
	if !keepClient {
		_ = client.Close()
	}
}

func (p *Peer) acceptInbound(conn *Conn) error {
	if p == nil || conn == nil {
		return ErrClosed
	}
	var cancel context.CancelFunc
	var closeClient *Client
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	if p.active != nil {
		if p.active.conn == conn {
			p.mu.Unlock()
			return nil
		}
		if p.active.direction == PeerDirectionOutbound && !clientReady(p.active.client) {
			closeClient = p.active.client
			p.active = nil
			p.ready = make(chan struct{})
		} else {
			p.mu.Unlock()
			return ErrPeerConnected
		}
	}
	if p.dialing && preferOutbound(p.manager.localName, p.name) {
		p.mu.Unlock()
		return ErrPeerConnected
	}
	cancel = p.dialCancel
	p.active = &peerLink{
		direction: PeerDirectionInbound,
		endpoint:  conn,
		conn:      conn,
	}
	p.lastDialErr = nil
	p.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if closeClient != nil {
		_ = closeClient.Close()
	}
	return nil
}

func (p *Peer) connected(conn *Conn) error {
	if p == nil || conn == nil {
		return ErrClosed
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	if p.active == nil || p.active.conn != conn {
		return ErrPeerConnected
	}
	if !p.active.ready {
		p.active.ready = true
		p.active.connectedAt = time.Now()
		close(p.ready)
	}
	return nil
}

func (p *Peer) disconnected(conn *Conn) {
	if p == nil || conn == nil {
		return
	}
	p.mu.Lock()
	if p.active == nil || p.active.conn != conn {
		p.mu.Unlock()
		return
	}
	p.active = nil
	p.ready = make(chan struct{})
	shouldDial := !p.closed && p.leases > 0 && p.dialConfig != nil
	p.mu.Unlock()
	if shouldDial {
		p.ensureDial()
	}
}

func (p *Peer) close() {
	if p == nil {
		return
	}
	var cancel context.CancelFunc
	var client *Client
	var conn *Conn
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	cancel = p.dialCancel
	if p.active != nil {
		client = p.active.client
		conn = p.active.conn
		p.active = nil
	}
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if client != nil {
		_ = client.Close()
	}
	if conn != nil {
		_ = conn.Close()
	}
}

// Name returns the remote peer identity.
func (p *Peer) Name() string {
	if p == nil {
		return ""
	}
	return p.name
}

// Status returns the current logical connection state.
func (p *Peer) Status() PeerStatus {
	status := PeerStatus{}
	if p == nil {
		return status
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	status.PeerName = p.name
	status.Dialing = p.dialing
	if p.lastDialErr != nil {
		status.LastError = p.lastDialErr.Error()
	}
	if p.dialConfig != nil {
		status.Network = p.dialConfig.network
		status.RemoteAddress = p.dialConfig.address
	}
	if p.active == nil {
		return status
	}
	status.Direction = p.active.direction
	status.ConnectedAt = p.active.connectedAt
	switch p.active.direction {
	case PeerDirectionInbound:
		status.Active = p.active.ready
		status.LocalAddress = addrString(p.active.conn.LocalAddr())
		status.RemoteAddress = addrString(p.active.conn.RemoteAddr())
		if p.active.conn.LocalAddr() != nil {
			status.Network = p.active.conn.LocalAddr().Network()
		}
	case PeerDirectionOutbound:
		status.Active = clientReady(p.active.client)
		if conn, ok := p.active.client.currentConn(); ok {
			status.LocalAddress = addrString(conn.LocalAddr())
			status.RemoteAddress = addrString(conn.RemoteAddr())
			status.Network = conn.RemoteAddr().Network()
		}
	}
	return status
}

// WaitReady waits until the peer has one active physical connection.
func (p *Peer) WaitReady(ctx context.Context) error {
	if p == nil {
		return ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return ErrClosed
		}
		active := p.active
		ready := p.ready
		dialing := p.dialing
		dialDone := p.dialDone
		lastErr := p.lastDialErr
		p.mu.Unlock()
		if active != nil && active.ready {
			if active.client != nil {
				err := active.client.WaitReady(ctx)
				if err == nil {
					return nil
				}
				p.mu.Lock()
				changed := p.active != active
				p.mu.Unlock()
				if changed {
					continue
				}
				return err
			}
			return nil
		}
		if !dialing && lastErr != nil {
			return lastErr
		}
		select {
		case <-ready:
		case <-dialDone:
		case <-ctx.Done():
			return ctx.Err()
		case <-p.manager.ctx.Done():
			return ErrClosed
		}
	}
}

func (p *Peer) endpoint(ctx context.Context) (peerEndpoint, error) {
	for {
		if err := p.WaitReady(ctx); err != nil {
			return nil, err
		}
		p.mu.Lock()
		if p.active != nil && p.active.ready && p.active.endpoint != nil {
			endpoint := p.active.endpoint
			p.mu.Unlock()
			return endpoint, nil
		}
		p.mu.Unlock()
	}
}

// Call invokes a unary function over the active physical connection.
func (p *Peer) Call(function string, req any, resp any) error {
	return p.CallContext(context.Background(), function, req, resp)
}

// CallWithTimeout invokes a unary function and bounds the complete wait and call.
func (p *Peer) CallWithTimeout(function string, req any, resp any, timeout time.Duration) error {
	if timeout <= 0 {
		return p.Call(function, req, resp)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return p.CallContext(ctx, function, req, resp)
}

// CallContext invokes a unary function using ctx for readiness and call cancellation.
func (p *Peer) CallContext(ctx context.Context, function string, req any, resp any) error {
	endpoint, err := p.endpoint(ctx)
	if err != nil {
		return err
	}
	return endpoint.CallContext(ctx, function, req, resp)
}

// AsyncCallContext starts an asynchronous unary call over the active connection.
func (p *Peer) AsyncCallContext(ctx context.Context, function string, req any, handler any, correlationID string) error {
	endpoint, err := p.endpoint(ctx)
	if err != nil {
		return err
	}
	return endpoint.AsyncCallContext(ctx, function, req, handler, correlationID)
}

// Notify sends a one-way notification over the active physical connection.
func (p *Peer) Notify(function string, req any) error {
	return p.NotifyContext(context.Background(), function, req)
}

// NotifyContext sends a one-way notification using ctx for readiness and cancellation.
func (p *Peer) NotifyContext(ctx context.Context, function string, req any) error {
	endpoint, err := p.endpoint(ctx)
	if err != nil {
		return err
	}
	return endpoint.NotifyContext(ctx, function, req)
}

func (p *Peer) openServerStream(ctx context.Context, function string, req any, opts StreamOptions) (*Stream, error) {
	endpoint, err := p.endpoint(ctx)
	if err != nil {
		return nil, err
	}
	return endpoint.openServerStream(ctx, function, req, opts)
}

func (p *Peer) openClientStream(ctx context.Context, function string, opts StreamOptions) (*Stream, chan clientResponse, func(uint64), Codec, error) {
	endpoint, err := p.endpoint(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return endpoint.openClientStream(ctx, function, opts)
}

func (p *Peer) openBidiStream(ctx context.Context, function string, opts StreamOptions) (*Stream, error) {
	endpoint, err := p.endpoint(ctx)
	if err != nil {
		return nil, err
	}
	return endpoint.openBidiStream(ctx, function, opts)
}

// Peer returns the shared logical peer behind this lease.
func (c *PeerClient) Peer() *Peer {
	if c == nil {
		return nil
	}
	return c.peer
}

func (c *PeerClient) ensureOpen() error {
	if c == nil || c.peer == nil || c.closed.Load() {
		return ErrClosed
	}
	return nil
}

// Close releases this caller's interest in keeping an outbound connection.
func (c *PeerClient) Close() error {
	if c == nil || c.peer == nil || !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.peer.release()
	return nil
}

// WaitReady waits until this lease has an active physical connection.
func (c *PeerClient) WaitReady(ctx context.Context) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	return c.peer.WaitReady(ctx)
}

// Status returns a point-in-time snapshot of this lease's logical peer.
func (c *PeerClient) Status() PeerStatus {
	if c == nil || c.peer == nil {
		return PeerStatus{}
	}
	return c.peer.Status()
}

// Call invokes a unary function over this lease's active connection.
func (c *PeerClient) Call(function string, req any, resp any) error {
	return c.CallContext(context.Background(), function, req, resp)
}

// CallContext invokes a unary function using ctx for readiness and cancellation.
func (c *PeerClient) CallContext(ctx context.Context, function string, req any, resp any) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	return c.peer.CallContext(ctx, function, req, resp)
}

// AsyncCallContext starts an asynchronous unary call over this lease.
func (c *PeerClient) AsyncCallContext(ctx context.Context, function string, req any, handler any, correlationID string) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	return c.peer.AsyncCallContext(ctx, function, req, handler, correlationID)
}

// Notify sends a one-way notification over this lease's active connection.
func (c *PeerClient) Notify(function string, req any) error {
	return c.NotifyContext(context.Background(), function, req)
}

// NotifyContext sends a one-way notification using ctx for readiness and cancellation.
func (c *PeerClient) NotifyContext(ctx context.Context, function string, req any) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	return c.peer.NotifyContext(ctx, function, req)
}

func (c *PeerClient) openServerStream(ctx context.Context, function string, req any, opts StreamOptions) (*Stream, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	return c.peer.openServerStream(ctx, function, req, opts)
}

func (c *PeerClient) openClientStream(ctx context.Context, function string, opts StreamOptions) (*Stream, chan clientResponse, func(uint64), Codec, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, nil, nil, nil, err
	}
	return c.peer.openClientStream(ctx, function, opts)
}

func (c *PeerClient) openBidiStream(ctx context.Context, function string, opts StreamOptions) (*Stream, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	return c.peer.openBidiStream(ctx, function, opts)
}

func normalizePeerName(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func preferOutbound(localName, peerName string) bool {
	return normalizePeerName(localName) < normalizePeerName(peerName)
}

func clientReady(client *Client) bool {
	if client == nil {
		return false
	}
	_, ok := client.currentConn()
	return ok
}

func addrString(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	return addr.String()
}
