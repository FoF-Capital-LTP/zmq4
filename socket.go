// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultRetry   = 250 * time.Millisecond
	defaultTimeout = 5 * time.Minute

	// defaultHandshakeTimeout bounds the ZMTP greeting and handshake, like libzmq's ZMQ_HANDSHAKE_IVL.
	defaultHandshakeTimeout = 30 * time.Second
	defaultMaxRetries       = 10
)

var (
	errInvalidAddress = errors.New("zmq4: invalid address")

	ErrBadProperty = errors.New("zmq4: bad property")
)

// socket implements the ZeroMQ socket interface
type socket struct {
	ep            string // socket end-point
	typ           SocketType
	id            SocketIdentity
	retry         time.Duration
	maxRetries    int
	sec           Security
	log           *log.Logger
	subTopics     func() []string
	autoReconnect bool
	inlineRecv    bool
	zeroCopyRecv  bool

	handshakeTimeout time.Duration
	maxRecvFrame     uint64
	timeout          time.Duration

	mu    sync.RWMutex
	conns []*Conn // ZMTP connections
	r     rpool
	w     wpool

	props map[string]interface{} // properties of this socket

	ctx      context.Context // life-line of socket
	cancel   context.CancelFunc
	listener net.Listener
	dialer   net.Dialer

	closedConns   []*Conn
	reaperCond    *sync.Cond
	reaperStarted bool
}

func newDefaultSocket(ctx context.Context, sockType SocketType) *socket {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	return &socket{
		typ:        sockType,
		retry:      defaultRetry,
		maxRetries: defaultMaxRetries,
		timeout:    defaultTimeout,

		handshakeTimeout: defaultHandshakeTimeout,
		maxRecvFrame:     defaultMaxRecvFrameSize,
		sec:              nullSecurity{},
		conns:            nil,
		r:                newQReader(ctx),
		w:                newMWriter(ctx),
		props:            make(map[string]interface{}),
		ctx:              ctx,
		cancel:           cancel,
		dialer:           net.Dialer{Timeout: defaultTimeout},
		reaperCond:       sync.NewCond(&sync.Mutex{}),
	}
}

func newSocket(ctx context.Context, sockType SocketType, opts ...Option) *socket {
	sck := newDefaultSocket(ctx, sockType)
	for _, opt := range opts {
		opt(sck)
	}
	if len(sck.id) == 0 {
		sck.id = SocketIdentity(newUUID())
	}
	if sck.log == nil {
		sck.log = log.New(os.Stderr, "zmq4: ", 0)
	}

	return sck
}

func (sck *socket) topics() []string {
	var (
		keys   = make(map[string]struct{})
		topics []string
	)
	sck.mu.RLock()
	for _, con := range sck.conns {
		con.mu.RLock()
		for topic := range con.topics {
			if _, dup := keys[topic]; dup {
				continue
			}
			keys[topic] = struct{}{}
			topics = append(topics, topic)
		}
		con.mu.RUnlock()
	}
	sck.mu.RUnlock()
	sort.Strings(topics)
	return topics
}

// Close closes the open Socket
func (sck *socket) Close() error {
	// The Lock around Signal ensures the connReaper is running
	// and is in sck.reaperCond.Wait()
	sck.reaperCond.L.Lock()
	sck.cancel()
	sck.reaperCond.Signal()
	sck.reaperCond.L.Unlock()

	if sck.listener != nil {
		defer sck.listener.Close()
	}

	sck.mu.RLock()
	defer sck.mu.RUnlock()

	var err error
	for _, conn := range sck.conns {
		e := conn.Close()
		if e != nil && err == nil {
			err = e
		}
	}

	// Remove the unix socket file if created by net.Listen
	if sck.listener != nil && strings.HasPrefix(sck.ep, "ipc://") {
		os.Remove(sck.ep[len("ipc://"):])
	}

	return err
}

// Send puts the message on the outbound send queue.
// Send blocks until the message can be queued or the send deadline expires.
func (sck *socket) Send(msg Msg) error {
	ctx, cancel := context.WithTimeout(sck.ctx, sck.Timeout())
	defer cancel()
	return sck.w.write(ctx, msg)
}

// SendMulti puts the message on the outbound send queue.
// SendMulti blocks until the message can be queued or the send deadline expires.
// The message will be sent as a multipart message.
func (sck *socket) SendMulti(msg Msg) error {
	msg.multipart = true
	ctx, cancel := context.WithTimeout(sck.ctx, sck.Timeout())
	defer cancel()
	return sck.w.write(ctx, msg)
}

// Recv receives a complete message.
func (sck *socket) Recv() (Msg, error) {
	if q, ok := sck.r.(*inlineReader); ok {
		return q.recv(sck.ctx)
	}
	var msg Msg
	err := sck.r.read(sck.ctx, &msg)
	return msg, err
}

// Listen connects a local endpoint to the Socket.
func (sck *socket) Listen(endpoint string) error {
	sck.ep = endpoint
	network, addr, err := splitAddr(endpoint)
	if err != nil {
		return err
	}

	trans, ok := drivers.get(network)
	if !ok {
		return UnknownTransportError{Name: network}
	}

	l, err := trans.Listen(sck.ctx, addr)
	if err != nil {
		return fmt.Errorf("zmq4: could not listen to %q: %w", endpoint, err)
	}
	sck.listener = l

	go sck.accept()
	if !sck.reaperStarted {
		sck.reaperCond.L.Lock()
		go sck.connReaper()
		sck.reaperStarted = true
	}

	return nil
}

func (sck *socket) accept() {
	ctx, cancel := context.WithCancel(sck.ctx)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := sck.listener.Accept()
			if err != nil {
				// FIXME(sbinet): maybe bubble up this error to application code?
				//sck.log.Printf("error accepting connection from %q: %+v", sck.ep, err)
				continue
			}

			zconn, err := sck.openConn(conn, true)
			if err != nil {
				// FIXME(sbinet): maybe bubble up this error to application code?
				sck.log.Printf("could not open a ZMTP connection with %q: %+v", sck.ep, err)
				continue
			}

			sck.addConn(zconn)
		}
	}
}

// Dial connects a remote endpoint to the Socket.
func (sck *socket) Dial(endpoint string) error {
	sck.ep = endpoint

	network, addr, err := splitAddr(endpoint)
	if err != nil {
		return err
	}

	var (
		conn      net.Conn
		trans, ok = drivers.get(network)
		retries   = 0
	)
	if !ok {
		return UnknownTransportError{Name: network}
	}

connect:
	conn, err = trans.Dial(sck.ctx, &sck.dialer, addr)
	if err != nil {
		// retry if retry count is lower than maximum retry count and context has not been canceled
		if (sck.maxRetries == -1 || retries < sck.maxRetries) && sck.ctx.Err() == nil {
			retries++
			t := time.NewTimer(sck.retry)
			select {
			case <-sck.ctx.Done():
				t.Stop()
			case <-t.C:
			}
			goto connect
		}
		return fmt.Errorf("zmq4: could not dial to %q (retry=%v): %w", endpoint, sck.retry, err)
	}

	if conn == nil {
		return fmt.Errorf("zmq4: got a nil dial-conn to %q", endpoint)
	}

	zconn, err := sck.openConn(conn, false)
	if err != nil {
		return fmt.Errorf("zmq4: could not open a ZMTP connection: %w", err)
	}
	if zconn == nil {
		return fmt.Errorf("zmq4: got a nil ZMTP connection to %q", endpoint)
	}

	if !sck.reaperStarted {
		sck.reaperCond.L.Lock()
		go sck.connReaper()
		sck.reaperStarted = true
	}
	sck.addConn(zconn)
	return nil
}

// openConn performs the ZMTP handshake on conn. The handshake is bounded by the
// handshake timeout and aborted when the socket is closed: a peer that accepts
// the TCP connection but never sends its greeting would otherwise block forever.
// conn is closed if the handshake fails.
func (sck *socket) openConn(conn net.Conn, server bool) (*Conn, error) {
	if sck.handshakeTimeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(sck.handshakeTimeout))
	}
	stop := context.AfterFunc(sck.ctx, func() {
		_ = conn.SetDeadline(time.Unix(1, 0))
	})
	zconn, err := Open(conn, sck.sec, sck.typ, sck.id, server, sck.scheduleRmConn)
	if !stop() && err == nil {
		// the socket was closed while (or right after) handshaking: the deadline is in the past
		err = sck.ctx.Err()
	}
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	zconn.maxRecvFrame = sck.maxRecvFrame
	return zconn, nil
}

func (sck *socket) addConn(c *Conn) {
	sck.mu.Lock()
	defer sck.mu.Unlock()
	sck.conns = append(sck.conns, c)
	if len(c.Peer.Meta[sysSockID]) == 0 {
		switch c.typ {
		case Router: // TODO: STREAM type when implemented
			// if empty Identity metadata is received from some client
			// need to assign an uuid such that router socket can reply to the correct client
			c.Peer.Meta[sysSockID] = newUUID()
		}
	}
	if sck.w != nil {
		sck.w.addConn(c)
	}
	if sck.r != nil {
		sck.r.addConn(c)
	}
	// resend subscriptions for topics if there are any
	if sck.subTopics != nil {
		for _, topic := range sck.subTopics() {
			_ = sck.Send(NewMsg(append([]byte{1}, topic...)))
		}
	}
}

func (sck *socket) rmConn(c *Conn) {
	sck.mu.Lock()
	defer sck.mu.Unlock()

	cur := -1
	for i := range sck.conns {
		if sck.conns[i] == c {
			cur = i
			break
		}
	}

	if cur == -1 {
		return
	}

	sck.conns = append(sck.conns[:cur], sck.conns[cur+1:]...)
	if sck.r != nil {
		sck.r.rmConn(c)
	}
	if sck.w != nil {
		sck.w.rmConn(c)
	}
}

func (sck *socket) scheduleRmConn(c *Conn) {
	sck.reaperCond.L.Lock()
	sck.closedConns = append(sck.closedConns, c)
	sck.reaperCond.Signal()
	sck.reaperCond.L.Unlock()

	if sck.autoReconnect {
		sck.Dial(sck.ep)
	}
}

// Type returns the type of this Socket (PUB, SUB, ...)
func (sck *socket) Type() SocketType {
	return sck.typ
}

// Addr returns the listener's address.
// Addr returns nil if the socket isn't a listener.
func (sck *socket) Addr() net.Addr {
	if sck.listener == nil {
		return nil
	}
	return sck.listener.Addr()
}

// GetOption is used to retrieve an option for a socket.
func (sck *socket) GetOption(name string) (interface{}, error) {
	v, ok := sck.props[name]
	if !ok {
		return nil, ErrBadProperty
	}
	return v, nil
}

// SetOption is used to set an option for a socket.
func (sck *socket) SetOption(name string, value interface{}) error {
	// FIXME(sbinet) different socket types support different options.
	sck.props[name] = value
	return nil
}

func (sck *socket) Timeout() time.Duration {
	return sck.timeout
}

func (sck *socket) connReaper() {
	// We are not locking here sck.reaperCond.L.Lock()
	// as it should be locked prior starting connReaper as goroutine
	// That would ensure that sck.reaperCond.Signal()
	// would be delivered only when reaper goroutine is really started
	// and is in sck.reaperCond.Wait()
	defer sck.reaperCond.L.Unlock()

	for {
		for len(sck.closedConns) == 0 && sck.ctx.Err() == nil {
			sck.reaperCond.Wait()
		}

		if sck.ctx.Err() != nil {
			return
		}

		// Clone the known closed connections to avoid data race
		// and remove those under reaper unlocked.
		// That should fix the deadlock reported in #149.
		cc := append([]*Conn{}, sck.closedConns...) // clone
		sck.closedConns = sck.closedConns[:0]
		sck.reaperCond.L.Unlock()
		for _, c := range cc {
			sck.rmConn(c)
		}
		sck.reaperCond.L.Lock()
	}
}

var (
	_ Socket = (*socket)(nil)
)
