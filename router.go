// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"fmt"
	"net"
	"sync"
)

// NewRouter returns a new ROUTER ZeroMQ socket.
// The returned socket value is initially unbound.
func NewRouter(ctx context.Context, opts ...Option) Socket {
	router := &routerSocket{newSocket(ctx, Router, opts...)}
	router.sck.r = newRouterQReader(router.sck.ctx)
	router.sck.w = newRouterMWriter(router.sck.ctx, router.sck.routerMandatory, router.sck.routerHandover)
	return router
}

// routerSocket is a ROUTER ZeroMQ socket.
type routerSocket struct {
	sck *socket
}

// Close closes the open Socket
func (router *routerSocket) Close() error {
	return router.sck.Close()
}

// Send puts the message on the outbound send queue.
// Send blocks until the message can be queued or the send deadline expires.
// Send queues msg.Frames[1:] for the peer named by msg.Frames[0]; it never blocks.
func (router *routerSocket) Send(msg Msg) error {
	return router.sck.w.write(router.sck.ctx, msg)
}

// SendMulti puts the message on the outbound send queue.
// SendMulti blocks until the message can be queued or the send deadline expires.
// The message will be sent as a multipart message.
func (router *routerSocket) SendMulti(msg Msg) error {
	msg.multipart = true
	return router.Send(msg)
}

// Recv receives a complete message.
func (router *routerSocket) Recv() (Msg, error) {
	return router.sck.Recv()
}

// Listen connects a local endpoint to the Socket.
func (router *routerSocket) Listen(ep string) error {
	return router.sck.Listen(ep)
}

// Dial connects a remote endpoint to the Socket.
func (router *routerSocket) Dial(ep string) error {
	return router.sck.Dial(ep)
}

// Type returns the type of this Socket (PUB, SUB, ...)
func (router *routerSocket) Type() SocketType {
	return router.sck.Type()
}

// Addr returns the listener's address.
// Addr returns nil if the socket isn't a listener.
func (router *routerSocket) Addr() net.Addr {
	return router.sck.Addr()
}

// GetOption is used to retrieve an option for a socket.
func (router *routerSocket) GetOption(name string) (interface{}, error) {
	return router.sck.GetOption(name)
}

// SetOption is used to set an option for a socket.
func (router *routerSocket) SetOption(name string, value interface{}) error {
	return router.sck.SetOption(name, value)
}

// routerQReader is a queued-message reader.
type routerQReader struct {
	ctx context.Context

	mu sync.RWMutex
	rs []*Conn
	c  chan Msg

	sem *semaphore // ready when a connection is live.
}

func newRouterQReader(ctx context.Context) *routerQReader {
	const qrsize = 10
	return &routerQReader{
		ctx: ctx,
		c:   make(chan Msg, qrsize),
		sem: newSemaphore(),
	}
}

func (q *routerQReader) Close() error {
	q.mu.RLock()
	var err error
	for _, r := range q.rs {
		e := r.Close()
		if e != nil && err == nil {
			err = e
		}
	}
	q.rs = nil
	q.mu.RUnlock()
	return err
}

func (q *routerQReader) addConn(r *Conn) {
	q.mu.Lock()
	q.sem.enable()
	q.rs = append(q.rs, r)
	q.mu.Unlock()
	go q.listen(q.ctx, r)
}

func (q *routerQReader) rmConn(r *Conn) {
	q.mu.Lock()
	defer q.mu.Unlock()

	cur := -1
	for i := range q.rs {
		if q.rs[i] == r {
			cur = i
			break
		}
	}
	if cur >= 0 {
		q.rs = append(q.rs[:cur], q.rs[cur+1:]...)
	}
}

func (q *routerQReader) read(ctx context.Context, msg *Msg) error {
	q.sem.lock(ctx)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case *msg = <-q.c:
	}
	return msg.err
}

func (q *routerQReader) listen(ctx context.Context, r *Conn) {
	defer q.rmConn(r)
	defer r.Close()
	// Whatever ends the read loop, the writer must drop this peer too, so a
	// mandatory Send to it fails instead of queueing into a dead connection.
	defer r.SetClosed()

	id := []byte(r.Peer.Meta[sysSockID])
	for {
		msg := r.readMsg()
		select {
		case <-ctx.Done():
			return
		default:
			if msg.err != nil {
				return
			}
			msg.Frames = append([][]byte{id}, msg.Frames...)
			select {
			case q.c <- msg:
			case <-ctx.Done():
				return
			}
		}
	}
}

// routerMWriter sends each message to the peer named by its first frame, through
// that peer's queue: a slow peer never holds up the others or the caller.
type routerMWriter struct {
	ctx       context.Context
	mandatory bool // unknown peer or full queue is an error, as ZMQ_ROUTER_MANDATORY
	handover  bool // a new peer with a known identity replaces the old one, as ZMQ_ROUTER_HANDOVER

	mu     sync.Mutex
	peers  map[string]*connQueue // by identity
	closed bool                  // after Close, a connection still finishing its handshake is not taken on
}

func newRouterMWriter(ctx context.Context, mandatory, handover bool) *routerMWriter {
	return &routerMWriter{ctx: ctx, mandatory: mandatory, handover: handover, peers: map[string]*connQueue{}}
}

func (w *routerMWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	var err error
	for _, q := range w.peers {
		q.stop()
		if e := q.conn.Close(); e != nil && err == nil {
			err = e
		}
	}
	w.peers = map[string]*connQueue{}
	return err
}

func (w *routerMWriter) addConn(c *Conn) {
	id := c.Peer.Meta[sysSockID]
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		_ = c.Close()
		return
	}
	if old, ok := w.peers[id]; ok {
		if !w.handover {
			w.mu.Unlock()
			// libzmq without handover refuses a second peer with the same identity
			_ = c.Close()
			return
		}
		old.stop()
		_ = old.conn.Close()
	}
	q := newConnQueue(w.ctx, c)
	w.peers[id] = q
	w.mu.Unlock()
	go q.run()
}

func (w *routerMWriter) rmConn(c *Conn) {
	id := c.Peer.Meta[sysSockID]
	w.mu.Lock()
	defer w.mu.Unlock()
	if q, ok := w.peers[id]; ok && q.conn == c {
		delete(w.peers, id)
		q.stop()
	}
}

// write queues msg.Frames[1:] for the peer whose identity is msg.Frames[0]. It
// never blocks. Without mandatory, a message for an unknown peer or a peer with
// a full queue is dropped, as libzmq does.
func (w *routerMWriter) write(ctx context.Context, msg Msg) error {
	if len(msg.Frames) < 2 {
		return fmt.Errorf("zmq4: ROUTER message needs an identity frame and a body")
	}
	w.mu.Lock()
	q := w.peers[string(msg.Frames[0])]
	w.mu.Unlock()
	if q == nil {
		if w.mandatory {
			return ErrHostUnreachable
		}
		return nil
	}
	if !q.push(Msg{Frames: msg.Frames[1:]}, DefaultSendHwm) {
		if w.mandatory {
			return ErrQueueFull
		}
		return nil
	}
	q.signal()
	return nil
}

var (
	_ rpool  = (*routerQReader)(nil)
	_ wpool  = (*routerMWriter)(nil)
	_ Socket = (*routerSocket)(nil)
)
