// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"sync/atomic"
)

const (
	DefaultSendHwm = 1000
)

// Topics is an interface that wraps the basic Topics method.
type Topics interface {
	// Topics returns the sorted list of topics a socket is subscribed to.
	Topics() []string
}

// NewPub returns a new PUB ZeroMQ socket.
// The returned socket value is initially unbound.
func NewPub(ctx context.Context, opts ...Option) Socket {
	pub := &pubSocket{sck: newSocket(ctx, Pub, opts...)}
	w, r := newPubMWriter(pub.sck.ctx, pub.sck.pubIOLoops), newPubQReader(pub.sck.ctx)
	r.onSubscribe = w.subscribed
	pub.sck.w, pub.sck.r = w, r
	return pub
}

// pubSocket is a PUB ZeroMQ socket.
type pubSocket struct {
	sck *socket
}

// Close closes the open Socket
func (pub *pubSocket) Close() error {
	return pub.sck.Close()
}

// Send puts the message on the outbound send queue.
// Send blocks until the message can be queued or the send deadline expires.
func (pub *pubSocket) Send(msg Msg) error {
	// write never blocks (a full peer queue drops the message), so it needs no
	// per-message deadline: a context with a timer per message is pure overhead.
	return pub.sck.w.write(pub.sck.ctx, msg)
}

// SendMulti puts the message on the outbound send queue.
// SendMulti blocks until the message can be queued or the send deadline expires.
// The message will be sent as a multipart message.
func (pub *pubSocket) SendMulti(msg Msg) error {
	msg.multipart = true
	// write never blocks (a full peer queue drops the message), so it needs no
	// per-message deadline: a context with a timer per message is pure overhead.
	return pub.sck.w.write(pub.sck.ctx, msg)
}

// Recv receives a complete message.
// SendBatch queues msgs and then wakes each subscriber once, so a burst goes
// out in one write per subscriber instead of one per message. It never blocks:
// a subscriber at its high-water mark drops what does not fit.
func (pub *pubSocket) SendBatch(msgs []Msg) error {
	return pub.sck.w.(*pubMWriter).writeBatch(pub.sck.ctx, msgs)
}

func (*pubSocket) Recv() (Msg, error) {
	msg := Msg{err: fmt.Errorf("zmq4: PUB sockets can't recv messages")}
	return msg, msg.err
}

// Listen connects a local endpoint to the Socket.
func (pub *pubSocket) Listen(ep string) error {
	return pub.sck.Listen(ep)
}

// Dial connects a remote endpoint to the Socket.
func (pub *pubSocket) Dial(ep string) error {
	return pub.sck.Dial(ep)
}

// Type returns the type of this Socket (PUB, SUB, ...)
func (pub *pubSocket) Type() SocketType {
	return pub.sck.Type()
}

// Addr returns the listener's address.
// Addr returns nil if the socket isn't a listener.
func (pub *pubSocket) Addr() net.Addr {
	return pub.sck.Addr()
}

// GetOption is used to retrieve an option for a socket.
func (pub *pubSocket) GetOption(name string) (interface{}, error) {
	return pub.sck.GetOption(name)
}

// SetOption is used to set an option for a socket.
func (pub *pubSocket) SetOption(name string, value interface{}) error {
	err := pub.sck.SetOption(name, value)
	if err != nil {
		return err
	}

	if name != OptionHWM {
		return ErrBadProperty
	}

	hwm, ok := value.(int)
	if !ok {
		return ErrBadProperty
	}

	w := pub.sck.w.(*pubMWriter)
	w.hwm.Store(int64(hwm))
	return nil
}

// Topics returns the sorted list of topics a socket is subscribed to.
func (pub *pubSocket) Topics() []string {
	return pub.sck.topics()
}

// pubQReader is a queued-message reader.
type pubQReader struct {
	ctx context.Context

	onSubscribe func(c *Conn, topic string, added bool) // called for each change to a peer's subscriptions

	mu sync.RWMutex
	rs []*Conn
	c  chan Msg

	sem *semaphore // ready when a connection is live.
}

func newPubQReader(ctx context.Context) *pubQReader {
	const qrsize = 10
	return &pubQReader{
		ctx: ctx,
		c:   make(chan Msg, qrsize),
		sem: newSemaphore(),
	}
}

func (q *pubQReader) Close() error {
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

func (q *pubQReader) addConn(r *Conn) {
	q.mu.Lock()
	q.sem.enable()
	q.rs = append(q.rs, r)
	q.mu.Unlock()
	go q.listen(q.ctx, r)
}

func (q *pubQReader) rmConn(r *Conn) {
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

func (q *pubQReader) read(ctx context.Context, msg *Msg) error {
	q.sem.lock(ctx)
	select {
	case <-ctx.Done():
	case *msg = <-q.c:
	}
	return msg.err
}

func (q *pubQReader) listen(ctx context.Context, r *Conn) {
	defer q.rmConn(r)
	defer r.Close()
	// Whatever ends the read loop, the writer must drop this peer too.
	defer r.SetClosed()

	for {
		msg := r.readMsg()
		select {
		case <-ctx.Done():
			return
		default:
			if msg.err != nil {
				return
			}
			switch {
			case q.topic(msg):
				if topic, added, changed := r.subscribe(msg); changed && q.onSubscribe != nil {
					q.onSubscribe(r, topic, added)
				}
			default:
				select {
				case q.c <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (q *pubQReader) topic(msg Msg) bool {
	if len(msg.Frames) != 1 {
		return false
	}
	frame := msg.Frames[0]
	if len(frame) == 0 {
		return false
	}
	topic := frame[0]
	return topic == 0 || topic == 1
}

// pubPeer is one subscriber connection of a PUB.
type pubPeer struct {
	*connQueue
	// With a loop, the queue is written by that loop (see pubLoop) instead of
	// the connQueue's own goroutine.
	loop     *pubLoop
	nb       *nbWriter
	inReady  atomic.Bool   // on loop's ready list
	slow     atomic.Bool   // catchUp is writing; the loop leaves the peer alone
	lastMsg  atomic.Uint64 // last message enqueued: a peer matching through several prefixes gets it once
	lastCall atomic.Uint64 // last write call that enqueued here: wake each peer once per call
}

// pubIndex maps every subscribed prefix to the peers holding it. Matching a
// message is one lookup per distinct prefix length, whatever the peer count.
// It is updated one (un)subscription at a time: rebuilding it per subscription
// would make a reconnect storm quadratic.
type pubIndex struct {
	lens     []int       // distinct prefix lengths in peers
	lenCount map[int]int // prefixes per length
	peers    map[string][]*pubPeer
}

func newPubIndex() *pubIndex {
	return &pubIndex{lenCount: map[int]int{}, peers: map[string][]*pubPeer{}}
}

func (x *pubIndex) add(topic string, p *pubPeer) {
	ps := x.peers[topic]
	if len(ps) == 0 {
		if x.lenCount[len(topic)] == 0 {
			x.lens = append(x.lens, len(topic))
		}
		x.lenCount[len(topic)]++
	}
	x.peers[topic] = append(ps, p)
}

func (x *pubIndex) remove(topic string, p *pubPeer) {
	ps := x.peers[topic]
	i := slices.Index(ps, p)
	if i < 0 {
		return
	}
	ps = slices.Delete(ps, i, i+1)
	if len(ps) > 0 {
		x.peers[topic] = ps
		return
	}
	delete(x.peers, topic)
	if x.lenCount[len(topic)]--; x.lenCount[len(topic)] == 0 {
		delete(x.lenCount, len(topic))
		x.lens = slices.DeleteFunc(x.lens, func(n int) bool { return n == len(topic) })
	}
}

type pubMWriter struct {
	ctx context.Context

	// mu guards peers, index and closed: writes match under the read lock,
	// (un)subscriptions and peer changes take the write lock.
	mu     sync.RWMutex
	peers  map[*Conn]*pubPeer
	index  *pubIndex
	closed bool // after Close, a connection still finishing its handshake is not taken on

	// sendMu serializes writeBatch calls: the per-message stamps that keep a
	// peer matching through several prefixes from getting a message twice are
	// only meaningful within one call at a time.
	sendMu sync.Mutex
	seq    uint64 // message and call stamps, under sendMu
	hwm    atomic.Int64

	loops    []*pubLoop // nil: every peer has its own writer goroutine
	nextLoop int
}

func newPubMWriter(ctx context.Context, ioLoops int) *pubMWriter {
	p := &pubMWriter{
		ctx:   ctx,
		peers: map[*Conn]*pubPeer{},
		index: newPubIndex(),
	}
	if n := ioLoops; n > 0 {
		for i := 0; i < n; i++ {
			l := newPubLoop()
			p.loops = append(p.loops, l)
			context.AfterFunc(ctx, l.stop)
			go l.run()
		}
	}
	p.hwm.Store(DefaultSendHwm)
	return p
}

func (w *pubMWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.closed = true
	for conn, p := range w.peers {
		_ = conn.Close()
		p.stop()
	}
	w.peers = map[*Conn]*pubPeer{}
	w.index = newPubIndex()
	return nil
}

func (w *pubMWriter) addConn(c *Conn) {
	p := &pubPeer{connQueue: newConnQueue(w.ctx, c)}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		_ = c.Close()
		return
	}
	if len(w.loops) > 0 {
		if p.nb = newNBWriter(c); p.nb != nil {
			p.loop = w.loops[w.nextLoop%len(w.loops)]
			w.nextLoop++
		}
	}
	w.peers[c] = p // no subscriptions yet: they arrive through subscribed
	w.mu.Unlock()
	if p.loop == nil {
		go p.run()
	}
}

func (w *pubMWriter) rmConn(c *Conn) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if p, ok := w.peers[c]; ok {
		_ = c.Close()
		delete(w.peers, c)
		p.stop()
		c.mu.RLock()
		for t := range c.topics {
			w.index.remove(t, p)
		}
		c.mu.RUnlock()
	}
}

// subscribed applies one change to c's subscriptions to the index.
func (w *pubMWriter) subscribed(c *Conn, topic string, added bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p, ok := w.peers[c]
	if !ok {
		return
	}
	if added {
		w.index.add(topic, p)
	} else {
		w.index.remove(topic, p)
	}
}

func (w *pubMWriter) write(ctx context.Context, msg Msg) error {
	msgs := [1]Msg{msg}
	return w.writeBatch(ctx, msgs[:])
}

// writeBatch queues each message for the peers subscribed to its topic, then
// wakes each of those peers once. A peer whose queue already holds hwm messages
// drops new ones, as libzmq's PUB does at its high-water mark; only messages the
// peer subscribed to count against it.
func (w *pubMWriter) writeBatch(ctx context.Context, msgs []Msg) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	w.mu.RLock()
	defer w.mu.RUnlock()
	idx := w.index
	if len(idx.lens) == 0 {
		return nil
	}
	hwm := int(w.hwm.Load())
	w.seq++
	call := w.seq
	var touchedBuf [64]*pubPeer
	touched := touchedBuf[:0]
	for i := range msgs {
		if len(msgs[i].Frames) == 0 {
			continue
		}
		topic := msgs[i].Frames[0]
		w.seq++
		stamp := w.seq
		for _, n := range idx.lens {
			if n > len(topic) {
				continue
			}
			for _, p := range idx.peers[string(topic[:n])] {
				if p.lastMsg.Swap(stamp) == stamp {
					continue
				}
				p.push(msgs[i], hwm)
				if p.lastCall.Swap(call) != call {
					touched = append(touched, p)
				}
			}
		}
	}
	var loopsBuf [8]*pubLoop
	loops := loopsBuf[:0]
	for _, p := range touched {
		if p.loop == nil {
			p.signal()
			continue
		}
		p.loop.enqueue(p)
		if !slices.Contains(loops, p.loop) {
			loops = append(loops, p.loop)
		}
	}
	for _, l := range loops {
		l.signal()
	}
	return nil
}

var (
	_ rpool  = (*pubQReader)(nil)
	_ wpool  = (*pubMWriter)(nil)
	_ Socket = (*pubSocket)(nil)
	_ Topics = (*pubSocket)(nil)
)
