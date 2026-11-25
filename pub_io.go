// Copyright 2026 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"net"
	"sync"
	"sync/atomic"
)

// pubLoop writes for a share of a PUB's subscribers, like one of libzmq's I/O
// threads: a batch wakes it once, and it sweeps every subscriber that has
// something queued, writing each one's whole queue with one non-blocking
// writev. Messages that arrive during a sweep go out together in the next one,
// so batches grow with load while an idle PUB writes at once.
//
// A subscriber whose socket would block, or whose write lock is taken (a PONG
// going out), is handed to its own goroutine until it has caught up, so one
// slow subscriber never holds up the others.
type pubLoop struct {
	wake    chan struct{}
	stopped atomic.Bool

	mu    sync.Mutex
	ready []*pubPeer
	spare []*pubPeer
}

func newPubLoop() *pubLoop { return &pubLoop{wake: make(chan struct{}, 1)} }

func (l *pubLoop) signal() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (l *pubLoop) stop() {
	l.stopped.Store(true)
	l.signal()
}

// enqueue puts p on the ready list unless it is already there.
func (l *pubLoop) enqueue(p *pubPeer) {
	if p.inReady.Swap(true) {
		return
	}
	l.mu.Lock()
	l.ready = append(l.ready, p)
	l.mu.Unlock()
}

func (l *pubLoop) run() {
	for {
		<-l.wake
		for !l.stopped.Load() {
			l.mu.Lock()
			if len(l.ready) == 0 {
				l.mu.Unlock()
				break
			}
			// ready and spare must never share an array: enqueue appends to
			// ready while the sweep walks batch.
			batch := l.ready
			l.ready = l.spare[:0]
			l.mu.Unlock()
			for _, p := range batch {
				p.inReady.Store(false)
				p.flush()
			}
			clear(batch)
			l.mu.Lock()
			l.spare = batch[:0]
			l.mu.Unlock()
		}
		if l.stopped.Load() {
			return
		}
	}
}

// flush writes p's queue without blocking; the part that would block goes to
// p's own goroutine. Called by p's loop only.
func (p *pubPeer) flush() {
	if p.slow.Load() {
		return // its own goroutine is writing and takes what is queued
	}
	p.mu.Lock()
	if len(p.q) == 0 {
		p.mu.Unlock()
		return
	}
	// q and spare must never share an array: push appends to q while msgs is written.
	msgs := p.q
	p.q = p.spare[:0]
	p.mu.Unlock()
	c := p.conn
	var rest net.Buffers
	locked := false // the write lock goes to catchUp with rest
	if c.wmu.TryLock() {
		var err error
		rest, err = p.nb.write(msgs)
		if err != nil {
			c.checkIO(err)
			rest = nil // the connection is gone; the reader's error removes the peer
		}
		if len(rest) == 0 {
			c.wmu.Unlock()
		} else {
			// rest can start in the middle of a frame: keep the lock until catchUp
			// has finished it, or a PONG or PING would land inside the frame.
			locked = true
		}
	} else {
		rest = frameBuffers(msgs) // a command is being written: let the slow path wait for it
	}
	clear(msgs)
	p.mu.Lock()
	p.spare = msgs[:0]
	p.mu.Unlock()
	if len(rest) > 0 {
		p.slow.Store(true)
		go p.catchUp(rest, locked)
	}
}

// catchUp finishes a write that would have blocked, then keeps writing p's
// queue with blocking writes until it is empty, and hands p back to its loop.
// locked says flush handed over the connection's write lock (rest then ends a
// frame flush started); otherwise rest is whole frames and catchUp takes it.
func (p *pubPeer) catchUp(rest net.Buffers, locked bool) {
	c := p.conn
	if !locked {
		c.wmu.Lock()
	}
	_, err := rest.WriteTo(c.rw)
	c.wmu.Unlock()
	if err != nil {
		c.checkIO(err)
	}
	for {
		p.mu.Lock()
		if len(p.q) == 0 {
			// Under p.mu: a message pushed after this re-enqueues p on its loop.
			p.slow.Store(false)
			p.mu.Unlock()
			return
		}
		msgs := p.q
		p.q = p.spare[:0]
		p.mu.Unlock()
		if c.sendBatch(msgs) != nil {
			// broken connection: keep draining so the queue does not grow until the peer is removed
		}
		clear(msgs)
		p.mu.Lock()
		p.spare = msgs[:0]
		p.mu.Unlock()
	}
}

// frameBuffers lays msgs out as ZMTP frames (header, body, header, body, ...).
// Headers are freshly allocated: the result may outlive the call.
func frameBuffers(msgs []Msg) net.Buffers {
	nframes := 0
	for i := range msgs {
		nframes += len(msgs[i].Frames)
	}
	hdrs := make([]byte, 9*nframes)
	bufs := make(net.Buffers, 0, 2*nframes)
	for i := range msgs {
		frames := msgs[i].Frames
		for j, frame := range frames {
			var flag byte
			if j < len(frames)-1 {
				flag ^= hasMoreBitFlag
			}
			hdr := hdrs[:9]
			hdrs = hdrs[9:]
			hsz := 2
			if size := len(frame); size > 255 {
				hdr[0] = flag ^ isLongBitFlag
				putUint64BE(hdr[1:], uint64(size))
				hsz = 9
			} else {
				hdr[0] = flag
				hdr[1] = uint8(size)
			}
			bufs = append(bufs, hdr[:hsz], frame)
		}
	}
	return bufs
}

func putUint64BE(b []byte, v uint64) {
	_ = b[7]
	b[0], b[1], b[2], b[3] = byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32)
	b[4], b[5], b[6], b[7] = byte(v>>24), byte(v>>16), byte(v>>8), byte(v)
}
