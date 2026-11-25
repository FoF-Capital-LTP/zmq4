// Copyright 2026 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"sync"
	"sync/atomic"
)

// connQueue is a connection's send queue, drained by its own goroutine into as
// few writes as possible. Senders never block on it: push refuses a message
// once hwm are waiting, like a libzmq pipe at its high-water mark.
type connQueue struct {
	conn *Conn
	// wake has capacity 1: "there is something to do". Stopping rings it too, so
	// run waits on a single channel instead of a select over three.
	wake      chan struct{}
	stopped   atomic.Bool
	stopOnCtx func() bool // unregisters the stop on the socket context

	mu    sync.Mutex
	q     []Msg // filled by push, swapped out by run
	spare []Msg
}

// newConnQueue returns the queue of c; it stops when stop is called or ctx
// (the socket's: Close cancels it without removing each peer) is done.
func newConnQueue(ctx context.Context, c *Conn) *connQueue {
	cq := &connQueue{conn: c, wake: make(chan struct{}, 1)}
	cq.stopOnCtx = context.AfterFunc(ctx, cq.halt)
	return cq
}

// push queues m, or reports false when hwm messages are already waiting.
func (cq *connQueue) push(m Msg, hwm int) bool {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if len(cq.q) >= hwm {
		return false
	}
	cq.q = append(cq.q, m)
	return true
}

// signal wakes the writer; calls before it runs collapse into one wakeup.
func (cq *connQueue) signal() {
	select {
	case cq.wake <- struct{}{}:
	default:
	}
}

// halt makes run return at its next wakeup and rings it.
func (cq *connQueue) halt() {
	cq.stopped.Store(true)
	cq.signal()
}

func (cq *connQueue) stop() {
	cq.halt()
	cq.stopOnCtx()
}

// queueBatch is the most messages run puts in one write.
const queueBatch = 256

// run writes out the queue each time it is woken. Messages queued while a write
// is in flight go out together in the next one, as libzmq's I/O thread does.
func (cq *connQueue) run() {
	for {
		<-cq.wake
		if cq.stopped.Load() {
			return
		}
		cq.mu.Lock()
		batch := cq.q
		cq.q = cq.spare[:0]
		cq.mu.Unlock()
		for rest := batch; len(rest) > 0; {
			n := min(len(rest), queueBatch)
			if cq.conn.sec.Type() == NullSecurity {
				_ = cq.conn.sendBatch(rest[:n])
			} else {
				for _, m := range rest[:n] {
					_ = cq.conn.SendMsg(m)
				}
			}
			rest = rest[n:]
		}
		clear(batch) // do not pin sent messages
		cq.mu.Lock()
		cq.spare = batch[:0]
		cq.mu.Unlock()
	}
}
