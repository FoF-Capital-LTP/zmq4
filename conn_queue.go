// Copyright 2026 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"sync"
)

// connQueue is a connection's send queue, drained by its own goroutine into as
// few writes as possible. Senders never block on it: push refuses a message
// once hwm are waiting, like a libzmq pipe at its high-water mark.
type connQueue struct {
	conn *Conn
	ctx  context.Context // the socket's: Close cancels it without removing each peer
	wake chan struct{}   // capacity 1: "the queue has something"
	done chan struct{}

	mu    sync.Mutex
	q     []Msg // filled by push, swapped out by run
	spare []Msg
}

func newConnQueue(ctx context.Context, c *Conn) *connQueue {
	return &connQueue{conn: c, ctx: ctx, wake: make(chan struct{}, 1), done: make(chan struct{})}
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

func (cq *connQueue) stop() { close(cq.done) }

// queueBatch is the most messages run puts in one write.
const queueBatch = 256

// run writes out the queue each time it is woken. Messages queued while a write
// is in flight go out together in the next one, as libzmq's I/O thread does.
func (cq *connQueue) run() {
	for {
		select {
		case <-cq.wake:
		case <-cq.done:
			return
		case <-cq.ctx.Done():
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
