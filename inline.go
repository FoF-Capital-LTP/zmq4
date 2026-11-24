// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// inlineReader reads messages in the goroutine calling Recv, straight from the
// socket's single live connection (see WithInlineRecv).
//
// The default qreader runs one reader goroutine per connection and hands every
// message to Recv through a channel: one goroutine wake-up per message, and the
// reader goroutine can block forever on the channel once nobody calls Recv.
type inlineReader struct {
	ctx context.Context

	recvMu sync.Mutex // serializes Recv callers

	mu     sync.Mutex // serializes connection changes
	conn   atomic.Pointer[Conn]
	notify chan struct{} // signaled when a connection is added

	// zero-copy receive (WithZeroCopyRecv): reused across Recv calls, guarded by recvMu
	zeroCopy bool
	frames   [][]byte
	scratch  []byte

	stop func() bool
}

func newInlineReader(ctx context.Context, zeroCopy bool) *inlineReader {
	q := &inlineReader{
		ctx:      ctx,
		notify:   make(chan struct{}, 1),
		zeroCopy: zeroCopy,
	}
	// A blocked read is not interrupted by the context: close the connection
	// so that Recv returns as soon as the socket is closed or ctx is canceled.
	q.stop = context.AfterFunc(ctx, func() {
		if c := q.current(); c != nil {
			_ = c.Close()
		}
	})
	return q
}

func (q *inlineReader) current() *Conn {
	return q.conn.Load()
}

func (q *inlineReader) Close() error {
	q.stop()
	q.mu.Lock()
	c := q.conn.Swap(nil)
	q.mu.Unlock()
	if c != nil {
		return c.Close()
	}
	return nil
}

func (q *inlineReader) addConn(c *Conn) {
	q.mu.Lock()
	old := q.conn.Swap(c)
	q.mu.Unlock()
	if old != nil && old != c {
		_ = old.Close()
	}
	select {
	case q.notify <- struct{}{}:
	default:
	}
	if q.ctx.Err() != nil {
		_ = c.Close()
	}
}

func (q *inlineReader) rmConn(c *Conn) {
	q.mu.Lock()
	q.conn.CompareAndSwap(c, nil)
	q.mu.Unlock()
}

// wait returns the live connection, waiting for one if there is none yet.
func (q *inlineReader) wait(ctx context.Context) (*Conn, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if c := q.current(); c != nil {
			return c, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.notify:
		}
	}
}

func (q *inlineReader) read(ctx context.Context, msg *Msg) error {
	m, err := q.recv(ctx)
	*msg = m
	return err
}

// recv returns the message by value: socket.Recv calls it directly, so the
// message does not escape to the heap through the rpool interface.
func (q *inlineReader) recv(ctx context.Context) (Msg, error) {
	q.recvMu.Lock()
	defer q.recvMu.Unlock()
	for {
		c, err := q.wait(ctx)
		if err != nil {
			return Msg{}, err
		}
		var m Msg
		if cap(q.scratch) > maxRetainedScratch {
			// frames of the previous message are no longer valid: give back what a big message grew
			q.scratch = nil
		}
		if q.zeroCopy && c.r != nil && c.sec.Type() == NullSecurity {
			m, q.scratch = c.readBorrowed(q.frames, q.scratch)
			q.frames = m.Frames
		} else {
			m = c.read()
		}
		if m.err != nil {
			q.rmConn(c)
			if err := ctx.Err(); err != nil {
				return Msg{}, err
			}
			return m, m.err
		}
		if m.isCmd() {
			if err := q.handleCmd(c, m); err != nil {
				q.rmConn(c)
				_ = c.Close()
				return Msg{err: err}, err
			}
			continue
		}
		return m, nil
	}
}

// handleCmd answers PING (like Conn.RecvMsg) and drops every other command.
func (q *inlineReader) handleCmd(c *Conn, m Msg) error {
	if len(m.Frames) != 1 {
		return fmt.Errorf("zmq4: invalid command with %d frames", len(m.Frames))
	}
	var cmd Cmd
	if err := cmd.unmarshalZMTP(m.Frames[0]); err != nil {
		return fmt.Errorf("zmq4: could not unmarshal ZMTP command: %w", err)
	}
	if cmd.Name == CmdPing {
		return c.SendCmd(CmdPong, nil)
	}
	return nil
}

// maxRetainedScratch is the largest scratch buffer kept between messages.
const maxRetainedScratch = 1 << 20

var _ rpool = (*inlineReader)(nil)
