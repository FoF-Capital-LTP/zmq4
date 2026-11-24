// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"fmt"
	"sync"
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

	mu     sync.Mutex
	conn   *Conn
	notify chan struct{} // signaled when a connection is added

	stop func() bool
}

func newInlineReader(ctx context.Context) *inlineReader {
	q := &inlineReader{
		ctx:    ctx,
		notify: make(chan struct{}, 1),
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
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.conn
}

func (q *inlineReader) Close() error {
	q.stop()
	q.mu.Lock()
	c := q.conn
	q.conn = nil
	q.mu.Unlock()
	if c != nil {
		return c.Close()
	}
	return nil
}

func (q *inlineReader) addConn(c *Conn) {
	q.mu.Lock()
	old := q.conn
	q.conn = c
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
	if q.conn == c {
		q.conn = nil
	}
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
	q.recvMu.Lock()
	defer q.recvMu.Unlock()
	for {
		c, err := q.wait(ctx)
		if err != nil {
			return err
		}
		m := c.read()
		if m.err != nil {
			q.rmConn(c)
			if err := ctx.Err(); err != nil {
				return err
			}
			*msg = m
			return m.err
		}
		if m.isCmd() {
			if err := q.handleCmd(c, m); err != nil {
				q.rmConn(c)
				_ = c.Close()
				*msg = Msg{err: err}
				return err
			}
			continue
		}
		*msg = m
		return nil
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

var _ rpool = (*inlineReader)(nil)
