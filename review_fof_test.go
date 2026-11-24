// Copyright 2026 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// One TCP client that connects and never handshakes must not hold up other
// peers until the handshake timeout.
func TestAcceptHandshakesConcurrently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ep := must(EndPoint("tcp"))
	pub := NewPub(ctx, WithHandshakeTimeout(5*time.Second))
	defer pub.Close()
	if err := pub.Listen(ep); err != nil {
		t.Fatal(err)
	}
	silent, err := net.Dial("tcp", ep[len("tcp://"):])
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	time.Sleep(50 * time.Millisecond) // the silent connection is being handshaken

	sub := NewSub(ctx, WithInlineRecv())
	defer sub.Close()
	start := time.Now()
	if err := sub.Dial(ep); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Dial took %v behind a silent connection", d)
	}
}

// A peer that stops reading wedges the writer inside a blocking write. The
// heartbeat must still close the connection after its timeout.
func TestHeartbeatClosesPeerThatStopsReading(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ep := must(EndPoint("tcp"))
	r := NewRouter(ctx, WithRouterMandatory(), WithHeartbeat(100*time.Millisecond, 300*time.Millisecond), WithSendBufferSize(4096))
	defer r.Close()
	if err := r.Listen(ep); err != nil {
		t.Fatal(err)
	}
	raw, err := net.Dial("tcp", ep[len("tcp://"):])
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	_ = raw.(*net.TCPConn).SetReadBuffer(4096)
	if _, err := Open(raw, nullSecurity{}, Dealer, SocketIdentity("frozen"), false, nil); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for r.Send(NewMsgFrom([]byte("frozen"), []byte("x"))) != nil {
		if time.Now().After(deadline) {
			t.Fatal("peer never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// fill the socket buffers so the writer blocks
	big := make([]byte, 256<<10)
	for i := 0; i < 64; i++ {
		_ = r.Send(NewMsgFrom([]byte("frozen"), big))
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		err := r.Send(NewMsgFrom([]byte("frozen"), []byte("probe")))
		if errors.Is(err, ErrHostUnreachable) {
			return // closed and removed
		}
		if time.Now().After(deadline) {
			t.Fatalf("connection to a peer that stopped reading still up after 10s (last send: %v)", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A peer whose reader stops on a protocol error (not an I/O error) must be
// dropped from the writer too, so a mandatory Send fails instead of queueing.
func TestRouterDropsPeerOnProtocolError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ep := must(EndPoint("tcp"))
	r := NewRouter(ctx, WithRouterMandatory())
	defer r.Close()
	if err := r.Listen(ep); err != nil {
		t.Fatal(err)
	}
	raw, err := net.Dial("tcp", ep[len("tcp://"):])
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	zc, err := Open(raw, nullSecurity{}, Dealer, SocketIdentity("bad"), false, nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for r.Send(NewMsgFrom([]byte("bad"), []byte("x"))) != nil {
		if time.Now().After(deadline) {
			t.Fatal("peer never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// a command message with two frames is a protocol error for the reader
	if err := zc.send(true, []byte("\x04PING"), hasMoreBitFlag); err != nil {
		t.Fatal(err)
	}
	if err := zc.send(true, []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	// Without sending anything (a failed write would also remove the peer), give
	// the reader time to stop: the very first Send must then fail.
	time.Sleep(300 * time.Millisecond)
	if err := r.Send(NewMsgFrom([]byte("bad"), []byte("y"))); !errors.Is(err, ErrHostUnreachable) {
		t.Fatalf("first send after the reader stopped: %v, want ErrHostUnreachable", err)
	}
}
