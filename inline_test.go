// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

func newInlineSub(t *testing.T, ctx context.Context, ep string, topics ...string) Socket {
	t.Helper()
	sub := NewSub(ctx, WithInlineRecv())
	for _, tp := range topics {
		if err := sub.SetOption(OptionSubscribe, tp); err != nil {
			t.Fatal(err)
		}
	}
	if err := sub.Dial(ep); err != nil {
		t.Fatal(err)
	}
	return sub
}

// waitSubscribed sends probes until the SUB receives one, so later messages are not lost
// to the subscription race.
func waitSubscribed(t *testing.T, pub, sub Socket, topic string) {
	t.Helper()
	got := make(chan struct{})
	go func() {
		for {
			msg, err := sub.Recv()
			if err != nil {
				return
			}
			if string(msg.Frames[0]) == topic+"probe" {
				close(got)
				return
			}
		}
	}()
	deadline := time.After(5 * time.Second)
	for {
		_ = pub.Send(NewMsgString(topic + "probe"))
		select {
		case <-got:
			return
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatal("subscription never became active")
		}
	}
}

func TestInlineRecvOrderAndFilter(t *testing.T) {
	ep := must(EndPoint("tcp"))
	pub := NewPub(context.Background())
	defer pub.Close()
	if err := pub.Listen(ep); err != nil {
		t.Fatal(err)
	}
	sub := newInlineSub(t, context.Background(), ep, "a")
	defer sub.Close()
	waitSubscribed(t, pub, sub, "a")

	const n = 500
	for i := 0; i < n; i++ {
		for _, tp := range []string{"a", "b"} {
			if err := pub.Send(NewMsgString(fmt.Sprintf("%s%d", tp, i))); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 0; i < n; i++ {
		msg, err := sub.Recv()
		if err != nil {
			t.Fatal(err)
		}
		// probes sent before the subscription was confirmed may still arrive first
		if string(msg.Frames[0]) == "aprobe" {
			i--
			continue
		}
		if got, want := string(msg.Frames[0]), fmt.Sprintf("a%d", i); got != want {
			t.Fatalf("message %d: got %q, want %q", i, got, want)
		}
	}
}

func TestInlineRecvUnblocksOnClose(t *testing.T) {
	ep := must(EndPoint("tcp"))
	pub := NewPub(context.Background())
	defer pub.Close()
	if err := pub.Listen(ep); err != nil {
		t.Fatal(err)
	}
	sub := newInlineSub(t, context.Background(), ep, "")
	errc := make(chan error, 1)
	go func() {
		_, err := sub.Recv()
		errc <- err
	}()
	time.Sleep(50 * time.Millisecond)
	_ = sub.Close()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("Recv returned nil error after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recv still blocked after Close")
	}
}

func TestInlineRecvUnblocksOnCancel(t *testing.T) {
	ep := must(EndPoint("tcp"))
	pub := NewPub(context.Background())
	defer pub.Close()
	if err := pub.Listen(ep); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	sub := newInlineSub(t, ctx, ep, "")
	defer sub.Close()
	errc := make(chan error, 1)
	go func() {
		_, err := sub.Recv()
		errc <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recv still blocked after cancel")
	}
}

// With automatic reconnect the new connection replaces the dead one and Recv resumes.
func TestInlineRecvReconnect(t *testing.T) {
	ep := must(EndPoint("tcp"))
	pub := NewPub(context.Background())
	if err := pub.Listen(ep); err != nil {
		t.Fatal(err)
	}
	sub := NewSub(context.Background(), WithInlineRecv(), WithAutomaticReconnect(true),
		WithDialerRetry(50*time.Millisecond), WithDialerMaxRetries(-1))
	defer sub.Close()
	if err := sub.SetOption(OptionSubscribe, ""); err != nil {
		t.Fatal(err)
	}
	if err := sub.Dial(ep); err != nil {
		t.Fatal(err)
	}
	waitSubscribed(t, pub, sub, "")
	_ = pub.Close()
	time.Sleep(200 * time.Millisecond)

	pub = NewPub(context.Background())
	defer pub.Close()
	if err := pub.Listen(ep); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 1)
	go func() {
		for {
			msg, err := sub.Recv()
			if err != nil {
				continue // the error of the dead connection
			}
			if s := string(msg.Frames[0]); s == "after" {
				got <- s
				return
			}
		}
	}()
	deadline := time.After(10 * time.Second)
	for {
		_ = pub.Send(NewMsgString("after"))
		select {
		case <-got:
			return
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatal("no message after the publisher restarted")
		}
	}
}

// A PING from the peer is answered with a PONG and never returned by Recv.
func TestInlineRecvHandlesPing(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	srv := make(chan *Conn, 1)
	go func() {
		raw, err := l.Accept()
		if err != nil {
			return
		}
		c, err := Open(raw, nullSecurity{}, Pub, SocketIdentity("pub"), true, nil)
		if err != nil {
			return
		}
		srv <- c
	}()
	sub := newInlineSub(t, context.Background(), "tcp://"+l.Addr().String(), "")
	defer sub.Close()
	var c *Conn
	select {
	case c = <-srv:
	case <-time.After(5 * time.Second):
		t.Fatal("no connection")
	}
	defer c.Close()
	// the SUB announces its subscription first
	if msg := c.read(); msg.err != nil {
		t.Fatal(msg.err)
	}
	if err := c.SendCmd(CmdPing, []byte{0, 10}); err != nil {
		t.Fatal(err)
	}
	if err := c.SendMsg(NewMsgString("data")); err != nil {
		t.Fatal(err)
	}
	msg, err := sub.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if got := string(msg.Frames[0]); got != "data" {
		t.Fatalf("Recv returned %q, want the data message only", got)
	}
	cmd, err := c.RecvCmd()
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Name != CmdPong {
		t.Fatalf("got command %q, want PONG", cmd.Name)
	}
	if msg.Type != UsrMsg {
		t.Fatal("command delivered as a message")
	}
}
