// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"bytes"
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

// zeroCopyPeer accepts the SUB of a WithInlineRecv+WithZeroCopyRecv socket and returns the
// server side of the ZMTP connection, so the test can write exact frames.
func zeroCopyPeer(t *testing.T) (Socket, *Conn) {
	t.Helper()
	return subPeer(t, WithInlineRecv(), WithZeroCopyRecv())
}

// inlinePeer is zeroCopyPeer without zero-copy: Recv allocates every frame.
func inlinePeer(t *testing.T) (Socket, *Conn) {
	t.Helper()
	return subPeer(t, WithInlineRecv())
}

func subPeer(t *testing.T, opts ...Option) (Socket, *Conn) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
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
	sub := NewSub(context.Background(), opts...)
	t.Cleanup(func() { sub.Close() })
	if err := sub.SetOption(OptionSubscribe, ""); err != nil {
		t.Fatal(err)
	}
	if err := sub.Dial("tcp://" + l.Addr().String()); err != nil {
		t.Fatal(err)
	}
	var c *Conn
	select {
	case c = <-srv:
	case <-time.After(5 * time.Second):
		t.Fatal("no connection")
	}
	t.Cleanup(func() { c.Close() })
	if msg := c.read(); msg.err != nil { // the subscription
		t.Fatal(msg.err)
	}
	return sub, c
}

func patterned(n, seed int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + seed)
	}
	return b
}

// Frames of every size class, single- and multi-frame messages, with commands in between:
// each message must read back exactly, including frames larger than the read buffer.
func TestInlineZeroCopyRecvFrames(t *testing.T) {
	sub, c := zeroCopyPeer(t)
	sizes := []int{0, 1, 255, 256, 400, connReadBufSize - 1, connReadBufSize, connReadBufSize + 1, 1 << 20}
	var want [][][]byte
	for i, n := range sizes {
		want = append(want, [][]byte{patterned(n, i)})
	}
	want = append(want,
		[][]byte{patterned(10, 1), patterned(400, 2)},
		[][]byte{patterned(connReadBufSize+5, 3), patterned(7, 4), patterned(connReadBufSize+9, 5)},
		[][]byte{patterned(0, 6), patterned(300, 7)},
	)
	// many small messages after the large ones: the read buffer has to refill and move
	for i := 0; i < 2000; i++ {
		want = append(want, [][]byte{patterned(100+i%400, i)})
	}
	go func() {
		for i, frames := range want {
			if i%97 == 0 {
				_ = c.SendCmd(CmdPing, []byte{0, 1})
			}
			if err := c.SendMsg(NewMsgFrom(frames...)); err != nil {
				return
			}
		}
	}()
	for i, frames := range want {
		msg, err := sub.Recv()
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if len(msg.Frames) != len(frames) {
			t.Fatalf("message %d: %d frames, want %d", i, len(msg.Frames), len(frames))
		}
		for j := range frames {
			if !bytes.Equal(msg.Frames[j], frames[j]) {
				t.Fatalf("message %d frame %d (%d bytes): content differs", i, j, len(frames[j]))
			}
		}
	}
}

// A WithInlineRecv DEALER talks to a ROUTER; Send may run concurrently with a blocked Recv.
func TestInlineDealerRoundTrip(t *testing.T) {
	ep := must(EndPoint("tcp"))
	router := NewRouter(context.Background(), WithID(SocketIdentity("router")))
	defer router.Close()
	if err := router.Listen(ep); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			msg, err := router.Recv()
			if err != nil {
				return
			}
			// [identity][payload] -> echo back with a suffix
			reply := NewMsgFrom(msg.Frames[0], append(append([]byte{}, msg.Frames[len(msg.Frames)-1]...), "-ok"...))
			if err := router.Send(reply); err != nil {
				return
			}
		}
	}()
	dealer := NewDealer(context.Background(), WithInlineRecv())
	defer dealer.Close()
	if err := dealer.Dial(ep); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 100)
	go func() {
		for {
			msg, err := dealer.Recv()
			if err != nil {
				return
			}
			got <- string(msg.Frames[len(msg.Frames)-1])
		}
	}()
	const n = 50
	for i := 0; i < n; i++ {
		if err := dealer.Send(NewMsgString(fmt.Sprintf("req%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		select {
		case s := <-got:
			seen[s] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d replies", i)
		}
	}
	for i := 0; i < n; i++ {
		if !seen[fmt.Sprintf("req%d-ok", i)] {
			t.Fatalf("missing reply %d", i)
		}
	}
}
