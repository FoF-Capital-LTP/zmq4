// Copyright 2026 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// recvLoop keeps an inline socket receiving, which is what reads PINGs and PONGs.
func recvLoop(s Socket) <-chan error {
	errc := make(chan error, 1)
	go func() {
		for {
			if _, err := s.Recv(); err != nil {
				errc <- err
				return
			}
		}
	}()
	return errc
}

func TestHeartbeatKeepsLiveConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ep := must(EndPoint("tcp"))
	router := NewRouter(ctx, WithInlineRecv())
	defer router.Close()
	if err := router.Listen(ep); err != nil {
		t.Fatal(err)
	}
	routerErr := recvLoop(router)
	dealer := NewDealer(ctx, WithInlineRecv(), WithHeartbeat(10*time.Millisecond, 40*time.Millisecond))
	defer dealer.Close()
	if err := dealer.Dial(ep); err != nil {
		t.Fatal(err)
	}
	dealerErr := recvLoop(dealer)

	// Idle for many timeouts: only PING/PONG flows.
	select {
	case err := <-dealerErr:
		t.Fatalf("dealer connection dropped while the peer was alive: %v", err)
	case err := <-routerErr:
		t.Fatalf("router recv: %v", err)
	case <-time.After(400 * time.Millisecond):
	}
}

func TestHeartbeatClosesSilentConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	// A peer that completes the handshake and then never reads or writes again,
	// like a host that went away without closing TCP.
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		if _, err := Open(c, nullSecurity{}, Router, SocketIdentity(""), true, nil); err != nil {
			return
		}
		<-ctx.Done()
	}()

	const ivl, timeout = 10 * time.Millisecond, 50 * time.Millisecond
	dealer := NewDealer(ctx, WithInlineRecv(), WithHeartbeat(ivl, timeout))
	defer dealer.Close()
	if err := dealer.Dial("tcp://" + ln.Addr().String()); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	select {
	case <-recvLoop(dealer):
		if d := time.Since(start); d < timeout {
			t.Fatalf("closed after %v, before the %v timeout", d, timeout)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("silent connection was not closed")
	}
}

// TestConcurrentSendWithPongs: the reader answers the peer's PINGs while the
// application sends. Each message and each PONG must reach the wire whole, or
// the peer sees a corrupted stream.
func TestConcurrentSendWithPongs(t *testing.T) {
	// A corrupted stream can wedge the reader on a bogus frame length: fail, don't hang.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ep := must(EndPoint("tcp"))
	router := NewRouter(ctx, WithHeartbeat(50*time.Microsecond, 10*time.Second))
	defer router.Close()
	if err := router.Listen(ep); err != nil {
		t.Fatal(err)
	}
	dealer := NewDealer(ctx, WithInlineRecv(), WithTimeout(10*time.Second))
	defer dealer.Close()
	if err := dealer.Dial(ep); err != nil {
		t.Fatal(err)
	}
	dealerErr := recvLoop(dealer)

	const senders, perSender = 4, 3000
	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perSender; i++ {
				// short and long (>255 bytes) frames, several per message
				body := bytes.Repeat([]byte{byte(s)}, 100+i%400)
				msg := NewMsgFrom([]byte(fmt.Sprintf("%d-%d", s, i)), body, body[:i%7])
				if err := dealer.SendMulti(msg); err != nil {
					t.Errorf("send: %v", err)
					return
				}
			}
		}()
	}

	got := 0
	for got < senders*perSender {
		msg, err := router.Recv()
		if err != nil {
			t.Fatalf("router recv after %d messages: %v", got, err)
		}
		if len(msg.Frames) != 4 { // identity + 3
			t.Fatalf("message %d has %d frames, want 4", got, len(msg.Frames))
		}
		var s, i int
		if _, err := fmt.Sscanf(string(msg.Frames[1]), "%d-%d", &s, &i); err != nil {
			t.Fatalf("bad header frame %q", msg.Frames[1])
		}
		want := bytes.Repeat([]byte{byte(s)}, 100+i%400)
		if !bytes.Equal(msg.Frames[2], want) || !bytes.Equal(msg.Frames[3], want[:i%7]) {
			t.Fatalf("message %d-%d corrupted", s, i)
		}
		got++
	}
	wg.Wait()
	select {
	case err := <-dealerErr:
		t.Fatalf("dealer recv: %v", err)
	default:
	}
}

// A peer that only receives (it never sends anything of its own) must stay
// connected while it answers PINGs: the timeout runs from an unanswered PING,
// not from the last frame received. (The earlier logic closed such a peer when
// the first tick fired late under load, before any PING went out; that timing
// cannot be forced here, so this checks the intended behaviour only.)
func TestHeartbeatKeepsReceiveOnlyPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ep := must(EndPoint("tcp"))
	// interval == timeout, as publishserver runs it (30s/30s)
	router := NewRouter(ctx, WithRouterMandatory(), WithHeartbeat(20*time.Millisecond, 20*time.Millisecond))
	defer router.Close()
	if err := router.Listen(ep); err != nil {
		t.Fatal(err)
	}
	dealer := NewDealer(ctx, WithID(SocketIdentity("listener")), WithInlineRecv())
	defer dealer.Close()
	if err := dealer.Dial(ep); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 16)
	go func() {
		for {
			m, err := dealer.Recv()
			if err != nil {
				close(got)
				return
			}
			got <- string(m.Frames[len(m.Frames)-1])
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for router.Send(NewMsgFrom([]byte("listener"), []byte("first"))) != nil {
		if time.Now().After(deadline) {
			t.Fatal("peer never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if s := <-got; s != "first" {
		t.Fatalf("got %q", s)
	}
	time.Sleep(400 * time.Millisecond) // many heartbeat timeouts, no data from the peer
	if err := router.Send(NewMsgFrom([]byte("listener"), []byte("later"))); err != nil {
		t.Fatalf("receive-only peer was dropped: %v", err)
	}
	select {
	case s, ok := <-got:
		if !ok || s != "later" {
			t.Fatalf("got %q, open=%v", s, ok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing received after idle period")
	}
}

// PINGs carry no TTL, as libzmq sends by default: libzmq peers drop the
// connection when nothing follows a PING within its TTL, and with the TTL equal
// to the interval an idle connection raced the next PING and was dropped.
func TestHeartbeatPingHasNoTTL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ep := must(EndPoint("tcp"))
	r := NewRouter(ctx, WithHeartbeat(10*time.Millisecond, 30*time.Second))
	defer r.Close()
	if err := r.Listen(ep); err != nil {
		t.Fatal(err)
	}
	raw, err := net.Dial("tcp", ep[len("tcp://"):])
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	zc, err := Open(raw, nullSecurity{}, Dealer, SocketIdentity("peer"), false, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = raw.SetReadDeadline(time.Now().Add(5 * time.Second))
	m := zc.read()
	if m.err != nil {
		t.Fatal(m.err)
	}
	var cmd Cmd
	if !m.isCmd() || cmd.unmarshalZMTP(m.Frames[0]) != nil || cmd.Name != CmdPing {
		t.Fatalf("expected a PING, got %+v", m)
	}
	if len(cmd.Body) < 2 || cmd.Body[0] != 0 || cmd.Body[1] != 0 {
		t.Fatalf("PING TTL = %v, want 0", cmd.Body)
	}
}
