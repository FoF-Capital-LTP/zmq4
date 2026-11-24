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
