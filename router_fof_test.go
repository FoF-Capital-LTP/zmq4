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

func dealerWithID(t *testing.T, ctx context.Context, ep, id string) Socket {
	t.Helper()
	d := NewDealer(ctx, WithID(SocketIdentity(id)), WithInlineRecv())
	if err := d.Dial(ep); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// waitPeer sends to id until the ROUTER knows it (Dial returns before the ROUTER
// side has registered the connection).
func waitPeer(t *testing.T, r Socket, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := r.Send(NewMsgFrom([]byte(id), []byte("ping")))
		if err == nil {
			return
		}
		if !errors.Is(err, ErrHostUnreachable) || time.Now().After(deadline) {
			t.Fatalf("peer %s never registered: %v", id, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func recvBody(t *testing.T, d Socket) string {
	t.Helper()
	got := make(chan string, 1)
	go func() {
		m, err := d.Recv()
		if err != nil {
			got <- "error: " + err.Error()
			return
		}
		got <- string(m.Frames[len(m.Frames)-1])
	}()
	select {
	case s := <-got:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("nothing received")
		return ""
	}
}

func TestRouterMandatory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ep := must(EndPoint("tcp"))
	r := NewRouter(ctx, WithRouterMandatory())
	defer r.Close()
	if err := r.Listen(ep); err != nil {
		t.Fatal(err)
	}
	if err := r.Send(NewMsgFrom([]byte("nobody"), []byte("x"))); !errors.Is(err, ErrHostUnreachable) {
		t.Fatalf("send to unknown peer: %v, want ErrHostUnreachable", err)
	}
	d := dealerWithID(t, ctx, ep, "a")
	waitPeer(t, r, "a")
	if got := recvBody(t, d); got != "ping" {
		t.Fatalf("dealer got %q", got)
	}

	// Without mandatory the same send is dropped silently.
	r2 := NewRouter(ctx)
	defer r2.Close()
	if err := r2.Send(NewMsgFrom([]byte("nobody"), []byte("x"))); err != nil {
		t.Fatalf("send to unknown peer without mandatory: %v", err)
	}
}

func TestRouterHandover(t *testing.T) {
	for _, handover := range []bool{true, false} {
		t.Run(map[bool]string{true: "handover", false: "refuse"}[handover], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ep := must(EndPoint("tcp"))
			opts := []Option{WithRouterMandatory()}
			if handover {
				opts = append(opts, WithRouterHandover())
			}
			r := NewRouter(ctx, opts...)
			defer r.Close()
			if err := r.Listen(ep); err != nil {
				t.Fatal(err)
			}
			first := dealerWithID(t, ctx, ep, "same")
			waitPeer(t, r, "same")
			if got := recvBody(t, first); got != "ping" {
				t.Fatalf("first got %q", got)
			}
			second := dealerWithID(t, ctx, ep, "same")
			// the second connection is registered (or refused) asynchronously
			time.Sleep(100 * time.Millisecond)
			if err := r.Send(NewMsgFrom([]byte("same"), []byte("after"))); err != nil {
				t.Fatalf("send after second connect: %v", err)
			}
			want, other := second, first
			if !handover {
				want, other = first, second
			}
			if got := recvBody(t, want); got != "after" {
				t.Fatalf("expected peer got %q", got)
			}
			// the other one must not get it
			got := make(chan struct{})
			go func() {
				if m, err := other.Recv(); err == nil && string(m.Frames[len(m.Frames)-1]) == "after" {
					close(got)
				}
			}()
			select {
			case <-got:
				t.Fatal("message went to the wrong peer")
			case <-time.After(200 * time.Millisecond):
			}
		})
	}
}

// A peer that stops reading fills its own queue; sends to other peers go on.
func TestRouterStuckPeerDoesNotBlockOthers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ep := must(EndPoint("tcp"))
	r := NewRouter(ctx, WithRouterMandatory())
	defer r.Close()
	if err := r.Listen(ep); err != nil {
		t.Fatal(err)
	}
	// a raw peer that handshakes and never reads again
	addr := ep[len("tcp://"):]
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := Open(raw, nullSecurity{}, Dealer, SocketIdentity("stuck"), false, nil); err != nil {
		t.Fatal(err)
	}
	good := dealerWithID(t, ctx, ep, "good")
	waitPeer(t, r, "good")
	_ = recvBody(t, good)
	waitPeer(t, r, "stuck")

	big := make([]byte, 64<<10)
	full := false
	start := time.Now()
	for i := 0; i < 100000 && !full; i++ {
		err := r.Send(NewMsgFrom([]byte("stuck"), big))
		switch {
		case errors.Is(err, ErrQueueFull):
			full = true
		case err != nil:
			t.Fatalf("send to stuck peer: %v", err)
		}
	}
	if !full {
		t.Fatal("stuck peer's queue never filled")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("filling the stuck peer took %v: sends blocked", d)
	}
	if err := r.Send(NewMsgFrom([]byte("good"), []byte("still here"))); err != nil {
		t.Fatalf("send to good peer: %v", err)
	}
	if got := recvBody(t, good); got != "still here" {
		t.Fatalf("good peer got %q", got)
	}
}
