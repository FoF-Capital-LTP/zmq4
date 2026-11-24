// Copyright 2026 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// Closing a PUB or ROUTER with connected peers must stop every per-peer writer.
func TestCloseStopsPeerWriters(t *testing.T) {
	for _, mk := range []struct {
		name string
		new  func(context.Context) Socket
		peer func(context.Context) Socket
	}{
		{"pub", func(ctx context.Context) Socket { return NewPub(ctx) }, func(ctx context.Context) Socket { return NewSub(ctx, WithInlineRecv()) }},
		{"router", func(ctx context.Context) Socket { return NewRouter(ctx) }, func(ctx context.Context) Socket { return NewDealer(ctx, WithInlineRecv()) }},
	} {
		t.Run(mk.name, func(t *testing.T) {
			runtime.GC()
			before := runtime.NumGoroutine()
			ep := must(EndPoint("tcp"))
			s := mk.new(context.Background())
			if err := s.Listen(ep); err != nil {
				t.Fatal(err)
			}
			peerCtx, cancelPeers := context.WithCancel(context.Background())
			var peers []Socket
			for i := 0; i < 10; i++ {
				p := mk.peer(peerCtx)
				if err := p.Dial(ep); err != nil {
					t.Fatal(err)
				}
				peers = append(peers, p)
			}
			time.Sleep(100 * time.Millisecond) // all registered on the server side
			_ = s.Close()
			cancelPeers()
			for _, p := range peers {
				_ = p.Close()
			}
			deadline := time.Now().Add(3 * time.Second)
			for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
			}
			if after := runtime.NumGoroutine(); after > before {
				buf := make([]byte, 1<<16)
				t.Fatalf("goroutines: %d before, %d after Close\n%s", before, after, buf[:runtime.Stack(buf, true)])
			}
		})
	}
}
