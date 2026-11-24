// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"net"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// Connected inline sockets run no goroutine of their own: no reader, and no reaper until a connection closes.
func TestInlineSocketsNoIdleGoroutines(t *testing.T) {
	ep := must(EndPoint("tcp"))
	pub := NewPub(context.Background())
	defer pub.Close()
	if err := pub.Listen(ep); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	before := runtime.NumGoroutine()
	var subs []Socket
	for i := 0; i < 50; i++ {
		sub := NewSub(context.Background(), WithInlineRecv(), WithZeroCopyRecv())
		_ = sub.SetOption(OptionSubscribe, "")
		if err := sub.Dial(ep); err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}
	time.Sleep(200 * time.Millisecond)
	// the Go PUB starts goroutines per subscriber on its side: count only what the SUBs add
	var subSide int
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	for _, stack := range splitStacks(buf[:n]) {
		if containsAny(stack, "connReaper", "(*qreader).listen") {
			subSide++
		}
	}
	if subSide != 0 {
		t.Fatalf("%d reader/reaper goroutines for 50 connected inline SUBs (goroutines %d -> %d)", subSide, before, runtime.NumGoroutine())
	}
	for _, s := range subs {
		_ = s.Close()
	}
}

// The reaper still removes closed connections once one closes.
func TestReaperStartsOnFirstClosedConn(t *testing.T) {
	ep := must(EndPoint("tcp"))
	pub := NewPub(context.Background())
	if err := pub.Listen(ep); err != nil {
		t.Fatal(err)
	}
	sub := NewSub(context.Background())
	defer sub.Close()
	_ = sub.SetOption(OptionSubscribe, "")
	if err := sub.Dial(ep); err != nil {
		t.Fatal(err)
	}
	sck := sub.(*subSocket).sck
	sck.reaperCond.L.Lock()
	started := sck.reaperStarted
	sck.reaperCond.L.Unlock()
	if started {
		t.Fatal("reaper started before any connection closed")
	}
	_ = pub.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		sck.mu.RLock()
		n := len(sck.conns)
		sck.mu.RUnlock()
		if n == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("closed connection never removed")
}

func TestRecvBufferSize(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		raw, err := l.Accept()
		if err != nil {
			return
		}
		c, err := Open(raw, nullSecurity{}, Pub, SocketIdentity("pub"), true, nil)
		if err == nil {
			defer c.Close()
			time.Sleep(time.Second)
		}
	}()
	const want = 512 << 10
	sub := NewSub(context.Background(), WithInlineRecv(), WithRecvBufferSize(want))
	defer sub.Close()
	if err := sub.Dial("tcp://" + l.Addr().String()); err != nil {
		t.Fatal(err)
	}
	c := sub.(*subSocket).sck.r.(*inlineReader).current()
	raw, err := c.rw.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var got int
	_ = raw.Control(func(fd uintptr) {
		got, err = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	})
	if err != nil {
		t.Fatal(err)
	}
	// Linux reports twice the requested size (bookkeeping overhead), capped by rmem_max
	if got < want && got < readRmemMax(t)*2 {
		t.Fatalf("SO_RCVBUF = %d, want >= %d", got, want)
	}
}

func splitStacks(b []byte) []string {
	var out []string
	start := 0
	for i := 0; i+1 < len(b); i++ {
		if b[i] == '\n' && b[i+1] == '\n' {
			out = append(out, string(b[start:i]))
			start = i + 2
		}
	}
	return append(out, string(b[start:]))
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}

func readRmemMax(t *testing.T) int {
	t.Helper()
	b, err := readFile("/proc/sys/net/core/rmem_max")
	if err != nil {
		return 1 << 30
	}
	n := 0
	for _, ch := range b {
		if ch < '0' || ch > '9' {
			break
		}
		n = n*10 + int(ch-'0')
	}
	return n
}

func readFile(name string) ([]byte, error) { return os.ReadFile(name) }
