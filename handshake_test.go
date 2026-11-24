// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// silentPeer accepts TCP connections and never sends a ZMTP greeting, like a
// frozen process whose listening socket is still served by the kernel.
// Each accepted connection is reported on the returned channel.
func silentPeer(t *testing.T) (string, chan net.Conn) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	conns := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { c.Close() })
			conns <- c
		}
	}()
	return "tcp://" + l.Addr().String(), conns
}

// waitClosedByPeer reports whether the dialer closed its end of c within d.
func waitClosedByPeer(c net.Conn, d time.Duration) bool {
	_ = c.SetReadDeadline(time.Now().Add(d))
	buf := make([]byte, 256)
	for {
		if _, err := c.Read(buf); err != nil {
			return errors.Is(err, io.EOF)
		}
	}
}

func TestDialHandshakeTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{"queued", nil},
		{"inline", []Option{WithInlineRecv(), WithZeroCopyRecv()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ep, conns := silentPeer(t)
			sub := NewSub(context.Background(), append(tc.opts, WithHandshakeTimeout(200*time.Millisecond), WithDialerMaxRetries(0))...)
			defer sub.Close()
			start := time.Now()
			err := sub.Dial(ep)
			if err == nil {
				t.Fatal("Dial succeeded without a greeting")
			}
			if d := time.Since(start); d > 3*time.Second {
				t.Fatalf("Dial returned after %v", d)
			}
			// the half-open connection must be closed, not left for the GC
			if !waitClosedByPeer(<-conns, 2*time.Second) {
				t.Fatal("connection not closed after the failed handshake")
			}
		})
	}
}

// Closing the socket (or canceling its context) aborts a handshake in progress.
func TestDialHandshakeCanceled(t *testing.T) {
	ep, conns := silentPeer(t)
	ctx, cancel := context.WithCancel(context.Background())
	sub := NewSub(ctx, WithInlineRecv(), WithZeroCopyRecv())
	defer sub.Close()
	errc := make(chan error, 1)
	go func() { errc <- sub.Dial(ep) }()
	c := <-conns
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("Dial succeeded after cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Dial still blocked in the handshake 2s after cancel")
	}
	if !waitClosedByPeer(c, 2*time.Second) {
		t.Fatal("connection not closed after the canceled handshake")
	}
}

// A corrupted length field must fail the connection, not panic or allocate the claimed size.
func TestRecvFrameTooLarge(t *testing.T) {
	for _, zeroCopy := range []bool{false, true} {
		var sub Socket
		var c *Conn
		if zeroCopy {
			sub, c = zeroCopyPeer(t)
		} else {
			sub, c = inlinePeer(t)
		}
		hdr := []byte{isLongBitFlag, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		if _, err := c.rw.Write(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := sub.Recv(); !errors.Is(err, errFrameTooLarge) {
			t.Fatalf("zeroCopy=%v: got %v, want errFrameTooLarge", zeroCopy, err)
		}
	}
}

// After a message bigger than maxRetainedScratch, the next Recv drops the grown scratch buffer.
func TestZeroCopyScratchShrinks(t *testing.T) {
	sub, c := zeroCopyPeer(t)
	big := NewMsgFrom(patterned(2<<20, 1), patterned(10, 2)) // first frame goes to scratch
	go func() {
		_ = c.SendMsg(big)
		_ = c.SendMsg(NewMsgString("small"))
	}()
	if _, err := sub.Recv(); err != nil {
		t.Fatal(err)
	}
	q := sub.(*subSocket).sck.r.(*inlineReader)
	if cap(q.scratch) <= maxRetainedScratch {
		t.Fatalf("scratch cap %d after a 2MB frame, expected it to have grown", cap(q.scratch))
	}
	msg, err := sub.Recv()
	if err != nil || string(msg.Frames[0]) != "small" {
		t.Fatalf("got %v, %v", msg, err)
	}
	if cap(q.scratch) > maxRetainedScratch {
		t.Fatalf("scratch still %d bytes after a small message", cap(q.scratch))
	}
}
