// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"net"
	"testing"
)

// benchSubRecv measures Recv alone: the peer writes pre-encoded frames straight to the TCP
// connection, so the writer allocates nothing and only the receive path is timed.
func benchSubRecv(b *testing.B, size int, opts ...Option) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
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
	sub := NewSub(context.Background(), opts...)
	defer sub.Close()
	if err := sub.SetOption(OptionSubscribe, ""); err != nil {
		b.Fatal(err)
	}
	if err := sub.Dial("tcp://" + l.Addr().String()); err != nil {
		b.Fatal(err)
	}
	c := <-srv
	defer c.Close()
	if msg := c.read(); msg.err != nil { // the subscription
		b.Fatal(msg.err)
	}
	// one frame: flags + 1-byte length for size <= 255, long form otherwise
	frame := make([]byte, size)
	var hdr []byte
	if size <= 255 {
		hdr = []byte{0, byte(size)}
	} else {
		hdr = []byte{isLongBitFlag, 0, 0, 0, 0, 0, byte(size >> 16), byte(size >> 8), byte(size)}
	}
	const batch = 256
	blob := make([]byte, 0, batch*(len(hdr)+size))
	for i := 0; i < batch; i++ {
		blob = append(blob, hdr...)
		blob = append(blob, frame...)
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if _, err := c.rw.Write(blob); err != nil {
				return
			}
		}
	}()
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg, err := sub.Recv()
		if err != nil {
			b.Fatal(err)
		}
		if len(msg.Frames) != 1 || len(msg.Frames[0]) != size {
			b.Fatalf("bad message: %d frames", len(msg.Frames))
		}
	}
}

func BenchmarkSubRecvQueued400(b *testing.B) { benchSubRecv(b, 400) }
func BenchmarkSubRecvInline400(b *testing.B) { benchSubRecv(b, 400, WithInlineRecv()) }
func BenchmarkSubRecvZeroCopy400(b *testing.B) {
	benchSubRecv(b, 400, WithInlineRecv(), WithZeroCopyRecv())
}
func BenchmarkSubRecvQueued4096(b *testing.B) { benchSubRecv(b, 4096) }
func BenchmarkSubRecvZeroCopy4096(b *testing.B) {
	benchSubRecv(b, 4096, WithInlineRecv(), WithZeroCopyRecv())
}
