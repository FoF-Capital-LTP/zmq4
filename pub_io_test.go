// Copyright 2026 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func seqMsg(i int) Msg {
	body := bytes.Repeat([]byte{byte(i)}, 4096)
	binary.BigEndian.PutUint32(body, uint32(i))
	return NewMsgFrom([]byte("t"), body)
}

func checkSeqMsg(t *testing.T, m Msg) int {
	t.Helper()
	if len(m.Frames) != 2 || string(m.Frames[0]) != "t" || len(m.Frames[1]) != 4096 {
		t.Fatalf("malformed message: %d frames", len(m.Frames))
	}
	i := int(binary.BigEndian.Uint32(m.Frames[1]))
	if !bytes.Equal(m.Frames[1][4:], bytes.Repeat([]byte{byte(i)}, 4092)) {
		t.Fatalf("message %d corrupted", i)
	}
	return i
}

// A subscriber that stops reading must not hold up the others; when it reads
// again it gets intact messages in order (with gaps where its queue was full).
func TestPubLoopSlowSubscriber(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ep := must(EndPoint("tcp"))
	pub := NewPub(ctx, WithPubIOLoops(1))
	defer pub.Close()
	if err := pub.Listen(ep); err != nil {
		t.Fatal(err)
	}
	if len(pub.(*pubSocket).sck.w.(*pubMWriter).loops) != 1 {
		t.Fatal("loops not enabled")
	}

	raw, err := net.Dial("tcp", ep[len("tcp://"):])
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	frozen, err := Open(raw, nullSecurity{}, Sub, SocketIdentity(""), false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := frozen.SendMsg(NewMsg([]byte{1})); err != nil { // subscribe to everything
		t.Fatal(err)
	}

	good := NewSub(ctx, WithInlineRecv())
	defer good.Close()
	_ = good.SetOption(OptionSubscribe, "")
	if err := good.Dial(ep); err != nil {
		t.Fatal(err)
	}
	// both subscriptions reach the PUB
	deadline := time.Now().Add(5 * time.Second)
	for {
		w := pub.(*pubSocket).sck.w.(*pubMWriter)
		w.mu.RLock()
		n := len(w.index.peers[""])
		w.mu.RUnlock()
		if n == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d subscribers registered, want 2", n)
		}
		time.Sleep(5 * time.Millisecond)
	}

	const n = 6000 // 24MB: more than the frozen subscriber's socket buffers hold
	recvd := make(chan error, 1)
	var got atomic.Int64
	go func() {
		for i := 0; i < n; i++ {
			got.Store(int64(i))
			m, err := good.Recv()
			if err != nil {
				recvd <- err
				return
			}
			if got := checkSeqMsg(t, m); got != i {
				t.Errorf("good subscriber got message %d, want %d", got, i)
				recvd <- nil
				return
			}
		}
		recvd <- nil
	}()
	start := time.Now()
	for i := 0; i < n; i += 10 {
		batch := make([]Msg, 0, 10)
		for j := i; j < i+10; j++ {
			batch = append(batch, seqMsg(j))
		}
		if err := pub.(*pubSocket).SendBatch(batch); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond) // let the good subscriber keep up (its own HWM is not under test)
	}
	select {
	case err := <-recvd:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("good subscriber held up by the frozen one: waiting for message %d of %d", got.Load(), n)
	}
	t.Logf("good subscriber got %d messages in %v", n, time.Since(start).Round(time.Millisecond))

	// the frozen subscriber now reads what it was sent: in order, intact
	last, count := -1, 0
	for {
		_ = raw.SetReadDeadline(time.Now().Add(time.Second)) // idle for a second: drained
		m := frozen.readMsg()
		if m.err != nil {
			break // deadline: drained
		}
		i := checkSeqMsg(t, m)
		if i <= last {
			t.Fatalf("frozen subscriber got %d after %d", i, last)
		}
		last, count = i, count+1
	}
	// it gets what its socket buffers held plus its whole queue (up to the HWM)
	if count < DefaultSendHwm {
		t.Fatalf("frozen subscriber got only %d messages", count)
	}
	t.Logf("frozen subscriber got %d messages, last %d", count, last)
}
