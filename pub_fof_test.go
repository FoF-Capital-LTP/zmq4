// Copyright 2026 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"
)

func subscribedConn(topics ...string) *Conn {
	rw, _ := net.Pipe() // only Close is used
	c := &Conn{topics: make(map[string]struct{}), rw: rw}
	for _, t := range topics {
		c.subscribe(NewMsg(append([]byte{1}, t...)))
	}
	return c
}

// queued returns the first frames of what p has queued.
func queued(p *pubPeer) (out []string) {
	for _, m := range p.q {
		out = append(out, string(m.Frames[0]))
	}
	return out
}

func TestPubIndexMatching(t *testing.T) {
	mw := newPubMWriter(context.Background(), 0)
	p := stuckPeer(mw, subscribedConn("ab", "xyz"))
	msgs := []string{"ab", "abc", "xyz1", "a", "xy", "b", ""}
	for _, m := range msgs {
		_ = mw.write(context.Background(), NewMsgString(m))
	}
	if g := queued(p); !slices.Equal(g, []string{"ab", "abc", "xyz1"}) {
		t.Fatalf("matched %v", g)
	}
	p.q = p.q[:0]
	unsubscribe(mw, p.conn, "ab")
	_ = mw.write(context.Background(), NewMsgString("abc"))
	_ = mw.write(context.Background(), NewMsgString("xyz"))
	if g := queued(p); !slices.Equal(g, []string{"xyz"}) {
		t.Fatalf("after unsubscribing ab matched %v", g)
	}
	if len(mw.index.lens) != 1 || mw.index.lens[0] != 3 {
		t.Fatalf("prefix lengths %v after unsubscribing ab, want [3]", mw.index.lens)
	}
	p.q = p.q[:0]
	subscribe(mw, p.conn, "") // everything
	_ = mw.write(context.Background(), NewMsgString("anything"))
	if g := queued(p); !slices.Equal(g, []string{"anything"}) {
		t.Fatalf("empty subscription matched %v", g)
	}
	// removing the peer removes its prefixes
	mw.rmConn(p.conn)
	if len(mw.index.peers) != 0 || len(mw.index.lens) != 0 {
		t.Fatalf("index not empty after rmConn: %v %v", mw.index.peers, mw.index.lens)
	}
}

func subscribe(mw *pubMWriter, c *Conn, topic string) {
	if t, added, changed := c.subscribe(NewMsg(append([]byte{1}, topic...))); changed {
		mw.subscribed(c, t, added)
	}
}

func unsubscribe(mw *pubMWriter, c *Conn, topic string) {
	if t, added, changed := c.subscribe(NewMsg(append([]byte{0}, topic...))); changed {
		mw.subscribed(c, t, added)
	}
}

// stuckPeer registers c with mw without a writer goroutine: the peer never drains.
func stuckPeer(mw *pubMWriter, c *Conn) *pubPeer {
	p := &pubPeer{connQueue: newConnQueue(context.Background(), c)}
	mw.mu.Lock()
	mw.peers[c] = p
	c.mu.RLock()
	for t := range c.topics {
		mw.index.add(t, p)
	}
	c.mu.RUnlock()
	mw.mu.Unlock()
	return p
}

// TestPubHWMCountsOnlySubscribed: messages the peer did not subscribe to must not
// take room in its queue, or unrelated traffic makes a slow peer drop its own.
func TestPubHWMCountsOnlySubscribed(t *testing.T) {
	mw := newPubMWriter(context.Background(), 0)
	mw.hwm.Store(10)
	p := stuckPeer(mw, subscribedConn("mine"))
	for i := 0; i < 1000; i++ {
		_ = mw.write(context.Background(), NewMsgString(fmt.Sprintf("other-%d", i)))
	}
	for i := 0; i < 5; i++ {
		_ = mw.write(context.Background(), NewMsgString(fmt.Sprintf("mine-%d", i)))
	}
	if len(p.q) != 5 {
		t.Fatalf("queued %d messages, want the 5 subscribed ones", len(p.q))
	}
	for i := 0; i < 20; i++ {
		_ = mw.write(context.Background(), NewMsgString("mine-more"))
	}
	if len(p.q) != 10 {
		t.Fatalf("queued %d messages, want the high-water mark of 10", len(p.q))
	}
}

// A peer subscribed to several prefixes of a topic gets the message once, and a
// batch wakes each peer once.
func TestPubWriteBatchDedupAndWake(t *testing.T) {
	mw := newPubMWriter(context.Background(), 0)
	a := stuckPeer(mw, subscribedConn("a", "ab", "abc"))
	b := stuckPeer(mw, subscribedConn("x"))
	all := stuckPeer(mw, subscribedConn(""))
	msgs := []Msg{NewMsgString("abc-1"), NewMsgString("x-1"), NewMsgString("abd-2"), NewMsgString("zzz")}
	_ = mw.writeBatch(context.Background(), msgs)
	got := func(p *pubPeer) (out []string) {
		for _, m := range p.q {
			out = append(out, string(m.Frames[0]))
		}
		return out
	}
	if g := got(a); !slices.Equal(g, []string{"abc-1", "abd-2"}) {
		t.Errorf("a got %v", g)
	}
	if g := got(b); !slices.Equal(g, []string{"x-1"}) {
		t.Errorf("b got %v", g)
	}
	if g := got(all); len(g) != 4 {
		t.Errorf("all got %v", g)
	}
	for name, p := range map[string]*pubPeer{"a": a, "b": b, "all": all} {
		if len(p.wake) != 1 {
			t.Errorf("%s not woken", name)
		}
	}
	for _, topic := range []string{"a", "ab", "abc"} {
		unsubscribe(mw, a.conn, topic)
	}
	a.q = a.q[:0]
	_ = mw.write(context.Background(), NewMsgString("abc-3"))
	if len(a.q) != 0 {
		t.Errorf("unsubscribed peer still gets %v", got(a))
	}
}

// TestPubBatchedDelivery: batched writes keep every subscribed message, in order
// and intact, across short, long and multipart frames.
func TestPubBatchedDelivery(t *testing.T) {
	for _, loops := range []int{0, 1, 3} {
		t.Run(fmt.Sprintf("io-loops=%d", loops), func(t *testing.T) { testPubBatchedDelivery(t, loops) })
	}
}

func testPubBatchedDelivery(t *testing.T, loops int) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ep := must(EndPoint("tcp"))
	pub := NewPub(ctx, WithPubIOLoops(loops))
	defer pub.Close()
	if err := pub.Listen(ep); err != nil {
		t.Fatal(err)
	}
	_ = pub.SetOption(OptionHWM, 1<<20) // this test is about batching, not dropping
	sub := NewSub(ctx, WithInlineRecv())
	defer sub.Close()
	if err := sub.SetOption(OptionSubscribe, "a"); err != nil {
		t.Fatal(err)
	}
	if err := sub.Dial(ep); err != nil {
		t.Fatal(err)
	}
	// wait until the subscription reached the PUB
	ready := make(chan error, 1)
	go func() {
		for {
			m, err := sub.Recv()
			if err != nil || string(m.Frames[0]) == "a-ready" {
				ready <- err
				return
			}
		}
	}()
wait:
	for {
		_ = pub.Send(NewMsgString("a-ready"))
		select {
		case err := <-ready:
			if err != nil {
				t.Fatal(err)
			}
			break wait
		case <-time.After(20 * time.Millisecond):
		}
	}

	const n = 20000
	go func() {
		for i := 0; i < n; i++ {
			body := bytes.Repeat([]byte{byte(i)}, i%600)
			_ = pub.SendMulti(NewMsgFrom([]byte(fmt.Sprintf("b-%d", i)), body)) // not subscribed
			_ = pub.SendMulti(NewMsgFrom([]byte(fmt.Sprintf("a-%d", i)), body, []byte("end")))
		}
	}()
	for i := 0; i < n; i++ {
		m, err := sub.Recv()
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		if string(m.Frames[0]) == "a-ready" {
			i--
			continue
		}
		if len(m.Frames) != 3 || string(m.Frames[0]) != fmt.Sprintf("a-%d", i) ||
			!bytes.Equal(m.Frames[1], bytes.Repeat([]byte{byte(i)}, i%600)) || string(m.Frames[2]) != "end" {
			t.Fatalf("message %d wrong or out of order: %q...", i, m.Frames[0])
		}
	}
}

// Concurrent senders: a peer matching through several prefixes still gets each
// message exactly once.
func TestPubConcurrentWriteBatchNoDuplicates(t *testing.T) {
	mw := newPubMWriter(context.Background(), 0)
	mw.hwm.Store(1 << 30)
	p := stuckPeer(mw, subscribedConn("", "a", "ab"))
	const senders, each = 4, 5000
	var wg sync.WaitGroup
	for g := 0; g < senders; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				_ = mw.write(context.Background(), NewMsgString("abc"))
			}
		}()
	}
	wg.Wait()
	if n := len(p.q); n != senders*each {
		t.Fatalf("peer got %d messages, want %d", n, senders*each)
	}
}

// Many subscribers reconnecting with many topics each: subscriptions must be
// indexed in time linear in their number, not quadratic.
func TestPubSubscriptionStorm(t *testing.T) {
	mw := newPubMWriter(context.Background(), 0)
	const peers, topics = 200, 300
	start := time.Now()
	for i := 0; i < peers; i++ {
		c := subscribedConn()
		stuckPeer(mw, c)
		for j := 0; j < topics; j++ {
			subscribe(mw, c, fmt.Sprintf("topic-%03d", j))
		}
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("indexing %d subscriptions took %v", peers*topics, d)
	}
	if n := len(mw.index.peers["topic-007"]); n != peers {
		t.Fatalf("topic-007 has %d peers, want %d", n, peers)
	}
}
