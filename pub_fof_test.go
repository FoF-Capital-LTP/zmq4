// Copyright 2026 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

func subscribedConn(topics ...string) *Conn {
	c := &Conn{topics: make(map[string]struct{})}
	for _, t := range topics {
		c.subscribe(NewMsg(append([]byte{1}, t...)))
	}
	return c
}

func TestConnSubscribedTo(t *testing.T) {
	c := subscribedConn("ab", "xyz")
	for topic, want := range map[string]bool{
		"ab": true, "abc": true, "xyz1": true, "a": false, "xy": false, "b": false, "": false,
	} {
		if got := c.subscribedTo([]byte(topic)); got != want {
			t.Errorf("subscribedTo(%q) = %v, want %v", topic, got, want)
		}
	}
	c.subscribe(NewMsg([]byte("\x00ab"))) // unsubscribe
	if c.subscribedTo([]byte("abc")) {
		t.Error("still subscribed to ab after unsubscribing")
	}
	if !c.subscribedTo([]byte("xyz")) {
		t.Error("lost xyz after unsubscribing ab")
	}
	c.subscribe(NewMsg([]byte{1})) // everything
	if !c.subscribedTo([]byte("anything")) || !c.subscribedTo(nil) {
		t.Error("empty subscription must match everything")
	}

	topic := []byte("xyz-some-long-topic")
	if n := testing.AllocsPerRun(100, func() { c.subscribedTo(topic) }); n != 0 {
		t.Errorf("subscribedTo allocates %v times per call", n)
	}
}

// TestPubHWMCountsOnlySubscribed: messages the peer did not subscribe to must not
// take room in its queue, or unrelated traffic makes a slow peer drop its own.
func TestPubHWMCountsOnlySubscribed(t *testing.T) {
	mw := newPubMWriter(context.Background())
	c := subscribedConn("mine")
	queue := make(chan Msg, 10)
	mw.subscribers[c] = queue // no writer goroutine: the peer is stuck
	for i := 0; i < 1000; i++ {
		_ = mw.write(context.Background(), NewMsgString(fmt.Sprintf("other-%d", i)))
	}
	for i := 0; i < 5; i++ {
		_ = mw.write(context.Background(), NewMsgString(fmt.Sprintf("mine-%d", i)))
	}
	if len(queue) != 5 {
		t.Fatalf("queued %d messages, want the 5 subscribed ones", len(queue))
	}
}

// TestPubBatchedDelivery: batched writes keep every subscribed message, in order
// and intact, across short, long and multipart frames.
func TestPubBatchedDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ep := must(EndPoint("tcp"))
	pub := NewPub(ctx)
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
