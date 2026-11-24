// Copyright 2024 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leaks_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
	"go.uber.org/goleak"
)

func freeTCPEndpoint(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return fmt.Sprintf("tcp://%s", l.Addr())
}

// Closing SUB sockets while the publisher keeps sending must not leave reader
// goroutines blocked on their message channel.
func TestSubCloseUnderTrafficNoLeak(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []zmq4.Option
	}{
		{"queued", nil},
		{"inline", []zmq4.Option{zmq4.WithInlineRecv()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Only the SUB side is under test: the Go PUB used as the peer keeps its own writer goroutines.
			defer goleak.VerifyNone(t,
				goleak.IgnoreAnyFunction("github.com/go-zeromq/zmq4.(*pubMWriter).run"),
				goleak.IgnoreAnyFunction("github.com/go-zeromq/zmq4.(*pubMWriter).addConn.func1"))

			ep := freeTCPEndpoint(t)
			pctx, pcancel := context.WithCancel(context.Background())
			pub := zmq4.NewPub(pctx)
			if err := pub.Listen(ep); err != nil {
				t.Fatal(err)
			}

			var subs []zmq4.Socket
			for i := 0; i < 20; i++ {
				sub := zmq4.NewSub(context.Background(), tc.opts...)
				if err := sub.SetOption(zmq4.OptionSubscribe, ""); err != nil {
					t.Fatal(err)
				}
				if err := sub.Dial(ep); err != nil {
					t.Fatal(err)
				}
				subs = append(subs, sub)
			}

			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				msg := zmq4.NewMsg(make([]byte, 400))
				for {
					select {
					case <-stop:
						return
					default:
						_ = pub.Send(msg)
					}
				}
			}()
			// let the reader goroutines fill their queues
			time.Sleep(300 * time.Millisecond)
			for _, sub := range subs {
				_ = sub.Close()
			}
			close(stop)
			wg.Wait()
			_ = pub.Close()
			pcancel()
		})
	}
}
