// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"log"
	"time"
)

// Option configures some aspect of a ZeroMQ socket.
// (e.g. SocketIdentity, Security, ...)
type Option func(s *socket)

// WithID configures a ZeroMQ socket identity.
func WithID(id SocketIdentity) Option {
	return func(s *socket) {
		s.id = id
	}
}

// WithSecurity configures a ZeroMQ socket to use the given security mechanism.
// If the security mechanims is nil, the NULL mechanism is used.
func WithSecurity(sec Security) Option {
	return func(s *socket) {
		s.sec = sec
	}
}

// WithDialerRetry configures the time to wait before two failed attempts
// at dialing an endpoint.
func WithDialerRetry(retry time.Duration) Option {
	return func(s *socket) {
		s.retry = retry
	}
}

// WithDialerTimeout sets the maximum amount of time a dial will wait
// for a connect to complete.
func WithDialerTimeout(timeout time.Duration) Option {
	return func(s *socket) {
		s.dialer.Timeout = timeout
	}
}

// WithTimeout sets the timeout value for socket operations
func WithTimeout(timeout time.Duration) Option {
	return func(s *socket) {
		s.timeout = timeout
	}
}

// WithLogger sets a dedicated log.Logger for the socket.
func WithLogger(msg *log.Logger) Option {
	return func(s *socket) {
		s.log = msg
	}
}

// WithDialerMaxRetries configures the maximum number of retries
// when dialing an endpoint (-1 means infinite retries).
func WithDialerMaxRetries(maxRetries int) Option {
	return func(s *socket) {
		s.maxRetries = maxRetries
	}
}

// WithInlineRecv configures a SUB socket to read messages directly in the
// goroutine calling Recv, instead of through a per-connection reader goroutine
// and a channel. This removes one goroutine hand-off per message (lower
// latency and CPU) and leaves no goroutine behind on Close.
//
// The socket must be connected to a single endpoint and Recv must be called
// from one goroutine at a time. A new connection (for example after an
// automatic reconnect) replaces the previous one. ZMTP commands are handled
// internally and never returned by Recv.
func WithInlineRecv() Option {
	return func(s *socket) {
		s.inlineRecv = true
	}
}

// WithAutomaticReconnect allows to configure a socket to automatically
// reconnect on connection loss.
func WithAutomaticReconnect(automaticReconnect bool) Option {
	return func(s *socket) {
		s.autoReconnect = automaticReconnect
	}
}

/*
// TODO(sbinet)

func WithIOThreads(threads int) Option {
	return nil
}

func WithSendBufferSize(size int) Option {
	return nil
}

func WithRecvBufferSize(size int) Option {
	return nil
}
*/

const (
	OptionSubscribe   = "SUBSCRIBE"
	OptionUnsubscribe = "UNSUBSCRIBE"
	OptionHWM         = "HWM"
)
