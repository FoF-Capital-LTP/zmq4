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

// WithInlineRecv configures a SUB or DEALER socket to read messages directly in the
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

// WithZeroCopyRecv makes Recv on a WithInlineRecv SUB or DEALER socket allocate nothing:
// the frames of the returned message point into buffers owned by the socket and
// are only valid until the next call to Recv. Copy whatever must outlive it.
// It applies to the NULL security mechanism; other mechanisms keep allocating.
func WithZeroCopyRecv() Option {
	return func(s *socket) {
		s.zeroCopyRecv = true
	}
}

// WithHandshakeTimeout bounds the ZMTP greeting and security handshake of each
// new connection (30s by default, like libzmq's ZMQ_HANDSHAKE_IVL).
// A connection that does not complete it in time is closed. 0 disables the limit.
func WithHandshakeTimeout(d time.Duration) Option {
	return func(s *socket) {
		s.handshakeTimeout = d
	}
}

// WithMaxRecvFrameSize caps the size of a received frame (256MiB by default).
// A larger frame, typically a corrupted length field, fails the connection
// instead of allocating that much memory.
func WithMaxRecvFrameSize(n int64) Option {
	return func(s *socket) {
		if n > 0 {
			s.maxRecvFrame = uint64(n)
		}
	}
}

// WithHeartbeat sends a ZMTP PING on each connection every ivl and closes a
// connection on which nothing has been received for timeout (rounded up to a
// multiple of ivl), like libzmq's ZMQ_HEARTBEAT_IVL and ZMQ_HEARTBEAT_TIMEOUT.
// Peers answer PINGs with PONGs, so a live connection always has traffic.
// timeout below ivl is raised to ivl. ivl 0 (the default) disables it.
// With WithInlineRecv nothing is read outside Recv, so keep a goroutine in Recv
// or a live connection looks silent and gets closed.
func WithHeartbeat(ivl, timeout time.Duration) Option {
	return func(s *socket) {
		if ivl <= 0 {
			s.hbIvl, s.hbTimeout = 0, 0
			return
		}
		s.hbIvl, s.hbTimeout = ivl, max(timeout, ivl)
	}
}

// WithAutomaticReconnect allows to configure a socket to automatically
// reconnect on connection loss.
func WithAutomaticReconnect(automaticReconnect bool) Option {
	return func(s *socket) {
		s.autoReconnect = automaticReconnect
	}
}

// WithRecvBufferSize sets the kernel receive buffer (SO_RCVBUF) of the
// socket's TCP connections, like libzmq's ZMQ_RCVBUF. On Linux an explicit
// size turns off receive buffer auto-tuning and is capped by net.core.rmem_max.
// 0 keeps the OS default.
func WithRecvBufferSize(size int) Option {
	return func(s *socket) {
		s.recvBufSize = size
	}
}

// WithSendBufferSize sets the kernel send buffer (SO_SNDBUF) of the socket's
// TCP connections, like libzmq's ZMQ_SNDBUF. 0 keeps the OS default.
func WithSendBufferSize(size int) Option {
	return func(s *socket) {
		s.sendBufSize = size
	}
}

/*
// TODO(sbinet)

func WithIOThreads(threads int) Option {
	return nil
}
*/

const (
	OptionSubscribe   = "SUBSCRIBE"
	OptionUnsubscribe = "UNSUBSCRIBE"
	OptionHWM         = "HWM"
)
