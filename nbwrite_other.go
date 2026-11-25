// Copyright 2026 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !linux

package zmq4

import "net"

// nbWriter is only implemented on Linux; elsewhere every PUB subscriber has
// its own writer goroutine.
type nbWriter struct{}

func newNBWriter(*Conn) *nbWriter { return nil }

func (*nbWriter) write([]Msg) (net.Buffers, error) { panic("unreachable") }
