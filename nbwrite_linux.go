// Copyright 2026 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build linux

package zmq4

import (
	"net"
	"syscall"
	"unsafe"
)

// nbWriter writes ZMTP frames to a socket without ever blocking.
type nbWriter struct {
	rc  syscall.RawConn
	iov []syscall.Iovec
}

// newNBWriter returns a non-blocking writer for c, or nil when its transport
// has no raw socket (inproc) or its security mechanism transforms frames.
func newNBWriter(c *Conn) *nbWriter {
	if c.sec.Type() != NullSecurity {
		return nil
	}
	sc, ok := c.rw.(syscall.Conn)
	if !ok {
		return nil
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return nil
	}
	return &nbWriter{rc: rc}
}

const maxIovecs = 1024 // IOV_MAX on Linux

// write writes msgs as far as the socket takes them without blocking and
// returns what is left (nil when everything went out). Call with c.wmu held.
func (w *nbWriter) write(msgs []Msg) (rest net.Buffers, err error) {
	bufs := frameBuffers(msgs)
	for len(bufs) > 0 {
		w.iov = w.iov[:0]
		for _, b := range bufs {
			if len(w.iov) == maxIovecs {
				break
			}
			if len(b) == 0 {
				continue
			}
			w.iov = append(w.iov, syscall.Iovec{Base: &b[0]})
			w.iov[len(w.iov)-1].SetLen(len(b))
		}
		if len(w.iov) == 0 {
			return nil, nil // only empty frames' bodies left
		}
		var n uintptr
		var errno syscall.Errno
		cerr := w.rc.Write(func(fd uintptr) bool {
			for {
				n, _, errno = syscall.Syscall(syscall.SYS_WRITEV, fd, uintptr(unsafe.Pointer(&w.iov[0])), uintptr(len(w.iov)))
				if errno != syscall.EINTR {
					return true // never wait for writability: the caller hands the rest on
				}
			}
		})
		clear(w.iov) // do not pin message bytes
		if cerr != nil {
			return nil, cerr
		}
		switch errno {
		case 0:
		case syscall.EAGAIN:
			return bufs, nil
		default:
			return nil, errno
		}
		bufs = consume(bufs, int64(n))
		if n == 0 {
			return bufs, nil
		}
	}
	return nil, nil
}

// consume drops the first n bytes of bufs.
func consume(bufs net.Buffers, n int64) net.Buffers {
	for len(bufs) > 0 {
		if l := int64(len(bufs[0])); l <= n {
			n -= l
			bufs = bufs[1:]
			continue
		}
		bufs[0] = bufs[0][n:]
		break
	}
	return bufs
}
