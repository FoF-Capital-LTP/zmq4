// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

var ErrClosedConn = errors.New("zmq4: read/write on closed connection")

// connReadBufSize is the size of the per-connection read buffer.
const connReadBufSize = 32 << 10

// defaultMaxRecvFrameSize is the largest frame accepted unless WithMaxRecvFrameSize says otherwise.
const defaultMaxRecvFrameSize = 256 << 20

var errFrameTooLarge = errors.New("zmq4: frame larger than the maximum receive size")

// reader returns the buffered reader of the connection.
// Conns built without Open (tests) fall back to the raw connection.
func (c *Conn) reader() io.Reader {
	if c.r != nil {
		return c.r
	}
	return c.rw
}

// Conn implements the ZeroMQ Message Transport Protocol as defined
// in https://rfc.zeromq.org/spec:23/ZMTP/.
type Conn struct {
	typ SocketType
	id  SocketIdentity
	rw  net.Conn
	r   *bufio.Reader // all reads go through r, so one syscall can fetch many frames
	// frame header scratch: a local array passed to io.ReadFull escapes to the heap on every frame.
	// A connection has a single reader at a time, so a field is enough.
	hdr     [2]byte
	longHdr [8]byte

	maxRecvFrame uint64 // 0 means defaultMaxRecvFrameSize
	sec          Security
	Server       bool
	Meta         Metadata
	Peer         struct {
		Server bool
		Meta   Metadata
	}

	mu     sync.RWMutex
	topics map[string]struct{} // set of subscribed topics

	// wmu makes each message (all its frames) and each command one unit on the
	// wire: the reader answers PINGs while the application may be sending.
	wmu sync.Mutex
	// rxFrames counts received frames; the heartbeat uses it to see traffic.
	rxFrames atomic.Uint64
	hbDone   chan struct{} // closed by Close; nil without a heartbeat
	hbOnce   sync.Once

	closed         int32
	onCloseErrorCB func(c *Conn)
}

func (c *Conn) Close() error {
	if c.hbDone != nil {
		c.hbOnce.Do(func() { close(c.hbDone) })
	}
	return c.rw.Close()
}

func (c *Conn) Read(p []byte) (int, error) {
	if c.Closed() {
		return 0, ErrClosedConn
	}
	n, err := io.ReadFull(c.reader(), p)
	c.checkIO(err)
	return n, err
}

func (c *Conn) Write(p []byte) (int, error) {
	if c.Closed() {
		return 0, ErrClosedConn
	}
	n, err := c.rw.Write(p)
	c.checkIO(err)
	return n, err
}

// Open opens a ZMTP connection over rw with the given security, socket type and identity.
// An optional onCloseErrorCB can be provided to inform the caller when this Conn is closed.
// Open performs a complete ZMTP handshake.
func Open(rw net.Conn, sec Security, sockType SocketType, sockID SocketIdentity, server bool, onCloseErrorCB func(c *Conn)) (*Conn, error) {
	if rw == nil {
		return nil, fmt.Errorf("zmq4: invalid nil read-writer")
	}

	if sec == nil {
		return nil, fmt.Errorf("zmq4: invalid nil security")
	}

	conn := &Conn{
		typ:            sockType,
		id:             sockID,
		rw:             rw,
		r:              bufio.NewReaderSize(rw, connReadBufSize),
		sec:            sec,
		Server:         server,
		Meta:           make(Metadata),
		topics:         make(map[string]struct{}),
		onCloseErrorCB: onCloseErrorCB,
	}
	conn.Meta[sysSockType] = string(conn.typ)
	conn.Meta[sysSockID] = conn.id.String()
	conn.Peer.Meta = make(Metadata)

	err := conn.init(sec)
	if err != nil {
		return nil, fmt.Errorf("zmq4: could not initialize ZMTP connection: %w", err)
	}

	return conn, nil
}

// init performs a ZMTP handshake over an io.ReadWriter
func (conn *Conn) init(sec Security) error {
	var err error

	err = conn.greet(conn.Server)
	if err != nil {
		return fmt.Errorf("zmq4: could not exchange greetings: %w", err)
	}

	err = conn.sec.Handshake(conn, conn.Server)
	if err != nil {
		return fmt.Errorf("zmq4: could not perform security handshake: %w", err)
	}

	peer := SocketType(conn.Peer.Meta[sysSockType])
	if !peer.IsCompatible(conn.typ) {
		return fmt.Errorf("zmq4: peer=%q not compatible with %q", peer, conn.typ)
	}

	// FIXME(sbinet): if security mechanism does not define a client/server
	// topology, enforce that p.server == p.peer.server == 0
	// as per:
	//  https://rfc.zeromq.org/spec:23/ZMTP/#topology

	return nil
}

func (conn *Conn) greet(server bool) error {
	var err error
	send := greeting{Version: defaultVersion}
	send.Sig.Header = sigHeader
	send.Sig.Footer = sigFooter
	kind := string(conn.sec.Type())
	if len(kind) > len(send.Mechanism) {
		return errSecMech
	}
	copy(send.Mechanism[:], kind)

	err = send.write(conn.rw)
	if err != nil {
		conn.checkIO(err)
		return fmt.Errorf("zmq4: could not send greeting: %w", err)
	}

	var recv greeting
	err = recv.read(conn.reader())
	if err != nil {
		conn.checkIO(err)
		return fmt.Errorf("zmq4: could not recv greeting: %w", err)
	}

	peerKind := asString(recv.Mechanism[:])
	if peerKind != kind {
		return errBadSec
	}

	conn.Peer.Server, err = asBool(recv.Server)
	if err != nil {
		return fmt.Errorf("zmq4: could not get peer server flag: %w", err)
	}

	return nil
}

// SendCmd sends a ZMTP command over the wire.
func (c *Conn) SendCmd(name string, body []byte) error {
	if c.Closed() {
		return ErrClosedConn
	}
	cmd := Cmd{Name: name, Body: body}
	buf, err := cmd.marshalZMTP()
	if err != nil {
		return err
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.send(true, buf, 0)
}

// SendMsg sends a ZMTP message over the wire.
func (c *Conn) SendMsg(msg Msg) error {
	if c.Closed() {
		return ErrClosedConn
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if msg.multipart {
		return c.sendMulti(msg)
	}

	nframes := len(msg.Frames)
	for i, frame := range msg.Frames {
		var flag byte
		if i < nframes-1 {
			flag ^= hasMoreBitFlag
		}
		err := c.send(false, frame, flag)
		if err != nil {
			return fmt.Errorf("zmq4: error sending frame %d/%d: %w", i+1, nframes, err)
		}
	}
	return nil
}

// RecvMsg receives a ZMTP message from the wire.
func (c *Conn) RecvMsg() (Msg, error) {
	if c.Closed() {
		return Msg{}, ErrClosedConn
	}
	msg := c.read()
	if msg.err != nil {
		return msg, fmt.Errorf("zmq4: could not read recv msg: %w", msg.err)
	}

	if !msg.isCmd() {
		return msg, nil
	}

	switch len(msg.Frames) {
	case 0:
		msg.err = fmt.Errorf("zmq4: empty command")
		return msg, msg.err
	case 1:
		// ok
	default:
		msg.err = fmt.Errorf("zmq4: invalid length command")
		return msg, msg.err
	}

	var cmd Cmd
	msg.err = cmd.unmarshalZMTP(msg.Frames[0])
	if msg.err != nil {
		return msg, fmt.Errorf("zmq4: could not unmarshal ZMTP recv msg: %w", msg.err)
	}

	switch cmd.Name {
	case CmdPing:
		// send back a PONG immediately.
		msg.err = c.SendCmd(CmdPong, nil)
		if msg.err != nil {
			return msg, msg.err
		}
	}

	switch len(cmd.Body) {
	case 0:
		msg.Frames = nil
	default:
		msg.Frames = msg.Frames[:1]
		msg.Frames[0] = cmd.Body
	}
	return msg, nil
}

func (c *Conn) RecvCmd() (Cmd, error) {
	var cmd Cmd

	if c.Closed() {
		return cmd, ErrClosedConn
	}

	msg := c.read()
	if msg.err != nil {
		return cmd, fmt.Errorf("zmq4: could not read recv cmd: %w", msg.err)
	}

	if !msg.isCmd() {
		return cmd, ErrBadFrame
	}

	switch len(msg.Frames) {
	case 0:
		msg.err = fmt.Errorf("zmq4: empty command")
		return cmd, msg.err
	case 1:
		// ok
	default:
		msg.err = fmt.Errorf("zmq4: invalid length command")
		return cmd, msg.err
	}

	err := cmd.unmarshalZMTP(msg.Frames[0])
	if err != nil {
		return cmd, fmt.Errorf("zmq4: could not unmarshal ZMTP recv cmd: %w", err)
	}

	return cmd, nil
}

func (c *Conn) sendMulti(msg Msg) error {
	var buffers net.Buffers

	nframes := len(msg.Frames)
	for i, frame := range msg.Frames {
		var flag byte
		if i < nframes-1 {
			flag ^= hasMoreBitFlag
		}

		size := len(frame)
		isLong := size > 255
		if isLong {
			flag ^= isLongBitFlag
		}

		var (
			hdr = [8 + 1]byte{flag}
			hsz int
		)
		if isLong {
			hsz = 9
			binary.BigEndian.PutUint64(hdr[1:], uint64(size))
		} else {
			hsz = 2
			hdr[1] = uint8(size)
		}

		switch c.sec.Type() {
		case NullSecurity:
			buffers = append(buffers, hdr[:hsz], frame)
		default:
			var secBuf bytes.Buffer
			if _, err := c.sec.Encrypt(&secBuf, frame); err != nil {
				return err
			}
			buffers = append(buffers, hdr[:hsz], secBuf.Bytes())
		}
	}

	if _, err := buffers.WriteTo(c.rw); err != nil {
		c.checkIO(err)
		return err
	}

	return nil
}

func (c *Conn) send(isCommand bool, body []byte, flag byte) error {
	// Long flag
	size := len(body)
	isLong := size > 255
	if isLong {
		flag ^= isLongBitFlag
	}

	if isCommand {
		flag ^= isCommandBitFlag
	}

	var (
		hdr = [8 + 1]byte{flag}
		hsz int
	)

	// Write out the message itself
	if isLong {
		hsz = 9
		binary.BigEndian.PutUint64(hdr[1:], uint64(size))
	} else {
		hsz = 2
		hdr[1] = uint8(size)
	}
	if _, err := c.rw.Write(hdr[:hsz]); err != nil {
		c.checkIO(err)
		return err
	}

	if _, err := c.sec.Encrypt(c.rw, body); err != nil {
		c.checkIO(err)
		return err
	}

	return nil
}

// readMsg reads the next application message. Commands are not for the
// application: PING is answered, the others are dropped.
func (c *Conn) readMsg() Msg {
	for {
		msg := c.read()
		if msg.err != nil || !msg.isCmd() {
			return msg
		}
		if err := c.handleCmd(msg); err != nil {
			return Msg{err: err}
		}
	}
}

// handleCmd answers PING (like RecvMsg) and ignores every other command.
func (c *Conn) handleCmd(m Msg) error {
	if len(m.Frames) != 1 {
		return fmt.Errorf("zmq4: invalid command with %d frames", len(m.Frames))
	}
	var cmd Cmd
	if err := cmd.unmarshalZMTP(m.Frames[0]); err != nil {
		return fmt.Errorf("zmq4: could not unmarshal ZMTP command: %w", err)
	}
	if cmd.Name == CmdPing {
		return c.SendCmd(CmdPong, nil)
	}
	return nil
}

// read returns the isCommand flag, the body of the message, and optionally an error
func (c *Conn) read() Msg {
	var (
		msg Msg

		hasMore = true
		isCmd   = false
	)

	for hasMore {
		fl, size, err := c.readFrameHeader()
		if err != nil {
			msg.err = err
			return msg
		}
		hasMore = fl.hasMore()
		isCmd = isCmd || fl.isCommand()

		body := make([]byte, size)
		_, msg.err = io.ReadFull(c.reader(), body)
		if msg.err != nil {
			c.checkIO(msg.err)
			return msg
		}

		// fast path for NULL security: we bypass the bytes.Buffer allocation.
		switch c.sec.Type() {
		case NullSecurity: // FIXME(sbinet): also do that for non-encrypted PLAIN?
			msg.Frames = append(msg.Frames, body)
			continue
		}

		buf := new(bytes.Buffer)
		if _, msg.err = c.sec.Decrypt(buf, body); msg.err != nil {
			return msg
		}
		msg.Frames = append(msg.Frames, buf.Bytes())
	}
	if isCmd {
		msg.Type = CmdMsg
	}
	return msg
}

// readFrameHeader reads the flags and the body size of the next frame.
func (c *Conn) readFrameHeader() (flag, uint64, error) {
	if _, err := io.ReadFull(c.reader(), c.hdr[:]); err != nil {
		c.checkIO(err)
		return 0, 0, err
	}
	c.rxFrames.Add(1)
	fl := flag(c.hdr[0])
	size := uint64(c.hdr[1])
	if fl.isLong() {
		// The 2 bytes read so far hold the flags and the first byte of the 8-byte length.
		c.longHdr[0] = c.hdr[1]
		if _, err := io.ReadFull(c.reader(), c.longHdr[1:]); err != nil {
			c.checkIO(err)
			return 0, 0, err
		}
		size = binary.BigEndian.Uint64(c.longHdr[:])
	}
	if size > uint64(maxInt64) {
		return 0, 0, errOverflow
	}
	limit := c.maxRecvFrame
	if limit == 0 {
		limit = defaultMaxRecvFrameSize
	}
	if size > limit {
		return 0, 0, errFrameTooLarge
	}
	return fl, size, nil
}

// readBorrowed reads a message like read, without allocating: the last frame
// points into the connection's read buffer when it fits there, the others are
// copied into scratch. Frames are only valid until the next read on c.
// frames and scratch are reused; the grown scratch is returned.
// It needs the NULL security mechanism and a buffered connection.
func (c *Conn) readBorrowed(frames [][]byte, scratch []byte) (Msg, []byte) {
	msg := Msg{Frames: frames[:0]}
	scratch = scratch[:0]
	isCmd := false
	for hasMore := true; hasMore; {
		fl, size, err := c.readFrameHeader()
		if err != nil {
			msg.err = err
			return msg, scratch
		}
		hasMore = fl.hasMore()
		isCmd = isCmd || fl.isCommand()

		n := int(size)
		var body []byte
		if !hasMore && n <= c.r.Size() {
			// Nothing else is read from c before the caller is done with the message,
			// so the buffered bytes stay put.
			if body, err = c.r.Peek(n); err == nil {
				_, err = c.r.Discard(n)
			}
		} else {
			off := len(scratch)
			if cap(scratch)-off < n {
				grown := make([]byte, off, off+n+off)
				copy(grown, scratch)
				// frames already in the old scratch stay valid: they keep the old array alive
				scratch = grown
			}
			scratch = scratch[:off+n]
			body = scratch[off : off+n : off+n]
			_, err = io.ReadFull(c.r, body)
		}
		if err != nil {
			c.checkIO(err)
			msg.err = err
			return msg, scratch
		}
		msg.Frames = append(msg.Frames, body)
	}
	if isCmd {
		msg.Type = CmdMsg
	}
	return msg, scratch
}

func (conn *Conn) subscribe(msg Msg) {
	conn.mu.Lock()
	v := msg.Frames[0]
	k := string(v[1:])
	switch v[0] {
	case 0:
		delete(conn.topics, k)
	case 1:
		conn.topics[k] = struct{}{}
	}
	conn.mu.Unlock()
}

func (conn *Conn) subscribed(topic string) bool {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	for k := range conn.topics {
		switch {
		case k == "":
			// subscribed to everything
			return true
		case strings.HasPrefix(topic, k):
			return true
		}
	}
	return false
}

func (conn *Conn) SetClosed() {
	if wasClosed := atomic.CompareAndSwapInt32(&conn.closed, 0, 1); wasClosed {
		conn.notifyOnCloseError()
	}
}

func (conn *Conn) Closed() bool {
	return atomic.LoadInt32(&conn.closed) == 1
}

func (conn *Conn) checkIO(err error) {
	if err == nil {
		return
	}

	if err == io.EOF || errors.Is(err, io.EOF) {
		conn.SetClosed()
		return
	}

	var e net.Error
	if errors.As(err, &e); e != nil && !e.Timeout() {
		conn.SetClosed()
	}
}

func (conn *Conn) notifyOnCloseError() {
	if conn.onCloseErrorCB == nil {
		return
	}
	conn.onCloseErrorCB(conn)
}
