package rpc

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

// TCPTransport implements the BlockTransport interface using raw TCP
// connections for maximum block transfer throughput.
//
// Wire protocol per frame:
//   [magic:4][type:1][blockID_len:2][blockID:N][payload_len:4][payload:M]
//
// Types: 1=WRITE_HEADER, 2=DATA_CHUNK, 3=ACK, 4=ERROR

const (
	tcpMagic        = 0x4D484450 // "MHDP" (Mini-HaDooP)
	typeWriteHeader = 1
	typeDataChunk   = 2
	typeAck         = 3
	typeError       = 4
)

// TCPTransport provides raw TCP block data transfer.
type TCPTransport struct {
	pool *ConnPool
}

// NewTCPTransport creates a new TCP-based block transport.
func NewTCPTransport() *TCPTransport {
	return &TCPTransport{
		pool: NewConnPool(4), // 4 idle connections per address
	}
}

// SendBlock streams block data over a raw TCP connection.
func (t *TCPTransport) SendBlock(blockID string, data io.Reader, dataSize int64, pipeline []string) error {
	if len(pipeline) == 0 {
		return fmt.Errorf("empty pipeline")
	}

	conn, err := t.pool.Get(pipeline[0])
	if err != nil {
		return fmt.Errorf("connect to %s: %w", pipeline[0], err)
	}

	// Send header frame
	if err := writeFrame(conn, typeWriteHeader, blockID, nil); err != nil {
		conn.Close()
		return fmt.Errorf("send header: %w", err)
	}

	// Stream data in chunks
	buf := make([]byte, ChunkSize)
	for {
		n, readErr := data.Read(buf)
		if n > 0 {
			if err := writeFrame(conn, typeDataChunk, blockID, buf[:n]); err != nil {
				conn.Close()
				return fmt.Errorf("send chunk: %w", err)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			conn.Close()
			return fmt.Errorf("read data: %w", readErr)
		}
	}

	// Wait for ACK
	ackType, _, _, err := readFrame(conn)
	if err != nil {
		conn.Close()
		return fmt.Errorf("read ack: %w", err)
	}
	if ackType == typeError {
		conn.Close()
		return fmt.Errorf("remote error during block write")
	}
	if ackType != typeAck {
		conn.Close()
		return fmt.Errorf("unexpected frame type %d (expected ACK)", ackType)
	}

	t.pool.Put(pipeline[0], conn)
	return nil
}

// ReceiveBlock reads block data from a source node via raw TCP.
func (t *TCPTransport) ReceiveBlock(blockID string, sourceAddr string) (io.ReadCloser, int64, error) {
	conn, err := t.pool.Get(sourceAddr)
	if err != nil {
		return nil, 0, fmt.Errorf("connect to %s: %w", sourceAddr, err)
	}

	// Send read request (header with blockID)
	if err := writeFrame(conn, typeWriteHeader, blockID, nil); err != nil {
		conn.Close()
		return nil, 0, fmt.Errorf("send read request: %w", err)
	}

	// Return connection as ReadCloser — caller reads frames until EOF
	return &tcpBlockReader{conn: conn, pool: t.pool, addr: sourceAddr}, 0, nil
}

// Close releases all pooled connections.
func (t *TCPTransport) Close() error {
	t.pool.CloseAll()
	return nil
}

// --- Wire protocol helpers ---

func writeFrame(conn net.Conn, frameType byte, blockID string, payload []byte) error {
	// Magic (4) + type (1) + blockID_len (2) + blockID (N) + payload_len (4) + payload (M)
	header := make([]byte, 4+1+2+len(blockID)+4)
	binary.BigEndian.PutUint32(header[0:4], tcpMagic)
	header[4] = frameType
	binary.BigEndian.PutUint16(header[5:7], uint16(len(blockID)))
	copy(header[7:7+len(blockID)], blockID)
	binary.BigEndian.PutUint32(header[7+len(blockID):], uint32(len(payload)))

	if _, err := conn.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func readFrame(conn net.Conn) (frameType byte, blockID string, payload []byte, err error) {
	// Read magic + type + blockID_len
	header := make([]byte, 7)
	if _, err = io.ReadFull(conn, header); err != nil {
		return 0, "", nil, err
	}

	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != tcpMagic {
		return 0, "", nil, fmt.Errorf("bad magic: %x", magic)
	}

	frameType = header[4]
	blockIDLen := binary.BigEndian.Uint16(header[5:7])

	// Read blockID
	blockIDBytes := make([]byte, blockIDLen)
	if _, err = io.ReadFull(conn, blockIDBytes); err != nil {
		return 0, "", nil, err
	}
	blockID = string(blockIDBytes)

	// Read payload length + payload
	payloadLenBuf := make([]byte, 4)
	if _, err = io.ReadFull(conn, payloadLenBuf); err != nil {
		return 0, "", nil, err
	}
	payloadLen := binary.BigEndian.Uint32(payloadLenBuf)

	if payloadLen > 0 {
		payload = make([]byte, payloadLen)
		if _, err = io.ReadFull(conn, payload); err != nil {
			return 0, "", nil, err
		}
	}

	return frameType, blockID, payload, nil
}

// --- TCP Block Reader ---

type tcpBlockReader struct {
	conn net.Conn
	pool *ConnPool
	addr string
	buf  []byte
	pos  int
	done bool // true = reached clean terminal state (ACK frame)
}

func (r *tcpBlockReader) Read(p []byte) (int, error) {
	// Drain current buffer
	if r.pos < len(r.buf) {
		n := copy(p, r.buf[r.pos:])
		r.pos += n
		return n, nil
	}

	// Read next frame
	frameType, _, payload, err := readFrame(r.conn)
	if err != nil {
		return 0, err
	}
	if frameType == typeAck || len(payload) == 0 {
		r.done = true // clean terminal state — safe to pool connection
		return 0, io.EOF
	}
	if frameType != typeDataChunk {
		return 0, fmt.Errorf("unexpected frame type %d", frameType)
	}

	r.buf = payload
	r.pos = 0
	n := copy(p, r.buf)
	r.pos = n
	return n, nil
}

func (r *tcpBlockReader) Close() error {
	if r.done {
		r.pool.Put(r.addr, r.conn) // safe to reuse — protocol at clean boundary
	} else {
		r.conn.Close() // discard — protocol state unknown, prevents corruption
	}
	return nil
}

// --- Connection Pool ---

// ConnPool manages a pool of idle TCP connections per address.
type ConnPool struct {
	mu      sync.Mutex
	conns   map[string][]net.Conn
	maxIdle int
}

// NewConnPool creates a connection pool.
func NewConnPool(maxIdle int) *ConnPool {
	return &ConnPool{
		conns:   make(map[string][]net.Conn),
		maxIdle: maxIdle,
	}
}

// Get returns a pooled connection or creates a new one.
func (p *ConnPool) Get(addr string) (net.Conn, error) {
	p.mu.Lock()
	if conns := p.conns[addr]; len(conns) > 0 {
		conn := conns[len(conns)-1]
		p.conns[addr] = conns[:len(conns)-1]
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	slog.Debug("tcp connection established", "addr", addr)
	return conn, nil
}

// Put returns a connection to the pool.
func (p *ConnPool) Put(addr string, conn net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.conns[addr]) >= p.maxIdle {
		conn.Close()
		return
	}
	p.conns[addr] = append(p.conns[addr], conn)
}

// CloseAll closes all pooled connections.
func (p *ConnPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, conns := range p.conns {
		for _, conn := range conns {
			conn.Close()
		}
		delete(p.conns, addr)
	}
}
