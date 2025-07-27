package protocol

import (
	"net"
	"sync/atomic"
	"time"

	m "github.com/Meander-Cloud/go-elect/message"
)

const (
	tcpWriteDeadline time.Duration = time.Second * 3
)

const (
	typicalBufferLen int    = 1024  // 1 KB
	maxPayloadLen    uint32 = 16384 // 16 KB
)

const (
	protocolPattern byte = 0x59
	protocolVersion byte = 0x01
)

const (
	ServerSenderID byte = 0x01
	ClientSenderID byte = 0x02
)

type ConnVolatileData struct {
	PeerParticipant *m.Participant
	PeerID          string
	Descriptor      string
}

type ConnState struct {
	ConnID uint32
	Conn   net.Conn
	// callers can set pointers but must not modify pointed data, to allow concurrent immutable read
	Data  atomic.Pointer[ConnVolatileData]
	Ready atomic.Bool
}
