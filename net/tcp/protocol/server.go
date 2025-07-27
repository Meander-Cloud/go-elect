package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/Meander-Cloud/go-transport/tcp"

	"github.com/Meander-Cloud/go-elect/arbiter"
	m "github.com/Meander-Cloud/go-elect/message"
)

type ServerHandler interface {
	ParticipantInit(*Server, *ConnState, *m.ParticipantInit)
	ParticipantExit(*Server, *ConnState, *m.ParticipantExit)
	CandidateVoteResponse(*Server, *ConnState, *m.CandidateVoteResponse)
	NomineeAckResponse(*Server, *ConnState, *m.NomineeAckResponse)
}

type ServerOptions struct {
	*tcp.Options
	Arbiter *arbiter.Arbiter
	ServerHandler

	Txid            byte
	RxidMap         map[byte]struct{}
	ReconnectWindow time.Duration

	SelfParticipant *m.Participant
	SelfID          string
}

type Server struct {
	options    *ServerOptions
	inShutdown atomic.Bool

	// if increment overflow will wrap to zero
	connIDGen atomic.Uint32
	txseqGen  atomic.Uint64

	mutex   sync.Mutex
	connMap map[uint32]*ConnState // connID -> tcp connection state
}

func NewServer(options *ServerOptions) (*Server, error) {
	if options.Arbiter == nil {
		err := fmt.Errorf("%s: nil Arbiter", options.LogPrefix)
		log.Printf("%s", err.Error())
		return nil, err
	}

	if options.SelfParticipant == nil {
		err := fmt.Errorf("%s: nil SelfParticipant", options.LogPrefix)
		log.Printf("%s", err.Error())
		return nil, err
	}

	if options.SelfID == "" {
		err := fmt.Errorf("%s: invalid SelfID", options.LogPrefix)
		log.Printf("%s", err.Error())
		return nil, err
	}

	p := &Server{
		options:    options,
		inShutdown: atomic.Bool{},

		connIDGen: atomic.Uint32{},
		txseqGen:  atomic.Uint64{},

		mutex:   sync.Mutex{},
		connMap: make(map[uint32]*ConnState),
	}

	return p, nil
}

func (p *Server) Options() *ServerOptions {
	return p.options
}

func (p *Server) Close() {
	log.Printf("%s: protocol closing", p.options.LogPrefix)
	p.inShutdown.Store(true)

	var exitwg sync.WaitGroup

	// send ParticipantExit
	func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()

		for _, volatileConnState := range p.connMap {
			scopedConnState := volatileConnState
			scopedConnState.Ready.Store(false)

			exitwg.Add(1)
			err := p.options.Arbiter.Dispatch(
				func() {
					// invoked on arbiter goroutine
					defer exitwg.Done()

					writeWireData(
						p.options.LogPrefix,
						p.options.Txid,
						scopedConnState,
						&m.Message{
							Txseq:  p.GetNextTxseq(),
							Txtime: time.Now().UTC().UnixMilli(),

							ParticipantExit: &m.ParticipantExit{
								InShutdown: true,
							},
						},
					)
				},
			)
			if err != nil {
				exitwg.Done()
			}
		}
	}()

	// wait until all peer updates are sent plus one second grace period
	exitwg.Wait()
	<-time.After(time.Second)

	// close connections
	func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()

		for _, connState := range p.connMap {
			connState.Conn.Close()
		}
	}()

	log.Printf("%s: protocol closed", p.options.LogPrefix)
}

func (p *Server) ReadLoop(conn net.Conn) {
	connState := &ConnState{
		ConnID: p.getNextConnID(),
		Conn:   conn,
		Data:   atomic.Pointer[ConnVolatileData]{},
		Ready:  atomic.Bool{},
	}
	cvd := &ConnVolatileData{
		// to be communicated by peer during initial protocol
		PeerParticipant: nil,
		PeerID:          "",

		Descriptor: fmt.Sprintf(
			"[%d]%s<-<%s>",
			connState.ConnID,
			p.options.SelfID,
			conn.RemoteAddr().String(),
		),
	}
	connState.Data.Store(cvd)

	network := conn.RemoteAddr().Network()
	participantExitDispatched := false
	peerInShutdown := false

	log.Printf("%s: %s: new %s connection", p.options.LogPrefix, cvd.Descriptor, network)

	defer func() {
		log.Printf("%s: %s: closing %s connection", p.options.LogPrefix, cvd.Descriptor, network)
		connState.Ready.Store(false)

		selfInShutdown := p.inShutdown.Load()
		inShutdown := selfInShutdown || peerInShutdown

		func() {
			if cvd.PeerID == "" {
				log.Printf("%s: %s: peer unknown, no need to dispatch ParticipantExit", p.options.LogPrefix, cvd.Descriptor)
				return
			}

			if participantExitDispatched {
				log.Printf("%s: %s: ParticipantExit already dispatched", p.options.LogPrefix, cvd.Descriptor)
				return
			}

			p.options.Arbiter.Dispatch(
				func() {
					// invoked on arbiter goroutine
					p.options.ParticipantExit(
						p,
						connState,
						&m.ParticipantExit{
							InShutdown: inShutdown,
						},
					)
				},
			)
			participantExitDispatched = true
		}()

		func() {
			p.mutex.Lock()
			defer p.mutex.Unlock()

			_, found := p.connMap[connState.ConnID]
			if !found {
				log.Printf("%s: %s: connID=%d not found in connection map", p.options.LogPrefix, cvd.Descriptor, connState.ConnID)
				return
			}
			delete(p.connMap, connState.ConnID)

			// TODO - reconnect logic
		}()

		conn.Close()
		log.Printf("%s: %s: %s connection closed, selfInShutdown=%t, peerInShutdown=%t", p.options.LogPrefix, cvd.Descriptor, network, selfInShutdown, peerInShutdown)
	}()

	func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()

		cached, found := p.connMap[connState.ConnID]
		if found {
			log.Printf("%s: %s: overriding duplicate connection %s", p.options.LogPrefix, cvd.Descriptor, cached.Data.Load().Descriptor)
		}
		p.connMap[connState.ConnID] = connState
	}()

	handleMessage := func(messageStruct *m.Message) error {
		if messageStruct.ParticipantInit != nil {
			if messageStruct.ParticipantInit.Participant == nil {
				err := fmt.Errorf("%s: %s: invalid ParticipantInit=%+v", p.options.LogPrefix, cvd.Descriptor, messageStruct.ParticipantInit)
				log.Printf("%s", err.Error())
				return err
			}
			peerParticipant := messageStruct.ParticipantInit.Participant

			if peerParticipant.Host == "" {
				err := fmt.Errorf("%s: %s: invalid Host=%s", p.options.LogPrefix, cvd.Descriptor, peerParticipant.Host)
				log.Printf("%s", err.Error())
				return err
			}

			if peerParticipant.Instance == "" {
				err := fmt.Errorf("%s: %s: invalid Instance=%s", p.options.LogPrefix, cvd.Descriptor, peerParticipant.Instance)
				log.Printf("%s", err.Error())
				return err
			}

			if peerParticipant.Time <= 0 {
				err := fmt.Errorf("%s: %s: invalid Time=%d", p.options.LogPrefix, cvd.Descriptor, peerParticipant.Time)
				log.Printf("%s", err.Error())
				return err
			}

			if cvd.PeerID != "" {
				err := fmt.Errorf("%s: %s: already processed ParticipantInit, incoming Participant=%+v", p.options.LogPrefix, cvd.Descriptor, *peerParticipant)
				log.Printf("%s", err.Error())
				return err
			}

			// update volatile data
			cvd = &ConnVolatileData{
				PeerParticipant: peerParticipant,
				PeerID: fmt.Sprintf(
					"%s-%s-%d",
					peerParticipant.Host,
					peerParticipant.Instance,
					peerParticipant.Time,
				),

				// populated next
				Descriptor: "",
			}
			cvd.Descriptor = fmt.Sprintf(
				"[%d]%s<-%s<%s>",
				connState.ConnID,
				p.options.SelfID,
				cvd.PeerID,
				conn.RemoteAddr().String(),
			)
			connState.Data.Store(cvd) // atomic

			// TODO - reconnect logic
			peerInReconnect := messageStruct.ParticipantInit.InReconnect
			inReconnect := peerInReconnect
			messageStruct.ParticipantInit.InReconnect = inReconnect

			scopedDescriptor := cvd.Descriptor
			return p.options.Arbiter.Dispatch(
				func() {
					// invoked on arbiter goroutine
					writeWireData(
						p.options.LogPrefix,
						p.options.Txid,
						connState,
						&m.Message{
							Txseq:  p.GetNextTxseq(),
							Txtime: time.Now().UTC().UnixMilli(),

							ParticipantInit: &m.ParticipantInit{
								Participant: p.options.SelfParticipant,
								InReconnect: inReconnect,
							},
						},
					)

					connState.Ready.Store(true)
					log.Printf("%s: %s: connection now ready", p.options.LogPrefix, scopedDescriptor)

					p.options.ParticipantInit(
						p,
						connState,
						messageStruct.ParticipantInit,
					)
				},
			)
		} else if messageStruct.ParticipantExit != nil {
			if cvd.PeerID == "" {
				err := fmt.Errorf("%s: %s: peer unknown, cannot process ParticipantExit=%+v", p.options.LogPrefix, cvd.Descriptor, *messageStruct.ParticipantExit)
				log.Printf("%s", err.Error())
				return err
			}

			if participantExitDispatched {
				err := fmt.Errorf("%s: %s: duplicate ParticipantExit", p.options.LogPrefix, cvd.Descriptor)
				log.Printf("%s", err.Error())
				return err
			}

			peerInShutdown = messageStruct.ParticipantExit.InShutdown
			selfInShutdown := p.inShutdown.Load()
			inShutdown := selfInShutdown || peerInShutdown
			messageStruct.ParticipantExit.InShutdown = inShutdown
			log.Printf("%s: %s: selfInShutdown=%t, peerInShutdown=%t", p.options.LogPrefix, cvd.Descriptor, selfInShutdown, peerInShutdown)

			scopedDescriptor := cvd.Descriptor
			err := p.options.Arbiter.Dispatch(
				func() {
					// invoked on arbiter goroutine
					p.options.ParticipantExit(
						p,
						connState,
						messageStruct.ParticipantExit,
					)

					connState.Ready.Store(false)
					log.Printf("%s: %s: connection no longer ready", p.options.LogPrefix, scopedDescriptor)
				},
			)
			participantExitDispatched = true
			return err
		} else if messageStruct.CandidateVoteResponse != nil {
			if cvd.PeerID == "" {
				err := fmt.Errorf("%s: %s: peer unknown, cannot process CandidateVoteResponse=%+v", p.options.LogPrefix, cvd.Descriptor, *messageStruct.CandidateVoteResponse)
				log.Printf("%s", err.Error())
				return err
			}

			return p.options.Arbiter.Dispatch(
				func() {
					// invoked on arbiter goroutine
					p.options.CandidateVoteResponse(
						p,
						connState,
						messageStruct.CandidateVoteResponse,
					)
				},
			)
		} else if messageStruct.NomineeAckResponse != nil {
			if cvd.PeerID == "" {
				err := fmt.Errorf("%s: %s: peer unknown, cannot process NomineeAckResponse=%+v", p.options.LogPrefix, cvd.Descriptor, *messageStruct.NomineeAckResponse)
				log.Printf("%s", err.Error())
				return err
			}

			return p.options.Arbiter.Dispatch(
				func() {
					// invoked on arbiter goroutine
					p.options.NomineeAckResponse(
						p,
						connState,
						messageStruct.NomineeAckResponse,
					)
				},
			)
		} else {
			err := fmt.Errorf("%s: %s: unsupported messageStruct=%+v", p.options.LogPrefix, cvd.Descriptor, messageStruct)
			log.Printf("%s", err.Error())
			return err
		}
	}

	for {
		// first read seven bytes
		// 0 - pre-designated bit pattern indicating valid message
		// 1 - protocol version
		// 2 - sender id
		// 3,4,5,6 - payload length of type uint32, little endian byte order
		buf1 := make([]byte, 7)
		log.Printf("%s: %s: reading header bytes", p.options.LogPrefix, cvd.Descriptor)
		n1, err := io.ReadFull(conn, buf1)
		if err != nil {
			log.Printf("%s: %s: failed to read header bytes, err=%s", p.options.LogPrefix, cvd.Descriptor, err.Error())
			return
		}
		log.Printf("%s: %s: read header bytes %X", p.options.LogPrefix, cvd.Descriptor, buf1[:n1])

		// protocol specific sanity check
		if buf1[0] != protocolPattern {
			log.Printf("%s: %s: invalid protocol pattern in header bytes %X", p.options.LogPrefix, cvd.Descriptor, buf1[:n1])
			return
		}
		if buf1[1] != protocolVersion {
			log.Printf("%s: %s: unsupported protocol version in header bytes %X", p.options.LogPrefix, cvd.Descriptor, buf1[:n1])
			return
		}
		_, found := p.options.RxidMap[buf1[2]]
		if !found {
			log.Printf("%s: %s: unrecognized sender id in header bytes %X", p.options.LogPrefix, cvd.Descriptor, buf1[:n1])
			return
		}

		var payloadLen uint32 = *(*uint32)(unsafe.Pointer(&buf1[3]))
		if payloadLen > maxPayloadLen {
			log.Printf("%s: %s: payloadLen=%d in header bytes %X is too large", p.options.LogPrefix, cvd.Descriptor, payloadLen, buf1[:n1])
			return
		}

		buf2 := make([]byte, payloadLen)
		log.Printf("%s: %s: reading %d payload bytes", p.options.LogPrefix, cvd.Descriptor, payloadLen)
		n2, err := io.ReadFull(conn, buf2)
		if err != nil {
			log.Printf("%s: %s: failed to read payload bytes, err=%s", p.options.LogPrefix, cvd.Descriptor, err.Error())
			return
		}
		log.Printf("%s: %s: read %d payload bytes", p.options.LogPrefix, cvd.Descriptor, n2)

		messageStruct := new(m.Message)
		err = msgpack.Unmarshal(buf2, messageStruct)
		if err != nil {
			log.Printf("%s: %s: failed to unmarshal payload bytes %X, err=%s", p.options.LogPrefix, cvd.Descriptor, buf2[:n2], err.Error())
			return
		}
		if p.options.LogDebug {
			log.Printf("%s: %s: received messageStruct=%+v", p.options.LogPrefix, cvd.Descriptor, messageStruct)
		}

		err = handleMessage(messageStruct)
		if err != nil {
			return
		}
	}
}

// invoked on arbiter goroutine
func writeWireData[M any](logPrefix string, txid byte, connState *ConnState, messageStruct *M) error {
	descriptor := connState.Data.Load().Descriptor

	buffer := new(bytes.Buffer)
	buffer.Grow(typicalBufferLen)

	// write header of seven bytes
	// 0 - pre-designated bit pattern indicating valid message
	// 1 - protocol version
	// 2 - sender id
	// 3,4,5,6 - payload length of type uint32, little endian byte order
	buffer.WriteByte(protocolPattern)
	buffer.WriteByte(protocolVersion)
	buffer.WriteByte(txid)

	// placeholder for payload length
	buffer.WriteByte(0x00)
	buffer.WriteByte(0x00)
	buffer.WriteByte(0x00)
	buffer.WriteByte(0x00)

	// write payload
	err := msgpack.NewEncoder(buffer).Encode(messageStruct)
	if err != nil {
		log.Printf("%s: %s: msgpack failed to encode messageStruct=%+v, err=%s", logPrefix, descriptor, messageStruct, err.Error())
		return err
	}

	buf := buffer.Bytes()
	// do not access buffer beyond this point

	bufLen := len(buf)
	if bufLen < 7 {
		err = fmt.Errorf("%s: %s: invalid written buf=%X", logPrefix, descriptor, buf)
		log.Printf("%s", err.Error())
		return err
	}

	// update payload length placeholder
	var payloadLen uint32 = uint32(bufLen) - 7
	binary.LittleEndian.PutUint32(buf[3:7], payloadLen)

	connState.Conn.SetWriteDeadline(time.Now().UTC().Add(tcpWriteDeadline))
	n, err := connState.Conn.Write(buf)
	if err != nil {
		log.Printf("%s: %s: failed to write %d bytes %X, err=%s", logPrefix, descriptor, bufLen, buf, err.Error())
		return err
	}
	log.Printf("%s: %s: wrote %d bytes, header %X", logPrefix, descriptor, n, buf[0:7])

	return nil
}

// invoked on ReadLoop goroutine
func (p *Server) getNextConnID() uint32 {
	return p.connIDGen.Add(1)
}

// invoked on any goroutine
func (p *Server) GetNextTxseq() uint64 {
	return p.txseqGen.Add(1)
}

// invoked on any goroutine
func (p *Server) CheckConnection(connID uint32) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	connState, found := p.connMap[connID]
	if !found {
		return false
	}

	return connState.Ready.Load()
}

// invoked on any goroutine
func (p *Server) GetConnection(connID uint32) (*ConnState, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	connState, found := p.connMap[connID]
	if !found {
		err := fmt.Errorf("%s: connID=%d, no active connection", p.options.LogPrefix, connID)
		log.Printf("%s", err.Error())
		return nil, err
	}
	if !connState.Ready.Load() {
		err := fmt.Errorf("%s: %s: connection not ready", p.options.LogPrefix, connState.Data.Load().Descriptor)
		log.Printf("%s", err.Error())
		return nil, err
	}

	return connState, nil
}

// caller must be on arbiter goroutine
func (p *Server) WriteSync(connState *ConnState, messageStruct *m.Message) error {
	return writeWireData(
		p.options.LogPrefix,
		p.options.Txid,
		connState,
		messageStruct,
	)
}

// invoked on any goroutine
func (p *Server) WriteAsync(connState *ConnState, messageStruct *m.Message) error {
	return p.options.Arbiter.Dispatch(
		func() {
			// invoked on arbiter goroutine
			writeWireData(
				p.options.LogPrefix,
				p.options.Txid,
				connState,
				messageStruct,
			)
		},
	)
}
