package protocol

import (
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

type ClientHandler interface {
	CandidateVoteRequest(*Client, *ConnState, *m.CandidateVoteRequest)
	NomineeAckRequest(*Client, *ConnState, *m.NomineeAckRequest)
	NomineeRelinquish(*Client, *ConnState, *m.NomineeRelinquish)
	LeaderAnnounce(*Client, *ConnState, *m.LeaderAnnounce)
	LeaderRelinquish(*Client, *ConnState, *m.LeaderRelinquish)
}

type ClientOptions struct {
	*tcp.Options
	Arbiter *arbiter.Arbiter
	ClientHandler

	Txid    byte
	RxidMap map[byte]struct{}

	SelfParticipant *m.Participant
	SelfID          string
}

type Client struct {
	options           *ClientOptions
	defaultDescriptor string
	inShutdown        atomic.Bool

	// if increment overflow will wrap to zero
	connIDGen atomic.Uint32
	txseqGen  atomic.Uint64

	mutex     sync.Mutex
	connState *ConnState // current active tcp connection, if any
}

func NewClient(options *ClientOptions) (*Client, error) {
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

	p := &Client{
		options: options,
		defaultDescriptor: fmt.Sprintf(
			"%s-><%s>",
			options.SelfID,
			options.Address,
		),
		inShutdown: atomic.Bool{},

		connIDGen: atomic.Uint32{},
		txseqGen:  atomic.Uint64{},

		mutex:     sync.Mutex{},
		connState: nil,
	}

	return p, nil
}

func (p *Client) Options() *ClientOptions {
	return p.options
}

func (p *Client) Close() {
	log.Printf("%s: %s: protocol closing", p.options.LogPrefix, p.defaultDescriptor)
	p.inShutdown.Store(true)

	var exitwg sync.WaitGroup

	// send ParticipantExit
	func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()

		if p.connState == nil {
			return
		}
		connState := p.connState
		connState.Ready.Store(false)

		exitwg.Add(1)
		err := p.options.Arbiter.Dispatch(
			func() {
				// invoked on arbiter goroutine
				defer exitwg.Done()

				writeWireData(
					p.options.LogPrefix,
					p.options.Txid,
					connState,
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
	}()

	// wait until peer update is sent plus one second grace period
	exitwg.Wait()
	<-time.After(time.Second)

	// close connection
	func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()

		if p.connState == nil {
			log.Printf("%s: %s: no active connection", p.options.LogPrefix, p.defaultDescriptor)
			return
		}

		p.connState.Conn.Close()
	}()

	log.Printf("%s: %s: protocol closed", p.options.LogPrefix, p.defaultDescriptor)
}

func (p *Client) ReadLoop(conn net.Conn) {
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
			"[%d]%s-><%s>",
			connState.ConnID,
			p.options.SelfID,
			conn.RemoteAddr().String(),
		),
	}
	connState.Data.Store(cvd)

	network := conn.RemoteAddr().Network()
	peerInShutdown := false

	log.Printf("%s: %s: new %s connection", p.options.LogPrefix, cvd.Descriptor, network)

	defer func() {
		log.Printf("%s: %s: closing %s connection", p.options.LogPrefix, cvd.Descriptor, network)
		connState.Ready.Store(false)

		selfInShutdown := p.inShutdown.Load()

		func() {
			p.mutex.Lock()
			defer p.mutex.Unlock()

			if p.connState == nil {
				log.Printf("%s: %s: no connection cached, state corrupt", p.options.LogPrefix, cvd.Descriptor)
				return
			}

			if connState.ConnID != p.connState.ConnID {
				log.Printf("%s: %s: connID mismatch stack<%d>:cached<%d>, state corrupt", p.options.LogPrefix, cvd.Descriptor, connState.ConnID, p.connState.ConnID)
				return
			}

			p.connState = nil
		}()

		conn.Close()
		log.Printf("%s: %s: %s connection closed, selfInShutdown=%t, peerInShutdown=%t", p.options.LogPrefix, cvd.Descriptor, network, selfInShutdown, peerInShutdown)
	}()

	func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()

		if p.connState != nil {
			log.Printf("%s: %s: overriding stale connection %s", p.options.LogPrefix, cvd.Descriptor, p.connState.Data.Load().Descriptor)
		}
		p.connState = connState
	}()

	// initiate ParticipantInit
	err := p.options.Arbiter.Dispatch(
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
						InReconnect: true,
					},
				},
			)
		},
	)
	if err != nil {
		return
	}

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
				"[%d]%s->%s<%s>",
				connState.ConnID,
				p.options.SelfID,
				cvd.PeerID,
				conn.RemoteAddr().String(),
			)
			connState.Data.Store(cvd) // atomic

			peerInReconnect := messageStruct.ParticipantInit.InReconnect

			connState.Ready.Store(true)
			log.Printf("%s: %s: connection now ready, peerInReconnect=%t", p.options.LogPrefix, cvd.Descriptor, peerInReconnect)

			return nil
		} else if messageStruct.ParticipantExit != nil {
			if cvd.PeerID == "" {
				err := fmt.Errorf("%s: %s: peer unknown, cannot process ParticipantExit=%+v", p.options.LogPrefix, cvd.Descriptor, *messageStruct.ParticipantExit)
				log.Printf("%s", err.Error())
				return err
			}

			peerInShutdown = messageStruct.ParticipantExit.InShutdown
			selfInShutdown := p.inShutdown.Load()

			connState.Ready.Store(false)
			log.Printf("%s: %s: connection no longer ready, selfInShutdown=%t, peerInShutdown=%t", p.options.LogPrefix, cvd.Descriptor, selfInShutdown, peerInShutdown)

			return nil
		} else if messageStruct.CandidateVoteRequest != nil {
			if cvd.PeerID == "" {
				err := fmt.Errorf("%s: %s: peer unknown, cannot process CandidateVoteRequest=%+v", p.options.LogPrefix, cvd.Descriptor, *messageStruct.CandidateVoteRequest)
				log.Printf("%s", err.Error())
				return err
			}

			return p.options.Arbiter.Dispatch(
				func() {
					// invoked on arbiter goroutine
					p.options.CandidateVoteRequest(
						p,
						connState,
						messageStruct.CandidateVoteRequest,
					)
				},
			)
		} else if messageStruct.NomineeAckRequest != nil {
			if cvd.PeerID == "" {
				err := fmt.Errorf("%s: %s: peer unknown, cannot process NomineeAckRequest=%+v", p.options.LogPrefix, cvd.Descriptor, *messageStruct.NomineeAckRequest)
				log.Printf("%s", err.Error())
				return err
			}

			return p.options.Arbiter.Dispatch(
				func() {
					// invoked on arbiter goroutine
					p.options.NomineeAckRequest(
						p,
						connState,
						messageStruct.NomineeAckRequest,
					)
				},
			)
		} else if messageStruct.NomineeRelinquish != nil {
			if cvd.PeerID == "" {
				err := fmt.Errorf("%s: %s: peer unknown, cannot process NomineeRelinquish=%+v", p.options.LogPrefix, cvd.Descriptor, *messageStruct.NomineeRelinquish)
				log.Printf("%s", err.Error())
				return err
			}

			return p.options.Arbiter.Dispatch(
				func() {
					// invoked on arbiter goroutine
					p.options.NomineeRelinquish(
						p,
						connState,
						messageStruct.NomineeRelinquish,
					)
				},
			)
		} else if messageStruct.LeaderAnnounce != nil {
			if cvd.PeerID == "" {
				err := fmt.Errorf("%s: %s: peer unknown, cannot process LeaderAnnounce=%+v", p.options.LogPrefix, cvd.Descriptor, *messageStruct.LeaderAnnounce)
				log.Printf("%s", err.Error())
				return err
			}

			return p.options.Arbiter.Dispatch(
				func() {
					// invoked on arbiter goroutine
					p.options.LeaderAnnounce(
						p,
						connState,
						messageStruct.LeaderAnnounce,
					)
				},
			)
		} else if messageStruct.LeaderRelinquish != nil {
			if cvd.PeerID == "" {
				err := fmt.Errorf("%s: %s: peer unknown, cannot process LeaderRelinquish=%+v", p.options.LogPrefix, cvd.Descriptor, *messageStruct.LeaderRelinquish)
				log.Printf("%s", err.Error())
				return err
			}

			return p.options.Arbiter.Dispatch(
				func() {
					// invoked on arbiter goroutine
					p.options.LeaderRelinquish(
						p,
						connState,
						messageStruct.LeaderRelinquish,
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

// invoked on ReadLoop goroutine
func (p *Client) getNextConnID() uint32 {
	return p.connIDGen.Add(1)
}

// invoked on any goroutine
func (p *Client) GetNextTxseq() uint64 {
	return p.txseqGen.Add(1)
}

// invoked on any goroutine
func (p *Client) CheckConnection() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.connState == nil {
		return false
	}

	return p.connState.Ready.Load()
}

// invoked on any goroutine
func (p *Client) GetConnection() (*ConnState, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.connState == nil {
		err := fmt.Errorf("%s: %s: no active connection", p.options.LogPrefix, p.defaultDescriptor)
		log.Printf("%s", err.Error())
		return nil, err
	}
	if !p.connState.Ready.Load() {
		err := fmt.Errorf("%s: %s: connection not ready", p.options.LogPrefix, p.connState.Data.Load().Descriptor)
		log.Printf("%s", err.Error())
		return nil, err
	}

	return p.connState, nil
}

// caller must be on arbiter goroutine
func (p *Client) WriteSync(connState *ConnState, messageStruct *m.Message) error {
	return writeWireData(
		p.options.LogPrefix,
		p.options.Txid,
		connState,
		messageStruct,
	)
}

// invoked on any goroutine
func (p *Client) WriteAsync(connState *ConnState, messageStruct *m.Message) error {
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
