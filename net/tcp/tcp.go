package tcp

import (
	"fmt"
	"log"
	"time"

	"github.com/Meander-Cloud/go-arbiter/arbiter"
	"github.com/Meander-Cloud/go-transport/tcp"

	"github.com/Meander-Cloud/go-elect/config"
	g "github.com/Meander-Cloud/go-elect/group"
	m "github.com/Meander-Cloud/go-elect/message"
	tp "github.com/Meander-Cloud/go-elect/net/tcp/protocol"
)

type ServerStruct struct {
	protocol  *tp.Server
	tcpServer *tcp.TcpServer
}

type ClientStruct struct {
	protocol  *tp.Client
	tcpClient *tcp.TcpClient
}

type Matrix struct {
	server    *ServerStruct
	clientMap map[string]*ClientStruct
}

func NewMatrix(
	c *config.Config,
	a *arbiter.Arbiter[g.Group],
	sh tp.ServerHandler,
	ch tp.ClientHandler,
	selfParticipant *m.Participant,
	selfID string,
) (*Matrix, error) {
	var tcpKeepAliveInterval time.Duration
	if c.TcpKeepAliveInterval == 0 {
		tcpKeepAliveInterval = config.TcpKeepAliveInterval
	} else {
		tcpKeepAliveInterval = time.Second * time.Duration(c.TcpKeepAliveInterval)
	}

	var tcpKeepAliveCount uint16
	if c.TcpKeepAliveCount == 0 {
		tcpKeepAliveCount = config.TcpKeepAliveCount
	} else {
		tcpKeepAliveCount = c.TcpKeepAliveCount
	}

	var tcpDialTimeout time.Duration
	if c.TcpDialTimeout == 0 {
		tcpDialTimeout = config.TcpDialTimeout
	} else {
		tcpDialTimeout = time.Second * time.Duration(c.TcpDialTimeout)
	}

	var tcpReconnectInterval time.Duration
	if c.TcpReconnectInterval == 0 {
		tcpReconnectInterval = config.TcpReconnectInterval
	} else {
		tcpReconnectInterval = time.Second * time.Duration(c.TcpReconnectInterval)
	}

	var tcpReconnectLogEvery uint32
	if c.TcpReconnectLogEvery == 0 {
		tcpReconnectLogEvery = config.TcpReconnectLogEvery
	} else {
		tcpReconnectLogEvery = c.TcpReconnectLogEvery
	}

	var tcpReconnectWindow time.Duration
	if c.TcpReconnectWindow == 0 {
		tcpReconnectWindow = config.TcpReconnectWindow
	} else {
		tcpReconnectWindow = time.Second * time.Duration(c.TcpReconnectWindow)
	}

	m := &Matrix{
		server: &ServerStruct{
			protocol:  nil,
			tcpServer: nil,
		},
		clientMap: make(map[string]*ClientStruct),
	}

	var err error
	defer func() {
		if err != nil {
			m.Shutdown() // wait
		}
	}()

	m.server.protocol, err = tp.NewServer(
		&tp.ServerOptions{
			Options: &tcp.Options{
				Address:           c.SelfAddress,
				KeepAliveInterval: tcpKeepAliveInterval,
				KeepAliveCount:    tcpKeepAliveCount,
				DialTimeout:       tcpDialTimeout,
				ReconnectInterval: tcpReconnectInterval,
				ReconnectLogEvery: tcpReconnectLogEvery,
				Protocol:          nil,
				LogPrefix:         "Server",
				LogDebug:          c.LogDebug,
			},
			Arbiter:       a,
			ServerHandler: sh,
			Txid:          tp.ServerSenderID,
			RxidMap: map[byte]struct{}{
				tp.ClientSenderID: {},
			},
			ReconnectWindow: tcpReconnectWindow,
			SelfParticipant: selfParticipant,
			SelfID:          selfID,
		},
	)
	if err != nil {
		return nil, err
	}
	m.server.protocol.Options().Protocol = m.server.protocol

	m.server.tcpServer, err = tcp.NewTcpServer(m.server.protocol.Options().Options)
	if err != nil {
		return nil, err
	}

	for index, address := range c.PeerAddressList {
		_, found := m.clientMap[address]
		if found {
			err = fmt.Errorf("%s: duplicate address=%s, invalid PeerAddressList=%+v", c.LogPrefix, address, c.PeerAddressList)
			log.Printf("%s", err.Error())
			return nil, err
		}

		client := &ClientStruct{
			protocol:  nil,
			tcpClient: nil,
		}

		client.protocol, err = tp.NewClient(
			&tp.ClientOptions{
				Options: &tcp.Options{
					Address:           address,
					KeepAliveInterval: tcpKeepAliveInterval,
					KeepAliveCount:    tcpKeepAliveCount,
					DialTimeout:       tcpDialTimeout,
					ReconnectInterval: tcpReconnectInterval,
					ReconnectLogEvery: tcpReconnectLogEvery,
					Protocol:          nil,
					LogPrefix:         fmt.Sprintf("Client-%d", index+1),
					LogDebug:          c.LogDebug,
				},
				Arbiter:       a,
				ClientHandler: ch,
				Txid:          tp.ClientSenderID,
				RxidMap: map[byte]struct{}{
					tp.ServerSenderID: {},
				},
				SelfParticipant: selfParticipant,
				SelfID:          selfID,
			},
		)
		if err != nil {
			return nil, err
		}
		client.protocol.Options().Protocol = client.protocol

		client.tcpClient, err = tcp.NewTcpClient(client.protocol.Options().Options)
		if err != nil {
			return nil, err
		}

		m.clientMap[address] = client
	}

	return m, nil
}

func (m *Matrix) Shutdown() {
	if m.server != nil &&
		m.server.tcpServer != nil {
		m.server.tcpServer.Shutdown() // wait
	}

	for _, client := range m.clientMap {
		if client.tcpClient != nil {
			client.tcpClient.Shutdown() // wait
		}
	}

	<-time.After(time.Second)
}

func (m *Matrix) Server() *tp.Server {
	return m.server.protocol
}
