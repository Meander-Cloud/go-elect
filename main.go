package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Meander-Cloud/go-elect/config"
	"github.com/Meander-Cloud/go-elect/election"
)

type UserCallback struct {
	LogPrefix string
}

func (uc *UserCallback) LeaderElected(elected *election.LeaderElected) {
	log.Printf(
		"%s: LeaderElected: participant=%s, time=%s",
		uc.LogPrefix,
		func() string {
			if elected.Participant == nil {
				return "<nil>"
			}
			return fmt.Sprintf("%+v", *elected.Participant)
		}(),
		elected.Time.Format(time.RFC3339),
	)
}

func (uc *UserCallback) LeaderRevoked(revoked *election.LeaderRevoked) {
	log.Printf(
		"%s: LeaderRevoked: participant=%s, time=%s",
		uc.LogPrefix,
		func() string {
			if revoked.Participant == nil {
				return "<nil>"
			}
			return fmt.Sprintf("%+v", *revoked.Participant)
		}(),
		revoked.Time.Format(time.RFC3339),
	)
}

func test1() {
	if len(os.Args) <= 1 {
		log.Printf("test1: must specify instance 1/2/3")
		return
	}

	instance := os.Args[1]
	c := &config.Config{
		Host:               "",
		Instance:           instance,
		EventChannelLength: 256,

		SelfAddress:          "",
		PeerAddressList:      nil,
		TcpKeepAliveInterval: 17,
		TcpKeepAliveCount:    2,
		TcpDialTimeout:       3,
		TcpReconnectInterval: 5,
		TcpReconnectWindow:   17,

		FollowerWaitRange: []uint16{
			6000,
			12000,
		},
		CandidateVoteWait:    3000,
		NomineeAckWait:       3000,
		CouncilLockWait:      30000,
		AscendantAssertWait:  12000,
		LeaderQuorumLossWait: 17000,

		LogPrefix: "test1",
		LogDebug:  false,
	}

	switch instance {
	case "1":
		c.Host = "A"
		c.SelfAddress = "localhost:8911"
		c.PeerAddressList = []string{
			"localhost:8912",
			"localhost:8913",
		}
	case "2":
		c.Host = "B"
		c.SelfAddress = "localhost:8912"
		c.PeerAddressList = []string{
			"localhost:8911",
			"localhost:8913",
		}
	case "3":
		c.Host = "C"
		c.SelfAddress = "localhost:8913"
		c.PeerAddressList = []string{
			"localhost:8911",
			"localhost:8912",
		}
	default:
		log.Printf("test1: must specify instance 1/2/3")
		return
	}

	e, err := election.NewElection(
		c,
		&UserCallback{
			LogPrefix: "test1",
		},
	)
	if err != nil {
		panic(err)
	}

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigch // wait
	log.Printf("test1: received signal %s, exiting", sig.String())

	e.Shutdown()
}

func main() {
	// enable microsecond and file line logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	test1()
}
