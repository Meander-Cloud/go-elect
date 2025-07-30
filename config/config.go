package config

import (
	"fmt"
	"log"
	"time"
)

const (
	// defaults for when not provided in Config
	EventChannelLength   uint16        = 1024
	TcpKeepAliveInterval time.Duration = time.Second * 17
	TcpKeepAliveCount    uint16        = 2
	TcpDialTimeout       time.Duration = time.Second * 3
	TcpReconnectInterval time.Duration = time.Second * 5
	TcpReconnectWindow   time.Duration = time.Second * 17
)

type Config struct {
	Host               string
	Instance           string
	EventChannelLength uint16

	SelfAddress          string
	PeerAddressList      []string
	TcpKeepAliveInterval uint16
	TcpKeepAliveCount    uint16
	TcpDialTimeout       uint16
	TcpReconnectInterval uint16
	TcpReconnectWindow   uint16

	FollowerWaitRange    []uint16
	CandidateVoteWait    uint16
	NomineeAckWait       uint16
	CouncilWait          uint16
	AscendantAssertWait  uint16
	LeaderQuorumLossWait uint16

	LogPrefix string
	LogDebug  bool
}

func (c *Config) Validate() error {
	if c == nil {
		err := fmt.Errorf("nil config")
		log.Printf("%s", err.Error())
		return err
	}

	if c.Host == "" {
		err := fmt.Errorf("%s: invalid Host=%s", c.LogPrefix, c.Host)
		log.Printf("%s", err.Error())
		return err
	}

	if c.Instance == "" {
		err := fmt.Errorf("%s: invalid Instance=%s", c.LogPrefix, c.Instance)
		log.Printf("%s", err.Error())
		return err
	}

	if c.SelfAddress == "" {
		err := fmt.Errorf("%s: invalid SelfAddress=%s", c.LogPrefix, c.SelfAddress)
		log.Printf("%s", err.Error())
		return err
	}

	palMap := make(map[string]struct{})
	for index, address := range c.PeerAddressList {
		if address == "" {
			err := fmt.Errorf("%s: empty address at index=%d, invalid PeerAddressList=%+v", c.LogPrefix, index, c.PeerAddressList)
			log.Printf("%s", err.Error())
			return err
		}

		_, found := palMap[address]
		if found {
			err := fmt.Errorf("%s: duplicate address=%s, invalid PeerAddressList=%+v", c.LogPrefix, address, c.PeerAddressList)
			log.Printf("%s", err.Error())
			return err
		}

		palMap[address] = struct{}{}
	}

	palLen := len(c.PeerAddressList)
	if palLen%2 != 0 {
		err := fmt.Errorf("%s: palLen=%d, must have even number of peers to form election with odd number of participants", c.LogPrefix, palLen)
		log.Printf("%s", err.Error())
		return err
	}

	if len(c.FollowerWaitRange) != 2 {
		err := fmt.Errorf("%s: invalid FollowerWaitRange=%+v, must specify min / max range", c.LogPrefix, c.FollowerWaitRange)
		log.Printf("%s", err.Error())
		return err
	}

	if c.FollowerWaitRange[0] > c.FollowerWaitRange[1] {
		err := fmt.Errorf("%s: invalid FollowerWaitRange=%+v, max must be no less than min", c.LogPrefix, c.FollowerWaitRange)
		log.Printf("%s", err.Error())
		return err
	}

	if c.CandidateVoteWait == 0 {
		err := fmt.Errorf("%s: invalid CandidateVoteWait=%d, must allow time for peer response", c.LogPrefix, c.CandidateVoteWait)
		log.Printf("%s", err.Error())
		return err
	}

	if c.NomineeAckWait == 0 {
		err := fmt.Errorf("%s: invalid NomineeAckWait=%d, must allow time for peer response", c.LogPrefix, c.NomineeAckWait)
		log.Printf("%s", err.Error())
		return err
	}

	if c.CouncilWait == 0 {
		err := fmt.Errorf("%s: invalid CouncilWait=%d, must allow time for peer advance", c.LogPrefix, c.CouncilWait)
		log.Printf("%s", err.Error())
		return err
	}

	if uint32(c.NomineeAckWait)+uint32(c.AscendantAssertWait) >= uint32(c.CouncilWait) {
		err := fmt.Errorf("%s: NomineeAckWait plus AscendantAssertWait must be less than CouncilWait to allow time for leader announce", c.LogPrefix)
		log.Printf("%s", err.Error())
		return err
	}

	if uint32(c.FollowerWaitRange[0])+uint32(c.AscendantAssertWait) <= uint32(c.LeaderQuorumLossWait) {
		err := fmt.Errorf("%s: FollowerWaitBase plus AscendantAssertWait must be more than LeaderQuorumLossWait to guarantee one leader in split-brain scenario", c.LogPrefix)
		log.Printf("%s", err.Error())
		return err
	}

	return nil
}
