package election

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/Meander-Cloud/go-elect/config"
	m "github.com/Meander-Cloud/go-elect/message"
	tp "github.com/Meander-Cloud/go-elect/net/tcp/protocol"
)

// arbiter / main goroutine access only, lock free
type State struct {
	SelfParticipant *m.Participant
	SelfID          string

	TotalParticipantCount  uint16
	QuorumParticipantCount uint16
	PeerMap                map[string]*tp.ConnState

	Role                          m.Role
	FollowerWaitBase              time.Duration
	FollowerWaitEpsilon           uint16
	FollowerWaitScheduled         bool
	SelfTerm                      uint32
	VotedTerm                     uint32
	CandidateVoteYesMap           map[string]struct{}
	CandidateVoteNoMap            map[string]struct{}
	NomineeAckYesMap              map[string]struct{}
	NomineeAckNoMap               map[string]struct{}
	CouncilForPeer                string
	LeaderQuorumLossWaitScheduled bool
}

func NewState(c *config.Config) *State {
	selfParticipant := &m.Participant{
		Host:     c.Host,
		Instance: c.Instance,
		Time:     time.Now().UTC().UnixMilli(),
	}
	selfID := fmt.Sprintf(
		"%s-%s-%d",
		selfParticipant.Host,
		selfParticipant.Instance,
		selfParticipant.Time,
	)

	totalParticipantCount := uint16(len(c.PeerAddressList)) + 1 // plus self
	quorumParticipantCount := totalParticipantCount/2 + 1
	log.Printf(
		"%s: participant quorum: %d/%d<%d>",
		c.LogPrefix,
		1,
		quorumParticipantCount,
		totalParticipantCount,
	)

	s := &State{
		SelfParticipant: selfParticipant,
		SelfID:          selfID,

		TotalParticipantCount:  totalParticipantCount,
		QuorumParticipantCount: quorumParticipantCount,
		PeerMap:                make(map[string]*tp.ConnState),

		Role:                          m.RoleFollower,
		FollowerWaitBase:              time.Millisecond * time.Duration(c.FollowerWaitRange[0]),
		FollowerWaitEpsilon:           c.FollowerWaitRange[1] - c.FollowerWaitRange[0],
		FollowerWaitScheduled:         false,
		SelfTerm:                      0,
		VotedTerm:                     0,
		CandidateVoteYesMap:           make(map[string]struct{}),
		CandidateVoteNoMap:            make(map[string]struct{}),
		NomineeAckYesMap:              make(map[string]struct{}),
		NomineeAckNoMap:               make(map[string]struct{}),
		CouncilForPeer:                "",
		LeaderQuorumLossWaitScheduled: false,
	}

	return s
}

func (s *State) GenerateFollowerWait() time.Duration {
	if s.FollowerWaitEpsilon == 0 {
		return s.FollowerWaitBase
	}

	return s.FollowerWaitBase + time.Millisecond*time.Duration(rand.Int31n(int32(s.FollowerWaitEpsilon)))
}
