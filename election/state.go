package election

import (
	"fmt"
	"log"
	"time"

	"github.com/Meander-Cloud/go-elect/config"
	m "github.com/Meander-Cloud/go-elect/message"
)

type State struct {
	SelfParticipant *m.Participant
	SelfID          string

	TotalParticipantCount   uint16
	QuorumParticipantCount  uint16
	CurrentParticipantCount uint16

	Role               m.Role
	SelfTerm           uint32
	VotedTerm          uint32
	CandidateVoteCount uint16
	NomineeAckCount    uint16
	CouncilForPeer     string
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

	totalParticipantCount := uint16(len(c.ParticipantAddressList))
	quorumParticipantCount := totalParticipantCount/2 + 1
	var currentParticipantCount uint16 = 1 // self

	log.Printf(
		"%s: totalParticipantCount=%d, quorumParticipantCount=%d, currentParticipantCount=%d",
		c.LogPrefix,
		totalParticipantCount,
		quorumParticipantCount,
		currentParticipantCount,
	)

	s := &State{
		SelfParticipant: selfParticipant,
		SelfID:          selfID,

		TotalParticipantCount:   totalParticipantCount,
		QuorumParticipantCount:  quorumParticipantCount,
		CurrentParticipantCount: currentParticipantCount,

		Role:               m.RoleFollower,
		SelfTerm:           0,
		VotedTerm:          0,
		CandidateVoteCount: 0,
		NomineeAckCount:    0,
		CouncilForPeer:     "",
	}

	return s
}
