package election

import (
	"log"
	"time"

	"github.com/Meander-Cloud/go-schedule/scheduler"

	"github.com/Meander-Cloud/go-elect/arbiter"
	m "github.com/Meander-Cloud/go-elect/message"
	tp "github.com/Meander-Cloud/go-elect/net/tcp/protocol"
)

// invoked on arbiter goroutine
func (e *Election) candidateRequestVote() {
	// reaching here implies quorum is intact
	selfTerm := e.state.SelfTerm
	votedTerm := e.state.VotedTerm
	var newTerm uint32
	if e.state.SelfTerm >= e.state.VotedTerm {
		newTerm = e.state.SelfTerm + 1
	} else {
		newTerm = e.state.VotedTerm + 1
	}
	e.state.SelfTerm = newTerm
	e.state.VotedTerm = newTerm // auto vote for self

	voteYesCount := uint16(len(e.state.CandidateVoteYesMap)) + 1
	log.Printf(
		"%s: role=%s, selfTerm=%d -> %d, votedTerm=%d -> %d, participant<%d> vote: %d-%d/%d<%d>",
		e.c.LogPrefix,
		e.state.Role,
		selfTerm,
		newTerm,
		votedTerm,
		newTerm,
		len(e.state.PeerMap)+1,
		voteYesCount,
		len(e.state.CandidateVoteNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)

	if voteYesCount >= e.state.QuorumParticipantCount {
		e.candidateToNominee()
		return
	}

	server := e.matrix.Server()
	for _, connState := range e.state.PeerMap {
		server.WriteSync(
			connState,
			&m.Message{
				Txseq:  server.GetNextTxseq(),
				Txtime: time.Now().UTC().UnixMilli(),

				CandidateVoteRequest: &m.CandidateVoteRequest{
					Term: newTerm,
				},
			},
		)
	}

	e.candidateScheduleVoteWait()
}

// invoked on arbiter goroutine
func (e *Election) candidateScheduleVoteWait() {
	group := arbiter.GroupCandidateVoteWait
	wait := time.Millisecond * time.Duration(e.c.CandidateVoteWait)

	e.a.Scheduler().ProcessSync(
		&scheduler.ScheduleAsyncEvent[arbiter.Group]{
			AsyncVariant: scheduler.TimerAsync(
				true,
				[]arbiter.Group{group},
				wait,
				func() {
					// invoked on arbiter goroutine
					log.Printf(
						"%s: role=%s, wait timeout, selfTerm=%d, participant<%d> vote: %d-%d/%d<%d>",
						e.c.LogPrefix,
						e.state.Role,
						e.state.SelfTerm,
						len(e.state.PeerMap)+1,
						len(e.state.CandidateVoteYesMap)+1,
						len(e.state.CandidateVoteNoMap),
						e.state.QuorumParticipantCount,
						e.state.TotalParticipantCount,
					)

					e.candidateToFollower()
				},
				nil,
			),
		},
	)

	log.Printf(
		"%s: role=%s, scheduled<%v>: %s",
		e.c.LogPrefix,
		e.state.Role,
		wait,
		group,
	)
}

// invoked on arbiter goroutine
func (e *Election) candidateReleaseVoteWait() {
	group := arbiter.GroupCandidateVoteWait

	e.a.Scheduler().ProcessSync(
		&scheduler.ReleaseGroupEvent[arbiter.Group]{
			Group: group,
		},
	)

	log.Printf(
		"%s: role=%s, released: %s",
		e.c.LogPrefix,
		e.state.Role,
		group,
	)
}

// invoked on arbiter goroutine
func (e *Election) candidateToFollower() {
	clear(e.state.CandidateVoteYesMap)
	clear(e.state.CandidateVoteNoMap)

	oldRole := e.state.Role
	newRole := m.RoleFollower
	e.state.Role = newRole

	log.Printf(
		"%s: role=%s -> %s",
		e.c.LogPrefix,
		oldRole,
		newRole,
	)

	e.followerCheckQuorum()
}

// invoked on arbiter goroutine
func (e *Election) candidateToNominee() {
	// reaching here implies quorum is intact
	clear(e.state.CandidateVoteYesMap)
	clear(e.state.CandidateVoteNoMap)

	oldRole := e.state.Role
	newRole := m.RoleNominee
	e.state.Role = newRole

	log.Printf(
		"%s: role=%s -> %s",
		e.c.LogPrefix,
		oldRole,
		newRole,
	)

	e.nomineeRequestAck()
}

// invoked on arbiter goroutine
func (e *Election) candidateParticipantInit(p *tp.Server, connState *tp.ConnState) {
	e.commonParticipantInit(connState)

	p.WriteSync(
		connState,
		&m.Message{
			Txseq:  p.GetNextTxseq(),
			Txtime: time.Now().UTC().UnixMilli(),

			CandidateVoteRequest: &m.CandidateVoteRequest{
				Term: e.state.SelfTerm,
			},
		},
	)
}

// invoked on arbiter goroutine
func (e *Election) candidateParticipantExit(p *tp.Server, connState *tp.ConnState) {
	// also remove from vote maps
}
