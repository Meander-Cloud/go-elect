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
func (e *Election) councilScheduleLockWait() {
	group := arbiter.GroupCouncilLockWait
	wait := time.Millisecond * time.Duration(e.c.CouncilLockWait)

	e.a.Scheduler().ProcessSync(
		&scheduler.ScheduleAsyncEvent[arbiter.Group]{
			AsyncVariant: scheduler.TimerAsync(
				true,
				[]arbiter.Group{group},
				wait,
				func() {
					// invoked on arbiter goroutine
					log.Printf(
						"%s: role=%s, councilForPeer=%s, wait timeout, participant<%d> lock: %d<%d>",
						e.c.LogPrefix,
						e.state.Role,
						e.state.CouncilForPeer,
						len(e.state.PeerMap)+1,
						e.state.QuorumParticipantCount,
						e.state.TotalParticipantCount,
					)

					e.councilToFollower()
				},
				nil,
			),
		},
	)

	log.Printf(
		"%s: role=%s, councilForPeer=%s, scheduled<%v>: %s",
		e.c.LogPrefix,
		e.state.Role,
		e.state.CouncilForPeer,
		wait,
		group,
	)
}

// invoked on arbiter goroutine
func (e *Election) councilReleaseLockWait() {
	group := arbiter.GroupCouncilLockWait

	e.a.Scheduler().ProcessSync(
		&scheduler.ReleaseGroupEvent[arbiter.Group]{
			Group: group,
		},
	)

	log.Printf(
		"%s: role=%s, councilForPeer=%s, released: %s",
		e.c.LogPrefix,
		e.state.Role,
		e.state.CouncilForPeer,
		group,
	)
}

// invoked on arbiter goroutine
func (e *Election) councilToFollower() {
	e.state.CouncilForPeer = ""

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
func (e *Election) councilParticipantInit(connState *tp.ConnState) {
	e.commonParticipantInit(connState)
}

// invoked on arbiter goroutine
func (e *Election) councilParticipantExit(connState *tp.ConnState) {
	cvd := connState.Data.Load()
	e.commonParticipantExit(cvd)

	func() {
		if e.state.CouncilForPeer != cvd.PeerID {
			return
		}

		log.Printf(
			"%s: %s: role=%s, councilForPeer=%s, peer exited, participant<%d> lock: %d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.CouncilForPeer,
			len(e.state.PeerMap)+1,
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)

		e.councilReleaseLockWait()

		e.councilToFollower()
	}()
}

// invoked on arbiter goroutine
func (e *Election) councilCandidateVoteRequest(p *tp.Client, connState *tp.ConnState, candidateVoteRequest *m.CandidateVoteRequest) {
	var vote uint8 = 0
	reason := m.CandidateVoteReasonRoleCouncil
	defer func() {
		p.WriteSync(
			connState,
			&m.Message{
				Txseq:  p.GetNextTxseq(),
				Txtime: time.Now().UTC().UnixMilli(),

				CandidateVoteResponse: &m.CandidateVoteResponse{
					Term:   candidateVoteRequest.Term,
					Vote:   vote,
					Reason: reason,
				},
			},
		)
	}()

	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, councilForPeer=%s, votedTerm=%d, requestTerm=%d, vote=%d, reason=%s, participant<%d> lock: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.CouncilForPeer,
		e.state.VotedTerm,
		candidateVoteRequest.Term,
		vote,
		reason,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) councilCandidateVoteResponse(connState *tp.ConnState, candidateVoteResponse *m.CandidateVoteResponse) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, councilForPeer=%s, no-op vote response, term=%d, vote=%d, reason=%s, participant<%d> lock: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.CouncilForPeer,
		candidateVoteResponse.Term,
		candidateVoteResponse.Vote,
		candidateVoteResponse.Reason,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}
