package election

import (
	"log"
	"time"

	"github.com/Meander-Cloud/go-schedule/scheduler"

	g "github.com/Meander-Cloud/go-elect/group"
	m "github.com/Meander-Cloud/go-elect/message"
	tp "github.com/Meander-Cloud/go-elect/net/tcp/protocol"
)

// invoked on arbiter goroutine
func (e *Election) ascendantAssert() {
	// reaching here implies quorum is intact
	log.Printf(
		"%s: role=%s, selfTerm=%d, participant<%d> assert: %d<%d>",
		e.c.LogPrefix,
		e.state.Role,
		e.state.SelfTerm,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)

	e.ascendantScheduleAssertWait()
}

// invoked on arbiter goroutine
func (e *Election) ascendantScheduleAssertWait() {
	group := g.GroupAscendantAssertWait
	wait := time.Millisecond * time.Duration(e.c.AscendantAssertWait)

	e.a.Scheduler().ProcessSync(
		&scheduler.ScheduleAsyncEvent[g.Group]{
			AsyncVariant: scheduler.TimerAsync(
				true,
				[]g.Group{group},
				wait,
				func() {
					// invoked on arbiter goroutine
					if e.state.Role != m.RoleAscendant {
						log.Printf(
							"%s: role=%s mismatch triggered: %s",
							e.c.LogPrefix,
							e.state.Role,
							group,
						)
						return
					}

					log.Printf(
						"%s: role=%s, selfTerm=%d, wait done, participant<%d> assert: %d<%d>",
						e.c.LogPrefix,
						e.state.Role,
						e.state.SelfTerm,
						len(e.state.PeerMap)+1,
						e.state.QuorumParticipantCount,
						e.state.TotalParticipantCount,
					)

					e.ascendantToLeader()
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
func (e *Election) ascendantReleaseAssertWait() {
	group := g.GroupAscendantAssertWait

	e.a.Scheduler().ProcessSync(
		&scheduler.ReleaseGroupEvent[g.Group]{
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
func (e *Election) ascendantToFollower() {
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
func (e *Election) ascendantToLeader() {
	// reaching here implies quorum is intact
	oldRole := e.state.Role
	newRole := m.RoleLeader
	e.state.Role = newRole

	log.Printf(
		"%s: role=%s -> %s",
		e.c.LogPrefix,
		oldRole,
		newRole,
	)

	e.leaderEnact()
}

// invoked on arbiter goroutine
func (e *Election) ascendantParticipantInit(connState *tp.ConnState) {
	e.commonParticipantInit(connState)
}

// invoked on arbiter goroutine
func (e *Election) ascendantParticipantExit(connState *tp.ConnState) {
	cvd := connState.Data.Load()
	e.commonParticipantExit(cvd)

	participantCount := uint16(len(e.state.PeerMap)) + 1

	func() {
		if participantCount >= e.state.QuorumParticipantCount {
			return
		}

		log.Printf(
			"%s: role=%s, selfTerm=%d, quorum loss, participant<%d> assert: %d<%d>",
			e.c.LogPrefix,
			e.state.Role,
			e.state.SelfTerm,
			participantCount,
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)

		server := e.matrix.Server()
		for _, cs := range e.state.PeerMap {
			server.WriteSync(
				cs,
				&m.Message{
					Txseq:  server.GetNextTxseq(),
					Txtime: time.Now().UTC().UnixMilli(),

					AscendantRelinquish: &m.AscendantRelinquish{
						Term:   e.state.SelfTerm,
						Reason: m.AscendantRelinquishReasonQuorumLoss,
					},
				},
			)
		}

		e.ascendantReleaseAssertWait()

		e.ascendantToFollower()
	}()
}

// invoked on arbiter goroutine
func (e *Election) ascendantCandidateVoteRequest(p *tp.Client, connState *tp.ConnState, candidateVoteRequest *m.CandidateVoteRequest) {
	var vote uint8 = 0
	reason := m.CandidateVoteReasonRoleAscendant
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
		"%s: %s: role=%s, selfTerm=%d, votedTerm=%d, requestTerm=%d, vote=%d, reason=%s, participant<%d> assert: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
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
func (e *Election) ascendantCandidateVoteResponse(connState *tp.ConnState, candidateVoteResponse *m.CandidateVoteResponse) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op vote-response, term=%d, vote=%d, reason=%s, participant<%d> assert: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		candidateVoteResponse.Term,
		candidateVoteResponse.Vote,
		candidateVoteResponse.Reason,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) ascendantNomineeAckRequest(p *tp.Client, connState *tp.ConnState, nomineeAckRequest *m.NomineeAckRequest) {
	var ack uint8 = 0
	reason := m.NomineeAckReasonRoleAscendant
	defer func() {
		p.WriteSync(
			connState,
			&m.Message{
				Txseq:  p.GetNextTxseq(),
				Txtime: time.Now().UTC().UnixMilli(),

				NomineeAckResponse: &m.NomineeAckResponse{
					Term:   nomineeAckRequest.Term,
					Ack:    ack,
					Reason: reason,
				},
			},
		)
	}()

	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, requestTerm=%d, ack=%d, reason=%s, participant<%d> assert: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		nomineeAckRequest.Term,
		ack,
		reason,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) ascendantNomineeAckResponse(connState *tp.ConnState, nomineeAckResponse *m.NomineeAckResponse) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op ack-response, term=%d, ack=%d, reason=%s, participant<%d> assert: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		nomineeAckResponse.Term,
		nomineeAckResponse.Ack,
		nomineeAckResponse.Reason,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) ascendantNomineeRelinquish(connState *tp.ConnState, nomineeRelinquish *m.NomineeRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op nominee-relinquish, term=%d, reason=%s, participant<%d> assert: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		nomineeRelinquish.Term,
		nomineeRelinquish.Reason,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) ascendantAscendantRelinquish(connState *tp.ConnState, ascendantRelinquish *m.AscendantRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, unexpected ascendant-relinquish, term=%d, reason=%s, participant<%d> assert: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		ascendantRelinquish.Term,
		ascendantRelinquish.Reason,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) ascendantLeaderAnnounce(connState *tp.ConnState, leaderAnnounce *m.LeaderAnnounce) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, unexpected leader-announce, term=%d, participant<%d> assert: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		leaderAnnounce.Term,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) ascendantLeaderRelinquish(connState *tp.ConnState, leaderRelinquish *m.LeaderRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, unexpected leader-relinquish, term=%d, reason=%s, participant<%d> assert: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		leaderRelinquish.Term,
		leaderRelinquish.Reason,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}
