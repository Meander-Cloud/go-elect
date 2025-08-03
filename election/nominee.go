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
func (e *Election) nomineeRequestAck() {
	// reaching here implies quorum is intact
	ackYesCount := uint16(len(e.state.NomineeAckYesMap)) + 1
	log.Printf(
		"%s: role=%s, selfTerm=%d, participant<%d> ack: %d-%d/%d<%d>",
		e.c.LogPrefix,
		e.state.Role,
		e.state.SelfTerm,
		len(e.state.PeerMap)+1,
		ackYesCount,
		len(e.state.NomineeAckNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)

	if ackYesCount >= e.state.QuorumParticipantCount {
		e.nomineeToAscendant()
		return
	}

	server := e.matrix.Server()
	for _, connState := range e.state.PeerMap {
		server.WriteSync(
			connState,
			&m.Message{
				Txseq:  server.GetNextTxseq(),
				Txtime: time.Now().UTC().UnixMilli(),

				NomineeAckRequest: &m.NomineeAckRequest{
					Term: e.state.SelfTerm,
				},
			},
		)
	}

	e.nomineeScheduleAckWait()
}

// invoked on arbiter goroutine
func (e *Election) nomineeScheduleAckWait() {
	group := arbiter.GroupNomineeAckWait
	wait := time.Millisecond * time.Duration(e.c.NomineeAckWait)

	e.a.Scheduler().ProcessSync(
		&scheduler.ScheduleAsyncEvent[arbiter.Group]{
			AsyncVariant: scheduler.TimerAsync(
				true,
				[]arbiter.Group{group},
				wait,
				func() {
					// invoked on arbiter goroutine
					log.Printf(
						"%s: role=%s, selfTerm=%d, wait timeout, participant<%d> ack: %d-%d/%d<%d>",
						e.c.LogPrefix,
						e.state.Role,
						e.state.SelfTerm,
						len(e.state.PeerMap)+1,
						len(e.state.NomineeAckYesMap)+1,
						len(e.state.NomineeAckNoMap),
						e.state.QuorumParticipantCount,
						e.state.TotalParticipantCount,
					)

					server := e.matrix.Server()
					for _, connState := range e.state.PeerMap {
						server.WriteSync(
							connState,
							&m.Message{
								Txseq:  server.GetNextTxseq(),
								Txtime: time.Now().UTC().UnixMilli(),

								NomineeRelinquish: &m.NomineeRelinquish{
									Term:   e.state.SelfTerm,
									Reason: m.NomineeRelinquishReasonAckTimeout,
								},
							},
						)
					}

					e.nomineeToFollower()
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
func (e *Election) nomineeReleaseAckWait() {
	group := arbiter.GroupNomineeAckWait

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
func (e *Election) nomineeToFollower() {
	clear(e.state.NomineeAckYesMap)
	clear(e.state.NomineeAckNoMap)

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
func (e *Election) nomineeToAscendant() {
	// reaching here implies quorum is intact
	clear(e.state.NomineeAckYesMap)
	clear(e.state.NomineeAckNoMap)

	oldRole := e.state.Role
	newRole := m.RoleAscendant
	e.state.Role = newRole

	log.Printf(
		"%s: role=%s -> %s",
		e.c.LogPrefix,
		oldRole,
		newRole,
	)

	e.ascendantAssert()
}

// invoked on arbiter goroutine
func (e *Election) nomineeParticipantInit(p *tp.Server, connState *tp.ConnState) {
	e.commonParticipantInit(connState)

	p.WriteSync(
		connState,
		&m.Message{
			Txseq:  p.GetNextTxseq(),
			Txtime: time.Now().UTC().UnixMilli(),

			NomineeAckRequest: &m.NomineeAckRequest{
				Term: e.state.SelfTerm,
			},
		},
	)
}

// invoked on arbiter goroutine
func (e *Election) nomineeParticipantExit(connState *tp.ConnState) {
	cvd := connState.Data.Load()
	e.commonParticipantExit(cvd)

	participantCount := uint16(len(e.state.PeerMap)) + 1

	func() {
		var found bool

		_, found = e.state.NomineeAckYesMap[cvd.PeerID]
		if found {
			delete(e.state.NomineeAckYesMap, cvd.PeerID)

			log.Printf(
				"%s: %s: role=%s, selfTerm=%d, removed ack yes, participant<%d> ack: %d-%d/%d<%d>",
				e.c.LogPrefix,
				cvd.Descriptor,
				e.state.Role,
				e.state.SelfTerm,
				participantCount,
				len(e.state.NomineeAckYesMap)+1,
				len(e.state.NomineeAckNoMap),
				e.state.QuorumParticipantCount,
				e.state.TotalParticipantCount,
			)
		}

		_, found = e.state.NomineeAckNoMap[cvd.PeerID]
		if found {
			delete(e.state.NomineeAckNoMap, cvd.PeerID)

			log.Printf(
				"%s: %s: role=%s, selfTerm=%d, removed ack no, participant<%d> ack: %d-%d/%d<%d>",
				e.c.LogPrefix,
				cvd.Descriptor,
				e.state.Role,
				e.state.SelfTerm,
				participantCount,
				len(e.state.NomineeAckYesMap)+1,
				len(e.state.NomineeAckNoMap),
				e.state.QuorumParticipantCount,
				e.state.TotalParticipantCount,
			)
		}
	}()

	func() {
		if participantCount >= e.state.QuorumParticipantCount {
			return
		}

		log.Printf(
			"%s: role=%s, selfTerm=%d, quorum loss, participant<%d> ack: %d-%d/%d<%d>",
			e.c.LogPrefix,
			e.state.Role,
			e.state.SelfTerm,
			participantCount,
			len(e.state.NomineeAckYesMap)+1,
			len(e.state.NomineeAckNoMap),
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)

		server := e.matrix.Server()
		for _, connState := range e.state.PeerMap {
			server.WriteSync(
				connState,
				&m.Message{
					Txseq:  server.GetNextTxseq(),
					Txtime: time.Now().UTC().UnixMilli(),

					NomineeRelinquish: &m.NomineeRelinquish{
						Term:   e.state.SelfTerm,
						Reason: m.NomineeRelinquishReasonQuorumLoss,
					},
				},
			)
		}

		e.nomineeReleaseAckWait()

		e.nomineeToFollower()
	}()
}

// invoked on arbiter goroutine
func (e *Election) nomineeCandidateVoteRequest(p *tp.Client, connState *tp.ConnState, candidateVoteRequest *m.CandidateVoteRequest) {
	var vote uint8 = 0
	reason := m.CandidateVoteReasonRoleNominee
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
		"%s: %s: role=%s, selfTerm=%d, votedTerm=%d, requestTerm=%d, vote=%d, reason=%s, participant<%d> ack: %d-%d/%d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		e.state.VotedTerm,
		candidateVoteRequest.Term,
		vote,
		reason,
		len(e.state.PeerMap)+1,
		len(e.state.NomineeAckYesMap)+1,
		len(e.state.NomineeAckNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) nomineeCandidateVoteResponse(connState *tp.ConnState, candidateVoteResponse *m.CandidateVoteResponse) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op vote-response, term=%d, vote=%d, reason=%s, participant<%d> ack: %d-%d/%d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		candidateVoteResponse.Term,
		candidateVoteResponse.Vote,
		candidateVoteResponse.Reason,
		len(e.state.PeerMap)+1,
		len(e.state.NomineeAckYesMap)+1,
		len(e.state.NomineeAckNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) nomineeNomineeAckRequest(p *tp.Client, connState *tp.ConnState, nomineeAckRequest *m.NomineeAckRequest) {
	var ack uint8 = 0
	reason := m.NomineeAckReasonRoleNominee
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
		"%s: %s: role=%s, selfTerm=%d, requestTerm=%d, ack=%d, reason=%s, participant<%d> ack: %d-%d/%d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		nomineeAckRequest.Term,
		ack,
		reason,
		len(e.state.PeerMap)+1,
		len(e.state.NomineeAckYesMap)+1,
		len(e.state.NomineeAckNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) nomineeNomineeAckResponse(connState *tp.ConnState, nomineeAckResponse *m.NomineeAckResponse) {

}

// invoked on arbiter goroutine
func (e *Election) nomineeNomineeRelinquish(connState *tp.ConnState, nomineeRelinquish *m.NomineeRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op nominee-relinquish, term=%d, reason=%s, participant<%d> ack: %d-%d/%d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		nomineeRelinquish.Term,
		nomineeRelinquish.Reason,
		len(e.state.PeerMap)+1,
		len(e.state.NomineeAckYesMap)+1,
		len(e.state.NomineeAckNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) nomineeAscendantRelinquish(connState *tp.ConnState, ascendantRelinquish *m.AscendantRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op ascendant-relinquish, term=%d, reason=%s, participant<%d> ack: %d-%d/%d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		ascendantRelinquish.Term,
		ascendantRelinquish.Reason,
		len(e.state.PeerMap)+1,
		len(e.state.NomineeAckYesMap)+1,
		len(e.state.NomineeAckNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) nomineeLeaderAnnounce(connState *tp.ConnState, leaderAnnounce *m.LeaderAnnounce) {

}

// invoked on arbiter goroutine
func (e *Election) nomineeLeaderRelinquish(connState *tp.ConnState, leaderRelinquish *m.LeaderRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op leader-relinquish, term=%d, reason=%s, participant<%d> ack: %d-%d/%d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		leaderRelinquish.Term,
		leaderRelinquish.Reason,
		len(e.state.PeerMap)+1,
		len(e.state.NomineeAckYesMap)+1,
		len(e.state.NomineeAckNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}
