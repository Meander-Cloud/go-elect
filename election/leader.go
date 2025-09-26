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
func (e *Election) leaderEnact() {
	// reaching here implies quorum is intact
	log.Printf(
		"%s: role=%s, selfTerm=%d, participant<%d> quorum: %d<%d>",
		e.c.LogPrefix,
		e.state.Role,
		e.state.SelfTerm,
		len(e.state.PeerMap)+1,
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

				LeaderAnnounce: &m.LeaderAnnounce{
					Term: e.state.SelfTerm,
				},
			},
		)
	}

	e.uc.LeaderElected(
		&LeaderElected{
			Participant: e.state.SelfParticipant.Clone(),
			Time:        time.Now().UTC(),
		},
	)
}

// invoked on arbiter goroutine
func (e *Election) leaderCheckQuorum() {
	participantCount := uint16(len(e.state.PeerMap)) + 1
	log.Printf(
		"%s: role=%s, selfTerm=%d, participant<%d> quorum: %d<%d>",
		e.c.LogPrefix,
		e.state.Role,
		e.state.SelfTerm,
		participantCount,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)

	if participantCount >= e.state.QuorumParticipantCount {
		e.leaderReleaseQuorumLossWait()
		return
	}

	e.leaderScheduleQuorumLossWait()
}

// invoked on arbiter goroutine
func (e *Election) leaderScheduleQuorumLossWait() {
	if e.state.LeaderQuorumLossWaitScheduled {
		// no-op
		return
	}

	group := g.GroupLeaderQuorumLossWait
	wait := time.Millisecond * time.Duration(e.c.LeaderQuorumLossWait)

	e.a.Scheduler().ProcessSync(
		&scheduler.ScheduleAsyncEvent[g.Group]{
			AsyncVariant: scheduler.TimerAsync(
				true,
				[]g.Group{group},
				wait,
				func() {
					// invoked on arbiter goroutine
					e.state.LeaderQuorumLossWaitScheduled = false

					if e.state.Role != m.RoleLeader {
						log.Printf(
							"%s: role=%s mismatch triggered: %s",
							e.c.LogPrefix,
							e.state.Role,
							group,
						)
						return
					}

					log.Printf(
						"%s: role=%s, selfTerm=%d, wait timeout, participant<%d> quorum: %d<%d>",
						e.c.LogPrefix,
						e.state.Role,
						e.state.SelfTerm,
						len(e.state.PeerMap)+1,
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

								LeaderRelinquish: &m.LeaderRelinquish{
									Term:   e.state.SelfTerm,
									Reason: m.LeaderRelinquishReasonQuorumLoss,
								},
							},
						)
					}

					e.uc.LeaderRevoked(
						&LeaderRevoked{
							Participant: e.state.SelfParticipant.Clone(),
							Time:        time.Now().UTC(),
						},
					)

					e.leaderToFollower()
				},
				nil,
			),
		},
	)

	e.state.LeaderQuorumLossWaitScheduled = true

	log.Printf(
		"%s: role=%s, scheduled<%v>: %s",
		e.c.LogPrefix,
		e.state.Role,
		wait,
		group,
	)
}

// invoked on arbiter goroutine
func (e *Election) leaderReleaseQuorumLossWait() {
	if !e.state.LeaderQuorumLossWaitScheduled {
		// no-op
		return
	}

	group := g.GroupLeaderQuorumLossWait

	e.a.Scheduler().ProcessSync(
		&scheduler.ReleaseGroupEvent[g.Group]{
			Group: group,
		},
	)

	e.state.LeaderQuorumLossWaitScheduled = false

	log.Printf(
		"%s: role=%s, released: %s",
		e.c.LogPrefix,
		e.state.Role,
		group,
	)
}

// invoked on arbiter goroutine
func (e *Election) leaderToFollower() {
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
func (e *Election) leaderParticipantInit(p *tp.Server, connState *tp.ConnState) {
	e.commonParticipantInit(connState)

	p.WriteSync(
		connState,
		&m.Message{
			Txseq:  p.GetNextTxseq(),
			Txtime: time.Now().UTC().UnixMilli(),

			LeaderAnnounce: &m.LeaderAnnounce{
				Term: e.state.SelfTerm,
			},
		},
	)

	e.leaderCheckQuorum()
}

// invoked on arbiter goroutine
func (e *Election) leaderParticipantExit(connState *tp.ConnState) {
	cvd := connState.Data.Load()
	e.commonParticipantExit(cvd)

	e.leaderCheckQuorum()
}

// invoked on arbiter goroutine
func (e *Election) leaderCandidateVoteRequest(p *tp.Client, connState *tp.ConnState, candidateVoteRequest *m.CandidateVoteRequest) {
	var vote uint8 = 0
	reason := m.CandidateVoteReasonRoleLeader
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
		"%s: %s: role=%s, selfTerm=%d, votedTerm=%d, requestTerm=%d, vote=%d, reason=%s, participant<%d> quorum: %d<%d>",
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
func (e *Election) leaderCandidateVoteResponse(connState *tp.ConnState, candidateVoteResponse *m.CandidateVoteResponse) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op vote-response, term=%d, vote=%d, reason=%s, participant<%d> quorum: %d<%d>",
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
func (e *Election) leaderNomineeAckRequest(p *tp.Client, connState *tp.ConnState, nomineeAckRequest *m.NomineeAckRequest) {
	var ack uint8 = 0
	reason := m.NomineeAckReasonRoleLeader
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
		"%s: %s: role=%s, selfTerm=%d, requestTerm=%d, ack=%d, reason=%s, participant<%d> quorum: %d<%d>",
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
func (e *Election) leaderNomineeAckResponse(connState *tp.ConnState, nomineeAckResponse *m.NomineeAckResponse) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op ack-response, term=%d, ack=%d, reason=%s, participant<%d> quorum: %d<%d>",
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
func (e *Election) leaderNomineeRelinquish(connState *tp.ConnState, nomineeRelinquish *m.NomineeRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op nominee-relinquish, term=%d, reason=%s, participant<%d> quorum: %d<%d>",
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
func (e *Election) leaderAscendantRelinquish(connState *tp.ConnState, ascendantRelinquish *m.AscendantRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, unexpected ascendant-relinquish, term=%d, reason=%s, participant<%d> quorum: %d<%d>",
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
func (e *Election) leaderLeaderAnnounce(connState *tp.ConnState, leaderAnnounce *m.LeaderAnnounce) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, unexpected leader-announce, term=%d, participant<%d> quorum: %d<%d>",
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
func (e *Election) leaderLeaderRelinquish(connState *tp.ConnState, leaderRelinquish *m.LeaderRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, unexpected leader-relinquish, term=%d, reason=%s, participant<%d> quorum: %d<%d>",
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
