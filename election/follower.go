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
func (e *Election) followerCheckQuorum() {
	participantCount := uint16(len(e.state.PeerMap)) + 1
	log.Printf(
		"%s: role=%s, participant<%d> quorum: %d<%d>",
		e.c.LogPrefix,
		e.state.Role,
		participantCount,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)

	if participantCount < e.state.QuorumParticipantCount {
		e.followerReleaseWait()
		return
	}

	e.followerScheduleWait()
}

// invoked on arbiter goroutine
func (e *Election) followerScheduleWait() {
	if e.state.FollowerWaitScheduled {
		// no-op
		return
	}

	group := arbiter.GroupFollowerWait
	wait := e.state.GenerateFollowerWait()

	e.a.Scheduler().ProcessSync(
		&scheduler.ScheduleAsyncEvent[arbiter.Group]{
			AsyncVariant: scheduler.TimerAsync(
				true,
				[]arbiter.Group{group},
				wait,
				func() {
					// invoked on arbiter goroutine
					e.state.FollowerWaitScheduled = false

					if e.state.Role != m.RoleFollower {
						log.Printf(
							"%s: role=%s mismatch triggered: %s",
							e.c.LogPrefix,
							e.state.Role,
							group,
						)
						return
					}

					e.followerToCandidate()
				},
				nil,
			),
		},
	)

	e.state.FollowerWaitScheduled = true

	log.Printf(
		"%s: role=%s, scheduled<%v>: %s",
		e.c.LogPrefix,
		e.state.Role,
		wait,
		group,
	)
}

// invoked on arbiter goroutine
func (e *Election) followerReleaseWait() {
	if !e.state.FollowerWaitScheduled {
		// no-op
		return
	}

	group := arbiter.GroupFollowerWait

	e.a.Scheduler().ProcessSync(
		&scheduler.ReleaseGroupEvent[arbiter.Group]{
			Group: group,
		},
	)

	e.state.FollowerWaitScheduled = false

	log.Printf(
		"%s: role=%s, released: %s",
		e.c.LogPrefix,
		e.state.Role,
		group,
	)
}

// invoked on arbiter goroutine
func (e *Election) followerToCandidate() {
	// reaching here implies quorum is intact
	oldRole := e.state.Role
	newRole := m.RoleCandidate
	e.state.Role = newRole

	log.Printf(
		"%s: role=%s -> %s",
		e.c.LogPrefix,
		oldRole,
		newRole,
	)

	e.candidateRequestVote()
}

// invoked on arbiter goroutine
func (e *Election) followerToCouncil(peerID string, ephemeral bool) {
	oldRole := e.state.Role
	newRole := m.RoleCouncil
	e.state.Role = newRole

	log.Printf(
		"%s: role=%s -> %s",
		e.c.LogPrefix,
		oldRole,
		newRole,
	)

	e.councilLockPeer(peerID, ephemeral)
}

// invoked on arbiter goroutine
func (e *Election) followerParticipantInit(connState *tp.ConnState) {
	e.commonParticipantInit(connState)

	e.followerCheckQuorum()
}

// invoked on arbiter goroutine
func (e *Election) followerParticipantExit(connState *tp.ConnState) {
	cvd := connState.Data.Load()
	e.commonParticipantExit(cvd)

	e.followerCheckQuorum()
}

// invoked on arbiter goroutine
func (e *Election) followerCandidateVoteRequest(p *tp.Client, connState *tp.ConnState, candidateVoteRequest *m.CandidateVoteRequest) {
	var vote uint8 = 0
	reason := m.CandidateVoteReasonInvalid
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
	if candidateVoteRequest.Term < e.state.VotedTerm {
		vote = 0
		reason = m.CandidateVoteReasonTermTooLow

		log.Printf(
			"%s: %s: role=%s, votedTerm=%d, requestTerm=%d, vote=%d, reason=%s",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.VotedTerm,
			candidateVoteRequest.Term,
			vote,
			reason,
		)
	} else if candidateVoteRequest.Term == e.state.VotedTerm {
		vote = 0
		reason = m.CandidateVoteReasonTermVoted

		log.Printf(
			"%s: %s: role=%s, votedTerm=%d, requestTerm=%d, vote=%d, reason=%s",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.VotedTerm,
			candidateVoteRequest.Term,
			vote,
			reason,
		)
	} else {
		vote = 1
		reason = m.CandidateVoteReasonAgreed

		oldTerm := e.state.VotedTerm
		newTerm := candidateVoteRequest.Term
		e.state.VotedTerm = newTerm

		log.Printf(
			"%s: %s: role=%s, votedTerm=%d -> %d, requestTerm=%d, vote=%d, reason=%s",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			oldTerm,
			newTerm,
			candidateVoteRequest.Term,
			vote,
			reason,
		)
	}
}

// invoked on arbiter goroutine
func (e *Election) followerCandidateVoteResponse(connState *tp.ConnState, candidateVoteResponse *m.CandidateVoteResponse) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, no-op vote-response, term=%d, vote=%d, reason=%s",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		candidateVoteResponse.Term,
		candidateVoteResponse.Vote,
		candidateVoteResponse.Reason,
	)
}

// invoked on arbiter goroutine
func (e *Election) followerNomineeAckRequest(p *tp.Client, connState *tp.ConnState, nomineeAckRequest *m.NomineeAckRequest) {
	var ack uint8 = 1
	reason := m.NomineeAckReasonAgreed
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
		"%s: %s: role=%s, requestTerm=%d, ack=%d, reason=%s",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		nomineeAckRequest.Term,
		ack,
		reason,
	)

	e.followerReleaseWait()

	e.followerToCouncil(cvd.PeerID, true)
}

// invoked on arbiter goroutine
func (e *Election) followerNomineeAckResponse(connState *tp.ConnState, nomineeAckResponse *m.NomineeAckResponse) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, no-op ack-response, term=%d, ack=%d, reason=%s",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		nomineeAckResponse.Term,
		nomineeAckResponse.Ack,
		nomineeAckResponse.Reason,
	)
}

// invoked on arbiter goroutine
func (e *Election) followerNomineeRelinquish(connState *tp.ConnState, nomineeRelinquish *m.NomineeRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, no-op nominee-relinquish, term=%d, reason=%s",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		nomineeRelinquish.Term,
		nomineeRelinquish.Reason,
	)
}

// invoked on arbiter goroutine
func (e *Election) followerAscendantRelinquish(connState *tp.ConnState, ascendantRelinquish *m.AscendantRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, no-op ascendant-relinquish, term=%d, reason=%s",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		ascendantRelinquish.Term,
		ascendantRelinquish.Reason,
	)
}

// invoked on arbiter goroutine
func (e *Election) followerLeaderAnnounce(connState *tp.ConnState, leaderAnnounce *m.LeaderAnnounce) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, leaderTerm=%d",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		leaderAnnounce.Term,
	)

	e.followerReleaseWait()

	e.followerToCouncil(cvd.PeerID, false)
}

// invoked on arbiter goroutine
func (e *Election) followerLeaderRelinquish(connState *tp.ConnState, leaderRelinquish *m.LeaderRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, no-op leader-relinquish, term=%d, reason=%s",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		leaderRelinquish.Term,
		leaderRelinquish.Reason,
	)
}
