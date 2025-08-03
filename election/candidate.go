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
						"%s: role=%s, selfTerm=%d, wait timeout, participant<%d> vote: %d-%d/%d<%d>",
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
func (e *Election) candidateToCouncil(peerID string, ephemeral bool) {
	clear(e.state.CandidateVoteYesMap)
	clear(e.state.CandidateVoteNoMap)

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
func (e *Election) candidateParticipantExit(connState *tp.ConnState) {
	cvd := connState.Data.Load()
	e.commonParticipantExit(cvd)

	participantCount := uint16(len(e.state.PeerMap)) + 1

	func() {
		var found bool

		_, found = e.state.CandidateVoteYesMap[cvd.PeerID]
		if found {
			delete(e.state.CandidateVoteYesMap, cvd.PeerID)

			log.Printf(
				"%s: %s: role=%s, selfTerm=%d, removed vote yes, participant<%d> vote: %d-%d/%d<%d>",
				e.c.LogPrefix,
				cvd.Descriptor,
				e.state.Role,
				e.state.SelfTerm,
				participantCount,
				len(e.state.CandidateVoteYesMap)+1,
				len(e.state.CandidateVoteNoMap),
				e.state.QuorumParticipantCount,
				e.state.TotalParticipantCount,
			)
		}

		_, found = e.state.CandidateVoteNoMap[cvd.PeerID]
		if found {
			delete(e.state.CandidateVoteNoMap, cvd.PeerID)

			log.Printf(
				"%s: %s: role=%s, selfTerm=%d, removed vote no, participant<%d> vote: %d-%d/%d<%d>",
				e.c.LogPrefix,
				cvd.Descriptor,
				e.state.Role,
				e.state.SelfTerm,
				participantCount,
				len(e.state.CandidateVoteYesMap)+1,
				len(e.state.CandidateVoteNoMap),
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
			"%s: role=%s, selfTerm=%d, quorum loss, participant<%d> vote: %d-%d/%d<%d>",
			e.c.LogPrefix,
			e.state.Role,
			e.state.SelfTerm,
			participantCount,
			len(e.state.CandidateVoteYesMap)+1,
			len(e.state.CandidateVoteNoMap),
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)

		e.candidateReleaseVoteWait()

		e.candidateToFollower()
	}()
}

// invoked on arbiter goroutine
func (e *Election) candidateCandidateVoteRequest(p *tp.Client, connState *tp.ConnState, candidateVoteRequest *m.CandidateVoteRequest) {
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
			"%s: %s: role=%s, selfTerm=%d, votedTerm=%d, requestTerm=%d, vote=%d, reason=%s, participant<%d> vote: %d-%d/%d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.SelfTerm,
			e.state.VotedTerm,
			candidateVoteRequest.Term,
			vote,
			reason,
			len(e.state.PeerMap)+1,
			len(e.state.CandidateVoteYesMap)+1,
			len(e.state.CandidateVoteNoMap),
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)
	} else if candidateVoteRequest.Term == e.state.VotedTerm {
		vote = 0
		reason = m.CandidateVoteReasonTermVoted

		log.Printf(
			"%s: %s: role=%s, selfTerm=%d, votedTerm=%d, requestTerm=%d, vote=%d, reason=%s, participant<%d> vote: %d-%d/%d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.SelfTerm,
			e.state.VotedTerm,
			candidateVoteRequest.Term,
			vote,
			reason,
			len(e.state.PeerMap)+1,
			len(e.state.CandidateVoteYesMap)+1,
			len(e.state.CandidateVoteNoMap),
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)
	} else {
		vote = 1
		reason = m.CandidateVoteReasonAgreed

		oldTerm := e.state.VotedTerm
		newTerm := candidateVoteRequest.Term
		e.state.VotedTerm = newTerm

		log.Printf(
			"%s: %s: role=%s, selfTerm=%d, votedTerm=%d -> %d, requestTerm=%d, vote=%d, reason=%s, participant<%d> vote: %d-%d/%d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.SelfTerm,
			oldTerm,
			newTerm,
			candidateVoteRequest.Term,
			vote,
			reason,
			len(e.state.PeerMap)+1,
			len(e.state.CandidateVoteYesMap)+1,
			len(e.state.CandidateVoteNoMap),
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)

		e.candidateReleaseVoteWait()

		e.candidateToFollower()
	}
}

// invoked on arbiter goroutine
func (e *Election) candidateCandidateVoteResponse(connState *tp.ConnState, candidateVoteResponse *m.CandidateVoteResponse) {
	cvd := connState.Data.Load()

	if candidateVoteResponse.Term != e.state.SelfTerm {
		log.Printf(
			"%s: %s: role=%s, selfTerm=%d, unexpected vote-response, term=%d, vote=%d, reason=%s, participant<%d> vote: %d-%d/%d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.SelfTerm,
			candidateVoteResponse.Term,
			candidateVoteResponse.Vote,
			candidateVoteResponse.Reason,
			len(e.state.PeerMap)+1,
			len(e.state.CandidateVoteYesMap)+1,
			len(e.state.CandidateVoteNoMap),
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)
		return
	}

	var found bool

	_, found = e.state.CandidateVoteYesMap[cvd.PeerID]
	if found {
		log.Printf(
			"%s: %s: role=%s, selfTerm=%d, duplicate<YesMap> vote-response, term=%d, vote=%d, reason=%s, participant<%d> vote: %d-%d/%d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.SelfTerm,
			candidateVoteResponse.Term,
			candidateVoteResponse.Vote,
			candidateVoteResponse.Reason,
			len(e.state.PeerMap)+1,
			len(e.state.CandidateVoteYesMap)+1,
			len(e.state.CandidateVoteNoMap),
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)
		return
	}

	_, found = e.state.CandidateVoteNoMap[cvd.PeerID]
	if found {
		log.Printf(
			"%s: %s: role=%s, selfTerm=%d, duplicate<NoMap> vote-response, term=%d, vote=%d, reason=%s, participant<%d> vote: %d-%d/%d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.SelfTerm,
			candidateVoteResponse.Term,
			candidateVoteResponse.Vote,
			candidateVoteResponse.Reason,
			len(e.state.PeerMap)+1,
			len(e.state.CandidateVoteYesMap)+1,
			len(e.state.CandidateVoteNoMap),
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)
		return
	}

	switch candidateVoteResponse.Vote {
	case 1:
		func() {
			e.state.CandidateVoteYesMap[cvd.PeerID] = struct{}{}

			voteYesCount := uint16(len(e.state.CandidateVoteYesMap)) + 1

			log.Printf(
				"%s: %s: role=%s, selfTerm=%d, added vote yes, participant<%d> vote: %d-%d/%d<%d>",
				e.c.LogPrefix,
				cvd.Descriptor,
				e.state.Role,
				e.state.SelfTerm,
				len(e.state.PeerMap)+1,
				voteYesCount,
				len(e.state.CandidateVoteNoMap),
				e.state.QuorumParticipantCount,
				e.state.TotalParticipantCount,
			)

			if voteYesCount >= e.state.QuorumParticipantCount {
				e.candidateReleaseVoteWait()

				e.candidateToNominee()
			}
		}()
	case 0:
		func() {
			e.state.CandidateVoteNoMap[cvd.PeerID] = struct{}{}

			voteNoCount := uint16(len(e.state.CandidateVoteNoMap))

			log.Printf(
				"%s: %s: role=%s, selfTerm=%d, added vote no, participant<%d> vote: %d-%d/%d<%d>",
				e.c.LogPrefix,
				cvd.Descriptor,
				e.state.Role,
				e.state.SelfTerm,
				len(e.state.PeerMap)+1,
				len(e.state.CandidateVoteYesMap)+1,
				voteNoCount,
				e.state.QuorumParticipantCount,
				e.state.TotalParticipantCount,
			)

			if voteNoCount >= e.state.QuorumParticipantCount {
				e.candidateReleaseVoteWait()

				e.candidateToFollower()
			}
		}()
	default:
		log.Printf(
			"%s: %s: role=%s, selfTerm=%d, invalid vote-response, term=%d, vote=%d, reason=%s, participant<%d> vote: %d-%d/%d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.SelfTerm,
			candidateVoteResponse.Term,
			candidateVoteResponse.Vote,
			candidateVoteResponse.Reason,
			len(e.state.PeerMap)+1,
			len(e.state.CandidateVoteYesMap)+1,
			len(e.state.CandidateVoteNoMap),
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)
	}
}

// invoked on arbiter goroutine
func (e *Election) candidateNomineeAckRequest(p *tp.Client, connState *tp.ConnState, nomineeAckRequest *m.NomineeAckRequest) {
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
		"%s: %s: role=%s, selfTerm=%d, requestTerm=%d, ack=%d, reason=%s, participant<%d> vote: %d-%d/%d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		nomineeAckRequest.Term,
		ack,
		reason,
		len(e.state.PeerMap)+1,
		len(e.state.CandidateVoteYesMap)+1,
		len(e.state.CandidateVoteNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)

	e.candidateReleaseVoteWait()

	e.candidateToCouncil(cvd.PeerID, true)
}

// invoked on arbiter goroutine
func (e *Election) candidateNomineeAckResponse(connState *tp.ConnState, nomineeAckResponse *m.NomineeAckResponse) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op ack-response, term=%d, ack=%d, reason=%s, participant<%d> vote: %d-%d/%d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		nomineeAckResponse.Term,
		nomineeAckResponse.Ack,
		nomineeAckResponse.Reason,
		len(e.state.PeerMap)+1,
		len(e.state.CandidateVoteYesMap)+1,
		len(e.state.CandidateVoteNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) candidateNomineeRelinquish(connState *tp.ConnState, nomineeRelinquish *m.NomineeRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op nominee-relinquish, term=%d, reason=%s, participant<%d> vote: %d-%d/%d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		nomineeRelinquish.Term,
		nomineeRelinquish.Reason,
		len(e.state.PeerMap)+1,
		len(e.state.CandidateVoteYesMap)+1,
		len(e.state.CandidateVoteNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) candidateAscendantRelinquish(connState *tp.ConnState, ascendantRelinquish *m.AscendantRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op ascendant-relinquish, term=%d, reason=%s, participant<%d> vote: %d-%d/%d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		ascendantRelinquish.Term,
		ascendantRelinquish.Reason,
		len(e.state.PeerMap)+1,
		len(e.state.CandidateVoteYesMap)+1,
		len(e.state.CandidateVoteNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) candidateLeaderAnnounce(connState *tp.ConnState, leaderAnnounce *m.LeaderAnnounce) {

}

// invoked on arbiter goroutine
func (e *Election) candidateLeaderRelinquish(connState *tp.ConnState, leaderRelinquish *m.LeaderRelinquish) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, selfTerm=%d, no-op leader-relinquish, term=%d, reason=%s, participant<%d> vote: %d-%d/%d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.SelfTerm,
		leaderRelinquish.Term,
		leaderRelinquish.Reason,
		len(e.state.PeerMap)+1,
		len(e.state.CandidateVoteYesMap)+1,
		len(e.state.CandidateVoteNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}
