package election

import (
	"log"

	m "github.com/Meander-Cloud/go-elect/message"
	tp "github.com/Meander-Cloud/go-elect/net/tcp/protocol"
)

type MessageHandler struct {
	e *Election
}

// invoked on arbiter goroutine
func (h *MessageHandler) ParticipantInit(p *tp.Server, connState *tp.ConnState, participantInit *m.ParticipantInit) {
	switch h.e.state.Role {
	case m.RoleFollower:
		h.e.followerParticipantInit(connState)
	case m.RoleCandidate:
		h.e.candidateParticipantInit(p, connState)
	case m.RoleNominee:
		h.e.nomineeParticipantInit(p, connState)
	case m.RoleCouncil:
		h.e.councilParticipantInit(connState)
	case m.RoleAscendant:
		h.e.ascendantParticipantInit(connState)
	case m.RoleLeader:
		h.e.leaderParticipantInit(p, connState)
	default:
		log.Printf(
			"%s: %s: role=%s, cannot process participantInit=%+v",
			h.e.c.LogPrefix,
			connState.Data.Load().Descriptor,
			h.e.state.Role,
			participantInit,
		)
	}
}

// invoked on arbiter goroutine
func (h *MessageHandler) ParticipantExit(p *tp.Server, connState *tp.ConnState, participantExit *m.ParticipantExit) {
	switch h.e.state.Role {
	case m.RoleFollower:
		h.e.followerParticipantExit(connState)
	case m.RoleCandidate:
		h.e.candidateParticipantExit(connState)
	case m.RoleNominee:
		h.e.nomineeParticipantExit(connState)
	case m.RoleCouncil:
		h.e.councilParticipantExit(connState)
	case m.RoleAscendant:
		h.e.ascendantParticipantExit(connState)
	case m.RoleLeader:
		h.e.leaderParticipantExit(connState)
	default:
		log.Printf(
			"%s: %s: role=%s, cannot process participantExit=%+v",
			h.e.c.LogPrefix,
			connState.Data.Load().Descriptor,
			h.e.state.Role,
			participantExit,
		)
	}
}

// invoked on arbiter goroutine
func (h *MessageHandler) CandidateVoteRequest(p *tp.Client, connState *tp.ConnState, candidateVoteRequest *m.CandidateVoteRequest) {
	switch h.e.state.Role {
	case m.RoleFollower:
		h.e.followerCandidateVoteRequest(p, connState, candidateVoteRequest)
	case m.RoleCandidate:
		h.e.candidateCandidateVoteRequest(p, connState, candidateVoteRequest)
	case m.RoleNominee:
		h.e.nomineeCandidateVoteRequest(p, connState, candidateVoteRequest)
	case m.RoleCouncil:
		h.e.councilCandidateVoteRequest(p, connState, candidateVoteRequest)
	case m.RoleAscendant:
		h.e.ascendantCandidateVoteRequest(p, connState, candidateVoteRequest)
	case m.RoleLeader:
		h.e.leaderCandidateVoteRequest(p, connState, candidateVoteRequest)
	default:
		log.Printf(
			"%s: %s: role=%s, cannot process candidateVoteRequest=%+v",
			h.e.c.LogPrefix,
			connState.Data.Load().Descriptor,
			h.e.state.Role,
			candidateVoteRequest,
		)
	}
}

// invoked on arbiter goroutine
func (h *MessageHandler) CandidateVoteResponse(p *tp.Server, connState *tp.ConnState, candidateVoteResponse *m.CandidateVoteResponse) {
	switch h.e.state.Role {
	case m.RoleFollower:
		h.e.followerCandidateVoteResponse(connState, candidateVoteResponse)
	case m.RoleCandidate:
		h.e.candidateCandidateVoteResponse(connState, candidateVoteResponse)
	case m.RoleNominee:
		h.e.nomineeCandidateVoteResponse(connState, candidateVoteResponse)
	case m.RoleCouncil:
		h.e.councilCandidateVoteResponse(connState, candidateVoteResponse)
	case m.RoleAscendant:
		h.e.ascendantCandidateVoteResponse(connState, candidateVoteResponse)
	case m.RoleLeader:
		h.e.leaderCandidateVoteResponse(connState, candidateVoteResponse)
	default:
		log.Printf(
			"%s: %s: role=%s, cannot process candidateVoteResponse=%+v",
			h.e.c.LogPrefix,
			connState.Data.Load().Descriptor,
			h.e.state.Role,
			candidateVoteResponse,
		)
	}
}

// invoked on arbiter goroutine
func (h *MessageHandler) NomineeAckRequest(p *tp.Client, connState *tp.ConnState, nomineeAckRequest *m.NomineeAckRequest) {
	switch h.e.state.Role {
	case m.RoleFollower:
		h.e.followerNomineeAckRequest(p, connState, nomineeAckRequest)
	case m.RoleCandidate:
		h.e.candidateNomineeAckRequest(p, connState, nomineeAckRequest)
	case m.RoleNominee:
		h.e.nomineeNomineeAckRequest(p, connState, nomineeAckRequest)
	case m.RoleCouncil:
		h.e.councilNomineeAckRequest(p, connState, nomineeAckRequest)
	case m.RoleAscendant:
		h.e.ascendantNomineeAckRequest(p, connState, nomineeAckRequest)
	case m.RoleLeader:
		h.e.leaderNomineeAckRequest(p, connState, nomineeAckRequest)
	default:
		log.Printf(
			"%s: %s: role=%s, cannot process nomineeAckRequest=%+v",
			h.e.c.LogPrefix,
			connState.Data.Load().Descriptor,
			h.e.state.Role,
			nomineeAckRequest,
		)
	}
}

// invoked on arbiter goroutine
func (h *MessageHandler) NomineeAckResponse(p *tp.Server, connState *tp.ConnState, nomineeAckResponse *m.NomineeAckResponse) {
	switch h.e.state.Role {
	case m.RoleFollower:
		h.e.followerNomineeAckResponse(connState, nomineeAckResponse)
	case m.RoleCandidate:
		h.e.candidateNomineeAckResponse(connState, nomineeAckResponse)
	case m.RoleNominee:
		h.e.nomineeNomineeAckResponse(connState, nomineeAckResponse)
	case m.RoleCouncil:
		h.e.councilNomineeAckResponse(connState, nomineeAckResponse)
	case m.RoleAscendant:
		h.e.ascendantNomineeAckResponse(connState, nomineeAckResponse)
	case m.RoleLeader:
		h.e.leaderNomineeAckResponse(connState, nomineeAckResponse)
	default:
		log.Printf(
			"%s: %s: role=%s, cannot process nomineeAckResponse=%+v",
			h.e.c.LogPrefix,
			connState.Data.Load().Descriptor,
			h.e.state.Role,
			nomineeAckResponse,
		)
	}
}

// invoked on arbiter goroutine
func (h *MessageHandler) NomineeRelinquish(p *tp.Client, connState *tp.ConnState, nomineeRelinquish *m.NomineeRelinquish) {
	switch h.e.state.Role {
	case m.RoleFollower:
		h.e.followerNomineeRelinquish(connState, nomineeRelinquish)
	case m.RoleCandidate:
		h.e.candidateNomineeRelinquish(connState, nomineeRelinquish)
	case m.RoleNominee:
		h.e.nomineeNomineeRelinquish(connState, nomineeRelinquish)
	case m.RoleCouncil:
		h.e.councilNomineeRelinquish(connState, nomineeRelinquish)
	case m.RoleAscendant:
		h.e.ascendantNomineeRelinquish(connState, nomineeRelinquish)
	case m.RoleLeader:
		h.e.leaderNomineeRelinquish(connState, nomineeRelinquish)
	default:
		log.Printf(
			"%s: %s: role=%s, cannot process nomineeRelinquish=%+v",
			h.e.c.LogPrefix,
			connState.Data.Load().Descriptor,
			h.e.state.Role,
			nomineeRelinquish,
		)
	}
}

// invoked on arbiter goroutine
func (h *MessageHandler) AscendantRelinquish(p *tp.Client, connState *tp.ConnState, ascendantRelinquish *m.AscendantRelinquish) {
	switch h.e.state.Role {
	case m.RoleFollower:
		h.e.followerAscendantRelinquish(connState, ascendantRelinquish)
	case m.RoleCandidate:
		h.e.candidateAscendantRelinquish(connState, ascendantRelinquish)
	case m.RoleNominee:
		h.e.nomineeAscendantRelinquish(connState, ascendantRelinquish)
	case m.RoleCouncil:
		h.e.councilAscendantRelinquish(connState, ascendantRelinquish)
	case m.RoleAscendant:
		h.e.ascendantAscendantRelinquish(connState, ascendantRelinquish)
	case m.RoleLeader:
		h.e.leaderAscendantRelinquish(connState, ascendantRelinquish)
	default:
		log.Printf(
			"%s: %s: role=%s, cannot process ascendantRelinquish=%+v",
			h.e.c.LogPrefix,
			connState.Data.Load().Descriptor,
			h.e.state.Role,
			ascendantRelinquish,
		)
	}
}

// invoked on arbiter goroutine
func (h *MessageHandler) LeaderAnnounce(p *tp.Client, connState *tp.ConnState, leaderAnnounce *m.LeaderAnnounce) {
	switch h.e.state.Role {
	case m.RoleFollower:
		h.e.followerLeaderAnnounce(connState, leaderAnnounce)
	case m.RoleCandidate:
		h.e.candidateLeaderAnnounce(connState, leaderAnnounce)
	case m.RoleNominee:
		h.e.nomineeLeaderAnnounce(connState, leaderAnnounce)
	case m.RoleCouncil:
		h.e.councilLeaderAnnounce(connState, leaderAnnounce)
	case m.RoleAscendant:
		h.e.ascendantLeaderAnnounce(connState, leaderAnnounce)
	case m.RoleLeader:
		h.e.leaderLeaderAnnounce(connState, leaderAnnounce)
	default:
		log.Printf(
			"%s: %s: role=%s, cannot process leaderAnnounce=%+v",
			h.e.c.LogPrefix,
			connState.Data.Load().Descriptor,
			h.e.state.Role,
			leaderAnnounce,
		)
	}
}

// invoked on arbiter goroutine
func (h *MessageHandler) LeaderRelinquish(p *tp.Client, connState *tp.ConnState, leaderRelinquish *m.LeaderRelinquish) {
	switch h.e.state.Role {
	case m.RoleFollower:
		h.e.followerLeaderRelinquish(connState, leaderRelinquish)
	case m.RoleCandidate:
		h.e.candidateLeaderRelinquish(connState, leaderRelinquish)
	case m.RoleNominee:
		h.e.nomineeLeaderRelinquish(connState, leaderRelinquish)
	case m.RoleCouncil:
		h.e.councilLeaderRelinquish(connState, leaderRelinquish)
	case m.RoleAscendant:
		h.e.ascendantLeaderRelinquish(connState, leaderRelinquish)
	case m.RoleLeader:
		h.e.leaderLeaderRelinquish(connState, leaderRelinquish)
	default:
		log.Printf(
			"%s: %s: role=%s, cannot process leaderRelinquish=%+v",
			h.e.c.LogPrefix,
			connState.Data.Load().Descriptor,
			h.e.state.Role,
			leaderRelinquish,
		)
	}
}
