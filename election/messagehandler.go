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
	case m.RoleCandidate:
	case m.RoleNominee:
	case m.RoleCouncil:
	case m.RoleAscendant:
	case m.RoleLeader:
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
	case m.RoleCandidate:
	case m.RoleNominee:
	case m.RoleCouncil:
	case m.RoleAscendant:
	case m.RoleLeader:
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
	case m.RoleCandidate:
	case m.RoleNominee:
	case m.RoleCouncil:
	case m.RoleAscendant:
	case m.RoleLeader:
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
	case m.RoleCandidate:
	case m.RoleNominee:
	case m.RoleCouncil:
	case m.RoleAscendant:
	case m.RoleLeader:
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
	case m.RoleCandidate:
	case m.RoleNominee:
	case m.RoleCouncil:
	case m.RoleAscendant:
	case m.RoleLeader:
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
	case m.RoleCandidate:
	case m.RoleNominee:
	case m.RoleCouncil:
	case m.RoleAscendant:
	case m.RoleLeader:
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
	case m.RoleCandidate:
	case m.RoleNominee:
	case m.RoleCouncil:
	case m.RoleAscendant:
	case m.RoleLeader:
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
	case m.RoleCandidate:
	case m.RoleNominee:
	case m.RoleCouncil:
	case m.RoleAscendant:
	case m.RoleLeader:
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
