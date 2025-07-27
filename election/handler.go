package election

import (
	m "github.com/Meander-Cloud/go-elect/message"
	tp "github.com/Meander-Cloud/go-elect/net/tcp/protocol"
)

type Handler struct {
	e *Election
}

func (h *Handler) ParticipantInit(*tp.Server, *tp.ConnState, *m.ParticipantInit) {

}

func (h *Handler) ParticipantExit(*tp.Server, *tp.ConnState, *m.ParticipantExit) {

}

func (h *Handler) CandidateVoteRequest(*tp.Client, *tp.ConnState, *m.CandidateVoteRequest) {

}

func (h *Handler) CandidateVoteResponse(*tp.Server, *tp.ConnState, *m.CandidateVoteResponse) {

}

func (h *Handler) NomineeAckRequest(*tp.Client, *tp.ConnState, *m.NomineeAckRequest) {

}

func (h *Handler) NomineeAckResponse(*tp.Server, *tp.ConnState, *m.NomineeAckResponse) {

}

func (h *Handler) NomineeRelinquish(*tp.Client, *tp.ConnState, *m.NomineeRelinquish) {

}

func (h *Handler) LeaderAnnounce(*tp.Client, *tp.ConnState, *m.LeaderAnnounce) {

}

func (h *Handler) LeaderRelinquish(*tp.Client, *tp.ConnState, *m.LeaderRelinquish) {

}
