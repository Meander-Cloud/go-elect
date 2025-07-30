package message

type Message struct {
	Txseq  uint64 `json:"txseq"`
	Txtime int64  `json:"txtime"` // epoch milliseconds

	ParticipantInit *ParticipantInit `json:"participant_init,omitempty" msgpack:",omitempty"`
	ParticipantExit *ParticipantExit `json:"participant_exit,omitempty" msgpack:",omitempty"`

	CandidateVoteRequest  *CandidateVoteRequest  `json:"candidate_vote_request,omitempty" msgpack:",omitempty"`
	CandidateVoteResponse *CandidateVoteResponse `json:"candidate_vote_response,omitempty" msgpack:",omitempty"`

	NomineeAckRequest  *NomineeAckRequest  `json:"nominee_ack_request,omitempty" msgpack:",omitempty"`
	NomineeAckResponse *NomineeAckResponse `json:"nominee_ack_response,omitempty" msgpack:",omitempty"`
	NomineeRelinquish  *NomineeRelinquish  `json:"nominee_relinquish,omitempty" msgpack:",omitempty"`

	AscendantRelinquish *AscendantRelinquish `json:"ascendant_relinquish,omitempty" msgpack:",omitempty"`

	LeaderAnnounce   *LeaderAnnounce   `json:"leader_announce,omitempty" msgpack:",omitempty"`
	LeaderRelinquish *LeaderRelinquish `json:"leader_relinquish,omitempty" msgpack:",omitempty"`
}
