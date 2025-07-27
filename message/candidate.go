package message

type CandidateVoteReason uint8

const (
	CandidateVoteReasonInvalid     CandidateVoteReason = 0
	CandidateVoteReasonAgreed      CandidateVoteReason = 1
	CandidateVoteReasonTermTooLow  CandidateVoteReason = 2
	CandidateVoteReasonTermVoted   CandidateVoteReason = 3
	CandidateVoteReasonRoleNominee CandidateVoteReason = 4
	CandidateVoteReasonRoleCouncil CandidateVoteReason = 5
	CandidateVoteReasonRoleLeader  CandidateVoteReason = 6
)

func (r CandidateVoteReason) String() string {
	switch r {
	case CandidateVoteReasonInvalid:
		return "Invalid Reason"
	case CandidateVoteReasonAgreed:
		return "Agreed"
	case CandidateVoteReasonTermTooLow:
		return "Term Too Low"
	case CandidateVoteReasonTermVoted:
		return "Term Voted"
	case CandidateVoteReasonRoleNominee:
		return "Role Nominee"
	case CandidateVoteReasonRoleCouncil:
		return "Role Council"
	case CandidateVoteReasonRoleLeader:
		return "Role Leader"
	default:
		return "Unknown Reason"
	}
}

type CandidateVoteRequest struct {
	Term uint32 `json:"term"`
}

type CandidateVoteResponse struct {
	Term   uint32              `json:"term"`
	Vote   uint8               `json:"vote"`
	Reason CandidateVoteReason `json:"reason"`
}
