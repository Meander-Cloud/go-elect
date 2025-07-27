package message

type NomineeAckReason uint8

const (
	NomineeAckReasonInvalid     NomineeAckReason = 0
	NomineeAckReasonAgreed      NomineeAckReason = 1
	NomineeAckReasonRoleNominee NomineeAckReason = 2
	NomineeAckReasonRoleCouncil NomineeAckReason = 3
	NomineeAckReasonRoleLeader  NomineeAckReason = 4
)

func (r NomineeAckReason) String() string {
	switch r {
	case NomineeAckReasonInvalid:
		return "Invalid Reason"
	case NomineeAckReasonAgreed:
		return "Agreed"
	case NomineeAckReasonRoleNominee:
		return "Role Nominee"
	case NomineeAckReasonRoleCouncil:
		return "Role Council"
	case NomineeAckReasonRoleLeader:
		return "Role Leader"
	default:
		return "Unknown Reason"
	}
}

type NomineeRelinquishReason uint8

const (
	NomineeRelinquishReasonInvalid         NomineeRelinquishReason = 0
	NomineeRelinquishReasonAckFailed       NomineeRelinquishReason = 1
	NomineeRelinquishReasonAckTimeout      NomineeRelinquishReason = 2
	NomineeRelinquishReasonQuorumLoss      NomineeRelinquishReason = 3
	NomineeRelinquishReasonLeaderAnnounced NomineeRelinquishReason = 4
)

func (r NomineeRelinquishReason) String() string {
	switch r {
	case NomineeRelinquishReasonInvalid:
		return "Invalid Reason"
	case NomineeRelinquishReasonAckFailed:
		return "Ack Failed"
	case NomineeRelinquishReasonAckTimeout:
		return "Ack Timeout"
	case NomineeRelinquishReasonQuorumLoss:
		return "Quorum Loss"
	case NomineeRelinquishReasonLeaderAnnounced:
		return "Leader Announced"
	default:
		return "Unknown Reason"
	}
}

type NomineeAckRequest struct {
	Term uint32 `json:"term"`
}

type NomineeAckResponse struct {
	Term   uint32           `json:"term"`
	Ack    uint8            `json:"ack"`
	Reason NomineeAckReason `json:"reason"`
}

type NomineeRelinquish struct {
	Term   uint32                  `json:"term"`
	Reason NomineeRelinquishReason `json:"reason"`
}
