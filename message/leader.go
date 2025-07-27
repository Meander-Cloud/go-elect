package message

type LeaderRelinquishReason uint8

const (
	LeaderRelinquishReasonInvalid    LeaderRelinquishReason = 0
	LeaderRelinquishReasonQuorumLoss LeaderRelinquishReason = 1
)

func (r LeaderRelinquishReason) String() string {
	switch r {
	case LeaderRelinquishReasonInvalid:
		return "Invalid Reason"
	case LeaderRelinquishReasonQuorumLoss:
		return "Quorum Loss"
	default:
		return "Unknown Reason"
	}
}

type LeaderAnnounce struct {
	Term uint32 `json:"term"`
}

type LeaderRelinquish struct {
	Term   uint32                 `json:"term"`
	Reason LeaderRelinquishReason `json:"reason"`
}
