package message

type AscendantRelinquishReason uint8

const (
	AscendantRelinquishReasonInvalid    AscendantRelinquishReason = 0
	AscendantRelinquishReasonQuorumLoss AscendantRelinquishReason = 1
)

func (r AscendantRelinquishReason) String() string {
	switch r {
	case AscendantRelinquishReasonInvalid:
		return "Invalid Reason"
	case AscendantRelinquishReasonQuorumLoss:
		return "Quorum Loss"
	default:
		return "Unknown Reason"
	}
}

type AscendantRelinquish struct {
	Term   uint32                    `json:"term"`
	Reason AscendantRelinquishReason `json:"reason"`
}
