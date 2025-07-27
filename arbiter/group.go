package arbiter

type Group uint8

const (
	GroupInvalid           Group = 0
	GroupFollowerWait      Group = 1
	GroupCandidateVoteWait Group = 2
	GroupNomineeAckWait    Group = 3
)

func (g Group) String() string {
	switch g {
	case GroupInvalid:
		return "Invalid Group"
	case GroupFollowerWait:
		return "Follower Wait"
	case GroupCandidateVoteWait:
		return "Candidate Vote Wait"
	case GroupNomineeAckWait:
		return "Nominee Ack Wait"
	default:
		return "Unknown Group"
	}
}
