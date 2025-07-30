package arbiter

type Group uint8

const (
	GroupInvalid              Group = 0
	GroupFollowerWait         Group = 1
	GroupCandidateVoteWait    Group = 2
	GroupNomineeAckWait       Group = 3
	GroupAscendantAssertWait  Group = 4
	GroupLeaderQuorumLossWait Group = 5
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
	case GroupAscendantAssertWait:
		return "Ascendant Assert Wait"
	case GroupLeaderQuorumLossWait:
		return "Leader Quorum Loss Wait"
	default:
		return "Unknown Group"
	}
}
