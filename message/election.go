package message

type Role uint8

const (
	RoleInvalid   Role = 0
	RoleFollower  Role = 1
	RoleCandidate Role = 2
	RoleNominee   Role = 3
	RoleCouncil   Role = 4
	RoleLeader    Role = 5
)

func (r Role) String() string {
	switch r {
	case RoleInvalid:
		return "Invalid Role"
	case RoleFollower:
		return "Follower"
	case RoleCandidate:
		return "Candidate"
	case RoleNominee:
		return "Nominee"
	case RoleCouncil:
		return "Council"
	case RoleLeader:
		return "Leader"
	default:
		return "Unknown Role"
	}
}
