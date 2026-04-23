package constants

import "github.com/Gentleman-Programming/engram/internal/store"

const (
	TargetKeyCloud = store.DefaultSyncTargetKey

	ReasonBlockedUnenrolled = "blocked_unenrolled"
	ReasonPaused            = "paused"
	ReasonAuthRequired      = "auth_required"
	ReasonPolicyForbidden   = "policy_forbidden"
	ReasonTransportFailed   = "transport_failed"
	ReasonCloudConfigError  = "cloud_config_error"
)

var DeterministicReasons = []string{
	ReasonBlockedUnenrolled,
	ReasonPaused,
	ReasonAuthRequired,
	ReasonPolicyForbidden,
	ReasonTransportFailed,
	ReasonCloudConfigError,
}
