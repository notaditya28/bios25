package api

import (
	"errors"
	"fmt"
	"net/http"
)

type Code string

const (
	CodeInvalidRequest     Code = "INVALID_REQUEST"
	CodeUnknownTopic       Code = "UNKNOWN_TOPIC"
	CodeUnknownPartition   Code = "UNKNOWN_PARTITION"
	CodeTopicExists        Code = "TOPIC_EXISTS"
	CodeOffsetOutOfRange   Code = "OFFSET_OUT_OF_RANGE"
	CodeNotLeader          Code = "NOT_LEADER"          // retry against the leader named in the error
	CodeStaleEpoch         Code = "STALE_EPOCH"         // request fenced by a newer epoch
	CodeNotEnoughReplicas  Code = "NOT_ENOUGH_REPLICAS" // quorum unreachable; write not acknowledged
	CodeRegionUnavailable  Code = "REGION_UNAVAILABLE"  // the region owning this data is down or isolated
	CodeResidencyViolation Code = "RESIDENCY_VIOLATION"
	CodeWrongRegion        Code = "WRONG_REGION"       // topic is resident in another region; Leader lists its brokers
	CodePolicyUnavailable  Code = "POLICY_UNAVAILABLE" // no valid signed policy for this data: fail closed
	CodeInternal           Code = "INTERNAL"
)

// Error is a typed, client-visible error. Reason is a short machine-friendly cause
// (e.g. "region_crashed", "region_isolated"); Leader hints where to retry for NOT_LEADER.
type Error struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
	Reason  string `json:"reason,omitempty"`
	Leader  string `json:"leader,omitempty"`
}

func (e *Error) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("%s (%s): %s", e.Code, e.Reason, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func Errorf(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// CodeOf returns the code of err if it is an *Error, else CodeInternal.
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return CodeInternal
}

// HTTPStatus maps an error code to an HTTP status.
func (c Code) HTTPStatus() int {
	switch c {
	case CodeInvalidRequest:
		return http.StatusBadRequest
	case CodeUnknownTopic, CodeUnknownPartition:
		return http.StatusNotFound
	case CodeTopicExists:
		return http.StatusConflict
	case CodeOffsetOutOfRange:
		return http.StatusRequestedRangeNotSatisfiable
	case CodeNotLeader, CodeWrongRegion:
		return http.StatusMisdirectedRequest
	case CodeStaleEpoch:
		return http.StatusConflict
	case CodeResidencyViolation:
		return http.StatusForbidden
	case CodeNotEnoughReplicas, CodeRegionUnavailable, CodePolicyUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
