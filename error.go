package falkordb

import "errors"

var (
	ErrRecordNoValue = errors.New("no value")

	// ErrInvalidName is returned when a parameter name or map key is empty or
	// contains a backtick or NUL byte, which FalkorDB cannot represent.
	ErrInvalidName = errors.New("invalid parameter name or map key")
)
