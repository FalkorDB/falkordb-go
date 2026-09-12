package falkordb

import "errors"

var (
	ErrRecordNoValue = errors.New("no value")

	// ErrUnsupportedType is returned when a value cannot be represented as a
	// Cypher literal, for example when it is passed as a query parameter.
	ErrUnsupportedType = errors.New("unsupported type")
)
