package rows

import (
	"errors"
)

// Some common error definitions
var (
	ErrInvalidType = errors.New("Invalid type")
	ErrNotPointer  = errors.New("Destination not a pointer")
	ErrPointerNil  = errors.New("destination pointer is nil")
)
