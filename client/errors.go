package client

import "errors"

var (
	ErrNotFound    = errors.New("not found")
	ErrUnavailable = errors.New("unavailable, try again")
)
