package server

type HttpError interface {
	error
	Code() int
}

type httperr struct {
	error
	code int
}

func (e httperr) Code() int {
	return e.code
}

func newerr(code int, err error) error {
	return httperr{
		code:  code,
		error: err,
	}
}
