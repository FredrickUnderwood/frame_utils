package frame_utils

import (
	"errors"
	"fmt"
)

// Default error start with 10001
// Service error start with 20001
const (
	SuccessCode           = 0
	ServerInternalErrCode = 10001
	ParamErrCode          = 10002
)

type ErrNo struct {
	ErrCode int64
	ErrMsg  string
}

func (e ErrNo) Error() string {
	return fmt.Sprintf("err_code:%d, err_msg:%s", e.ErrCode, e.ErrMsg)
}

func NewErrNo(code int64, msg string) ErrNo {
	return ErrNo{ErrCode: code, ErrMsg: msg}
}

var (
	Success           = NewErrNo(SuccessCode, "Success")
	ServerInternalErr = NewErrNo(ServerInternalErrCode, "Server internal error")
	ParamErr          = NewErrNo(ParamErrCode, "Parameter error")
)

func ConvertErr(err error) ErrNo {
	errNo := ErrNo{}
	if errors.As(err, &errNo) {
		return errNo
	}

	e := ServerInternalErr
	e.ErrMsg = err.Error()
	return e
}
