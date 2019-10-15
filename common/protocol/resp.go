package protocol

import "encoding/json"

type Response struct {
	Code string
	Msg  string
	Data interface{}
}

// 返回应答
func BuildResp(code, msg, data string) (resp []byte, err error) {
	var (
		response Response
	)
	response.Code = code
	response.Msg = msg
	response.Data = data
	resp, err = json.Marshal(response)
	return
}
