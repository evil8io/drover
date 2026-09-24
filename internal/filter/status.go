package filter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

const (
	reasonInternalError = "InternalError"
	reasonTooLarge      = "RequestEntityTooLarge"
	reasonThrottled     = "TooManyRequests"
	reasonForbidden     = "Forbidden"
	reasonUnavailable   = "ServiceUnavailable"
	reasonExpired       = "Expired"
)

type statusBody struct {
	Kind       string         `json:"kind"`
	APIVersion string         `json:"apiVersion"`
	Metadata   map[string]any `json:"metadata"`
	Status     string         `json:"status"`
	Message    string         `json:"message"`
	Reason     string         `json:"reason"`
	Code       int            `json:"code"`
}

func statusJSON(code int, reason, message string) []byte {
	body, _ := json.Marshal(statusBody{
		Kind:       "Status",
		APIVersion: "v1",
		Metadata:   map[string]any{},
		Status:     "Failure",
		Message:    message,
		Reason:     reason,
		Code:       code,
	})
	return body
}

func statusResponse(req *http.Request, code int, reason, message string) *http.Response {
	body := statusJSON(code, reason, message)
	return &http.Response{
		Status:     fmt.Sprintf("%d %s", code, http.StatusText(code)),
		StatusCode: code,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header: http.Header{
			"Content-Type":   []string{"application/json"},
			"Content-Length": []string{strconv.Itoa(len(body))},
		},
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}

func writeStatus(w http.ResponseWriter, code int, reason, message string) {
	body := statusJSON(code, reason, message)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(code)
	_, _ = w.Write(body)
}
