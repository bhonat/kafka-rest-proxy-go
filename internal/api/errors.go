package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/example/kafka-rest-proxy-go/internal/producer"
)

const (
	errorCodeBadRequest         = 400
	errorCodeUnauthorized       = 401
	errorCodeNotAcceptable      = 406
	errorCodeUnsupportedMedia   = 415
	errorCodeUnprocessable      = 42201
	errorCodeTooManyRequests    = 429
	errorCodeProduceUnavailable = 503
	errorCodeTimeout            = 504
)

type validationError struct {
	message string
}

func (e validationError) Error() string {
	return e.message
}

func writeAPIError(w http.ResponseWriter, status int, code int, message string) {
	w.Header().Set("Content-Type", mediaKafkaV2)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{
		ErrorCode: code,
		Message:   message,
	})
}

func classifyProduceError(err error) (status int, code int, message string) {
	switch {
	case errors.Is(err, producer.ErrOverloaded):
		return http.StatusTooManyRequests, errorCodeTooManyRequests, "producer is overloaded"
	case errors.Is(err, contextCanceled):
		return http.StatusGatewayTimeout, errorCodeTimeout, "produce request timed out"
	default:
		return http.StatusServiceUnavailable, errorCodeProduceUnavailable, err.Error()
	}
}

var contextCanceled = errors.New("context canceled or timed out")
