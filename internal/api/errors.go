package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/bhonat/kafka-rest-proxy-go/internal/producer"
)

const (
	errorCodeBadRequest         = 400
	errorCodeUnauthorized       = 401
	errorCodeForbidden          = 403
	errorCodeNotAcceptable      = 406
	errorCodeUnsupportedMedia   = 415
	errorCodeUnprocessable      = 422
	errorCodeTooManyRequests    = 429
	errorCodeProduceUnavailable = 503
	errorCodeTimeout            = 504
)

const rateLimitMessage = "Request rate limit exceeded: The rate limit of requests per second has been exceeded."

type validationError struct {
	message string
}

func (e validationError) Error() string {
	return e.message
}

type unprocessableError struct {
	message string
}

func (e unprocessableError) Error() string {
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

func writeRateLimitExceeded(w http.ResponseWriter) {
	writeAPIError(w, http.StatusTooManyRequests, errorCodeTooManyRequests, rateLimitMessage)
}

func classifyProduceError(err error) (status int, code int, message string) {
	switch {
	case errors.Is(err, producer.ErrOverloaded):
		return http.StatusTooManyRequests, errorCodeTooManyRequests, "producer is overloaded"
	case errors.Is(err, errContextCanceled):
		return http.StatusGatewayTimeout, errorCodeTimeout, "produce request timed out"
	default:
		return http.StatusServiceUnavailable, errorCodeProduceUnavailable, err.Error()
	}
}

var errContextCanceled = errors.New("context canceled or timed out")
