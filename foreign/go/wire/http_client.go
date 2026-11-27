package wire

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// A typed client for the managed HTTP surface, over an injected transport.
//
// The client never builds a URL beyond the path: the transport owns the base
// URL, the credentials, and the headers. Authentication and any refresh-retry
// live in the transport too, which keeps the client itself auth-agnostic and
// gives a real application one place to inject what it needs.
//
// Binary key-value keys are base64url-encoded into the path by the client, so
// raw bytes never enter a path segment. A namespace, fork id, or projection id
// is placed verbatim, so a caller passes a path-safe identifier.

// Method is the HTTP verb a transport performs.
type Method string

const (
	MethodGet    Method = "GET"
	MethodPost   Method = "POST"
	MethodPut    Method = "PUT"
	MethodDelete Method = "DELETE"
)

// HTTPRequest is one request for the transport to perform.
//
// The path is already the full path plus query string, and the transport
// prepends its own base URL. A body is present only on a write, and when it is, the
// transport sends it as JSON.
type HTTPRequest struct {
	Method Method
	Path   string
	Body   []byte
}

// HTTPHeader is one response header.
type HTTPHeader struct {
	Name  string
	Value string
}

// HTTPResponse is what the transport returns.
//
// A transport that does not surface headers leaves them empty, in which case
// header-carried metadata reads as absent rather than failing the call.
type HTTPResponse struct {
	Status  uint16
	Headers []HTTPHeader
	Body    []byte
}

// NewHTTPResponse is a response with no headers.
func NewHTTPResponse(status uint16, body []byte) HTTPResponse {
	return HTTPResponse{Status: status, Body: body}
}

// WithHeader attaches a header.
func (response HTTPResponse) WithHeader(name, value string) HTTPResponse {
	response.Headers = append(response.Headers, HTTPHeader{Name: name, Value: value})
	return response
}

// Header is the first value of a header, matched case-insensitively because
// HTTP header names are.
func (response HTTPResponse) Header(name string) (string, bool) {
	for _, header := range response.Headers {
		if strings.EqualFold(header.Name, name) {
			return header.Value, true
		}
	}
	return "", false
}

// IsSuccess reports a 2xx status.
func (response HTTPResponse) IsSuccess() bool {
	return response.Status >= 200 && response.Status < 300
}

// KvReadValue is the result of a single-key read: the raw bytes plus the
// optional absolute expiry.
//
// A scan page carries the base64 entry view instead, because a JSON array
// cannot hold raw bytes.
type KvReadValue struct {
	Value           []byte
	ExpiresAtMicros *uint64
}

// Transport is the IO seam.
//
// A non-2xx status is not an error here: the client inspects the status and
// decodes the failure body. Return an error only when the request never
// produced a response at all.
//
// The context carries the caller's deadline and cancellation, so a transport
// must abandon an in-flight request when it is done.
type Transport interface {
	Send(ctx context.Context, request HTTPRequest) (HTTPResponse, error)
}

// ClientErrorKind is why a typed call failed.
type ClientErrorKind uint8

const (
	// ClientTransportFailed means the transport never got a response.
	ClientTransportFailed ClientErrorKind = iota
	// ClientDecodeFailed means a response arrived but did not decode as the
	// expected type.
	ClientDecodeFailed
	// ClientAPIFailed means the server returned a classified failure body.
	ClientAPIFailed
	// ClientStatusFailed means a failure status arrived without a classified
	// body, such as a gateway's plain-text error.
	ClientStatusFailed
)

// ClientError is why a typed call failed.
//
// A caller branches on the result code rather than matching the message text,
// which is for people and may change.
type ClientError struct {
	Kind ClientErrorKind
	// Body is the classified failure, present for an API or status failure.
	Body ErrorBody
	// Status is the HTTP status of a failure with no classified body.
	Status uint16
	// Detail explains a decode failure.
	Detail string
	// Cause is the transport's own failure, surfaced verbatim.
	Cause error
}

func (failure *ClientError) Error() string {
	switch failure.Kind {
	case ClientTransportFailed:
		return fmt.Sprintf("transport error: %v", failure.Cause)
	case ClientDecodeFailed:
		return fmt.Sprintf("decode error: %s", failure.Detail)
	case ClientStatusFailed:
		return fmt.Sprintf("http status %d (%s): %s",
			failure.Status, failure.Body.Code, failure.Body.Message)
	default:
		return fmt.Sprintf("api error (%s): %s", failure.Body.Code, failure.Body.Message)
	}
}

// Unwrap exposes the transport's own failure, so a caller can match on it.
func (failure *ClientError) Unwrap() error { return failure.Cause }

// Code is the classified result code of a server failure.
//
// The second result is false for a transport or decode failure, which the
// server never saw.
func (failure *ClientError) Code() (ResultCode, bool) {
	switch failure.Kind {
	case ClientAPIFailed, ClientStatusFailed:
		return failure.Body.Code, true
	default:
		return 0, false
	}
}

// ErrorCode is the classified result code of err, when it is a client failure
// the server classified.
func ErrorCode(err error) (ResultCode, bool) {
	var failure *ClientError
	if errors.As(err, &failure) {
		return failure.Code()
	}
	return 0, false
}

// HTTPClient is a typed client for the managed surface.
//
// It holds no mutable state of its own, so one client may be used from several
// goroutines as long as its transport may be.
type HTTPClient struct {
	transport Transport
}

// NewHTTPClient wraps a transport, which owns the base URL and the
// credentials.
func NewHTTPClient(transport Transport) *HTTPClient {
	return &HTTPClient{transport: transport}
}

// Transport is the seam this client sends through.
func (client *HTTPClient) Transport() Transport { return client.transport }

// dispatch performs one request.
func (client *HTTPClient) dispatch(
	ctx context.Context, method Method, path string, body []byte,
) (HTTPResponse, error) {
	response, err := client.transport.Send(ctx,
		HTTPRequest{Method: method, Path: path, Body: body})
	if err != nil {
		return HTTPResponse{}, &ClientError{Kind: ClientTransportFailed, Cause: err}
	}
	return response, nil
}

// sendJSON performs a request with a JSON body and decodes the reply.
func sendJSON[Reply any](
	ctx context.Context, client *HTTPClient, method Method, path string, body any,
) (Reply, error) {
	var reply Reply
	payload, err := MarshalJSON(body)
	if err != nil {
		return reply, &ClientError{
			Kind: ClientDecodeFailed, Detail: "request body: " + err.Error()}
	}
	response, err := client.dispatch(ctx, method, path, payload)
	if err != nil {
		return reply, err
	}
	return decodeOk[Reply](response)
}

// sendEmpty performs a request with no body and decodes the reply.
func sendEmpty[Reply any](
	ctx context.Context, client *HTTPClient, method Method, path string,
) (Reply, error) {
	var reply Reply
	response, err := client.dispatch(ctx, method, path, nil)
	if err != nil {
		return reply, err
	}
	return decodeOk[Reply](response)
}

// sendJSONOk performs a request with a JSON body whose success reply is empty.
func (client *HTTPClient) sendJSONOk(
	ctx context.Context, method Method, path string, body any,
) error {
	payload, err := MarshalJSON(body)
	if err != nil {
		return &ClientError{
			Kind: ClientDecodeFailed, Detail: "request body: " + err.Error()}
	}
	response, err := client.dispatch(ctx, method, path, payload)
	if err != nil {
		return err
	}
	return checkStatus(response)
}

// expectOk performs a request whose success reply is empty.
func (client *HTTPClient) expectOk(
	ctx context.Context, method Method, path string, body []byte,
) error {
	response, err := client.dispatch(ctx, method, path, body)
	if err != nil {
		return err
	}
	return checkStatus(response)
}

// decodeOk reads the bare success payload, or turns a failure into a
// classified error.
func decodeOk[Reply any](response HTTPResponse) (Reply, error) {
	var reply Reply
	if !response.IsSuccess() {
		return reply, apiError(response)
	}
	if err := UnmarshalJSON(response.Body, &reply); err != nil {
		return reply, &ClientError{
			Kind: ClientDecodeFailed, Detail: "response body: " + err.Error()}
	}
	return reply, nil
}

// checkStatus accepts a success and classifies a failure, for a route whose
// success reply is empty.
func checkStatus(response HTTPResponse) error {
	if response.IsSuccess() {
		return nil
	}
	return apiError(response)
}

// apiError classifies a failure response.
//
// A body that carries a result code is used as-is. One that does not falls
// back to a code inferred from the status, so a gateway's plain-text failure
// still reaches the caller as something it can branch on.
func apiError(response HTTPResponse) error {
	var body ErrorBody
	if err := UnmarshalJSON(response.Body, &body); err == nil && body.Code.IsRecognized() {
		return &ClientError{Kind: ClientAPIFailed, Body: body}
	}
	return &ClientError{
		Kind:   ClientStatusFailed,
		Status: response.Status,
		Body: NewErrorBody(
			codeForStatus(response.Status), string(response.Body)),
	}
}

// codeForStatus is a best-effort classification of a failure that carried no
// result code, inferred from the status alone.
func codeForStatus(status uint16) ResultCode {
	switch status {
	case 404:
		return ResultNotFound
	case 400:
		return ResultInvalidArgument
	case 401:
		return ResultUnauthenticated
	case 403:
		return ResultForbidden
	case 409:
		return ResultConflict
	case 413:
		return ResultTooLarge
	case 501:
		return ResultUnsupported
	case 503:
		return ResultStale
	default:
		return ResultBackend
	}
}

// withQuery appends parameters to a path that may already carry a query
// string.
func withQuery(path string, parameters []QueryParameter) string {
	query := QueryString(parameters)
	switch {
	case query == "":
		return path
	case strings.Contains(path, "?"):
		return path + "&" + query[1:]
	default:
		return path + query
	}
}

// base64urlAlphabet is URL-safe unpadded base64, the encoding this surface
// uses for every binary value.
const base64urlAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

// Base64URLEncode encodes bytes as URL-safe unpadded base64.
func Base64URLEncode(input []byte) string {
	var out strings.Builder
	out.Grow((len(input) + 2) / 3 * 4)
	for at := 0; at < len(input); at += 3 {
		chunk := input[at:min(at+3, len(input))]
		first := int(chunk[0])
		out.WriteByte(base64urlAlphabet[first>>2])
		switch len(chunk) {
		case 1:
			out.WriteByte(base64urlAlphabet[(first&0b11)<<4])
		case 2:
			second := int(chunk[1])
			out.WriteByte(base64urlAlphabet[(first&0b11)<<4|second>>4])
			out.WriteByte(base64urlAlphabet[(second&0b1111)<<2])
		default:
			second, third := int(chunk[1]), int(chunk[2])
			out.WriteByte(base64urlAlphabet[(first&0b11)<<4|second>>4])
			out.WriteByte(base64urlAlphabet[(second&0b1111)<<2|third>>6])
			out.WriteByte(base64urlAlphabet[third&0b111111])
		}
	}
	return out.String()
}

// base64urlReverse maps a character back to its six bits, with 0xff for one
// that is not in the alphabet.
var base64urlReverse = func() [256]byte {
	var table [256]byte
	for index := range table {
		table[index] = 0xff
	}
	for index := 0; index < len(base64urlAlphabet); index++ {
		table[base64urlAlphabet[index]] = byte(index)
	}
	return table
}()

// Base64URLDecode reads URL-safe unpadded base64.
func Base64URLDecode(text string) ([]byte, error) {
	if len(text)%4 == 1 {
		return nil, &InvalidError{Reason: "base64url text is truncated"}
	}
	out := make([]byte, 0, len(text)/4*3)
	var accumulator uint32
	bits := 0
	for index := 0; index < len(text); index++ {
		value := base64urlReverse[text[index]]
		if value == 0xff {
			return nil, &InvalidError{Reason: fmt.Sprintf(
				"base64url text holds %q", text[index])}
		}
		accumulator = accumulator<<6 | uint32(value)
		bits += 6
		if bits >= 8 {
			bits -= 8
			out = append(out, byte(accumulator>>bits))
		}
	}
	// The leftover bits of an unpadded run must be zero, or the text named
	// bits that no byte carries.
	if bits > 0 && accumulator&(1<<bits-1) != 0 {
		return nil, &InvalidError{Reason: "base64url text has trailing bits"}
	}
	return out, nil
}
