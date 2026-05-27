// package nexusclient implements a Nexus client that communicates with a Nexus HTTP API endpoint.
// It is copied from the implementation embedded in the server repo for testing purposes.
package nexusclient

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	"github.com/nexus-rpc/sdk-go/nexus"
)

// HTTPClientOptions are options for creating an [HTTPClient].
type HTTPClientOptions struct {
	// Base URL for all requests. Required.
	BaseURL string
	// Service name. Required.
	Service string
	// A function for making HTTP requests.
	// Defaults to [http.DefaultClient.Do].
	HTTPCaller func(*http.Request) (*http.Response, error)
	// A [Serializer] to customize client serialization behavior.
	// By default the client handles JSONables, byte slices, and nil.
	Serializer nexus.Serializer
	// A [FailureConverter] to convert a [Failure] instance to and from an [error]. Defaults to
	// [DefaultFailureConverter].
	FailureConverter failureConverter
}

// User-Agent header set on HTTP requests.
const userAgent = "temporalio/server"

const headerUserAgent = "User-Agent"

var errEmptyOperationName = errors.New("empty operation name")

var errEmptyOperationToken = errors.New("empty operation token")

// UnexpectedResponseError indicates a client encountered something unexpected in the server's response.
type UnexpectedResponseError struct {
	// Error message.
	Message string
	// Optional failure that may have been emedded in the response.
	Failure *nexus.Failure
	// Additional transport specific details.
	// For HTTP, this would include the HTTP response. The response body will have already been read into memory and
	// does not need to be closed.
	Details any
}

// Error implements the error interface.
func (e *UnexpectedResponseError) Error() string { _ = "STUB: not implemented"; return "" }

func newUnexpectedResponseError(message string, response *http.Response, body []byte) error {
	_ = "STUB: not implemented"
	return nil
}

// An HTTPClient makes Nexus service requests as defined in the [Nexus HTTP API].
//
// It can start a new operation and get an [OperationHandle] to an existing, asynchronous operation.
//
// Use an [OperationHandle] to cancel, get the result of, and get information about asynchronous operations.
//
// OperationHandles can be obtained either by starting new operations or by calling [HTTPClient.NewOperationHandle] for
// existing operations.
//
// [Nexus HTTP API]: https://github.com/nexus-rpc/api
type HTTPClient struct {
	// The options this client was created with after applying defaults.
	options        HTTPClientOptions
	serviceBaseURL *url.URL
}

// NewHTTPClient creates a new [HTTPClient] from provided [HTTPClientOptions].
// BaseURL and Service are required.
func NewHTTPClient(options HTTPClientOptions) (*HTTPClient, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ClientStartOperationResponse is the return type of [HTTPClient.StartOperation].
// One and only one of Successful or Pending will be non-nil.
type ClientStartOperationResponse[T any] struct {
	// Set when start completes synchronously and successfully.
	//
	// If T is a [LazyValue], ensure that your consume it or read the underlying content in its entirety and close it to
	// free up the underlying connection.
	Successful T
	// Set when the handler indicates that it started an asynchronous operation.
	// The attached handle can be used to perform actions such as cancel the operation or get its result.
	Pending *OperationHandle[T]
	// Links contain information about the operations done by the handler.
	Links []nexus.Link
}

// StartOperation calls the configured Nexus endpoint to start an operation.
//
// This method has the following possible outcomes:
//
//  1. The operation completes successfully. The result of this call will be set as a [LazyValue] in
//     ClientStartOperationResult.Successful and must be consumed to free up the underlying connection.
//
//  2. The operation was started and the handler has indicated that it will complete asynchronously. An
//     [OperationHandle] will be returned as ClientStartOperationResult.Pending, which can be used to perform actions
//     such as getting its result.
//
//  3. The operation was unsuccessful. The returned result will be nil and error will be an
//     [OperationError].
//
//  4. Any other error.
//
// nolint:revive // (cyclomatic complexity) Containing the entire implementation inline is clearer here.
func (c *HTTPClient) StartOperation(
	ctx context.Context,
	operation string,
	input any,
	options nexus.StartOperationOptions,
) (*ClientStartOperationResponse[*nexus.LazyValue], error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Close the input reader in case we error before sending the HTTP request (which may double close but
// that's fine since we ignore the error).
// nolint:errcheck // double close is fine

// Have to read body here to check if it is a Failure.

// Do not close response body here to allow successful result to read it.

// Do this once here and make sure it doesn't leak.

// NewOperationHandle gets a handle to an asynchronous operation by name and token.
// Does not incur a trip to the server.
// Fails if provided an empty operation or token.
func (c *HTTPClient) NewOperationHandle(operation string, token string) (*OperationHandle[*nexus.LazyValue], error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// readAndReplaceBody reads the response body in its entirety and closes it, and then replaces the original response
// body with an in-memory buffer.
// The body is replaced even when there was an error reading the entire body.
func readAndReplaceBody(response *http.Response) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func operationInfoFromResponse(response *http.Response, body []byte) (*nexus.OperationInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *HTTPClient) failureFromResponse(response *http.Response, body []byte) (nexus.Failure, error) {
	_ = "STUB: not implemented"
	return *new(nexus.Failure), nil
}

func (c *HTTPClient) failureFromResponseOrDefault(response *http.Response, body []byte, defaultMessage string) nexus.Failure {
	_ = "STUB: not implemented"
	return *new(nexus.Failure)
}

func (c *HTTPClient) failureErrorFromResponseOrDefault(response *http.Response, body []byte, defaultMessage string) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *HTTPClient) bestEffortHandlerErrorFromResponse(response *http.Response, body []byte) error {
	_ = "STUB: not implemented"
	return nil
}

func retryBehaviorFromHeader(header http.Header) nexus.HandlerErrorRetryBehavior {
	_ = "STUB: not implemented"
	return *new(nexus.HandlerErrorRetryBehavior)
}

func getUnsuccessfulStateFromHeader(response *http.Response, body []byte) (nexus.OperationState, error) {
	_ = "STUB: not implemented"
	return *new(nexus.OperationState), nil
}

// StartOperation is the type safe version of [HTTPClient.StartOperation].
// It accepts input of type I and returns a [ClientStartOperationResponse] of type O, removing the need to consume the
// [LazyValue] returned by the client method.
func StartOperation[I, O any](ctx context.Context, client *HTTPClient, operation nexus.OperationReference[I, O], input I, request nexus.StartOperationOptions) (*ClientStartOperationResponse[O], error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func ExecuteOperation[I, O any](ctx context.Context, client *HTTPClient, operation nexus.OperationReference[I, O], input I, request nexus.StartOperationOptions) (O, error) {
	_ = "STUB: not implemented"
	return *new(O), nil
}

type failureConverter interface {
	// ErrorToFailure converts an [error] to a [Failure].
	// Implementors should take a best-effort approach and never fail this method.
	// Note that the provided error may be nil.
	ErrorToFailure(error) nexus.Failure
	// ErrorToFailure converts a [Failure] to an [error].
	// Implementors should take a best-effort approach and never fail this method.
	FailureToError(nexus.Failure) error
}

type failureErrorFailureConverter struct{}

// ErrorToFailure implements FailureConverter.
func (e failureErrorFailureConverter) ErrorToFailure(err error) nexus.Failure {
	_ = "STUB: not implemented"
	return *new(nexus.Failure)
}

// FailureToError implements FailureConverter.
func (e failureErrorFailureConverter) FailureToError(f nexus.Failure) error {
	_ = "STUB: not implemented"
	return nil
}
