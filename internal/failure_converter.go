package internal

import (
	failurepb "go.temporal.io/api/failure/v1"

	"go.temporal.io/sdk/converter"
)

var defaultFailureConverter = NewDefaultFailureConverter(DefaultFailureConverterOptions{})

// GetDefaultFailureConverter returns the default failure converter used by Temporal.
//
// Exposed as: [go.temporal.io/sdk/temporal.GetDefaultFailureConverter]
func GetDefaultFailureConverter() converter.FailureConverter {
	_ = "STUB: not implemented"
	return *new(converter.FailureConverter)
}

// DefaultFailureConverterOptions are optional parameters for DefaultFailureConverter creation.
//
// Exposed as: [go.temporal.io/sdk/temporal.DefaultFailureConverterOptions]
type DefaultFailureConverterOptions struct {
	// Optional: Sets DataConverter to customize serialization/deserialization of fields.
	//
	// default: Default data converter
	DataConverter converter.DataConverter

	// Optional: Whether to encode error messages and stack traces.
	//
	// default: false
	EncodeCommonAttributes bool
}

// DefaultFailureConverter seralizes errors with the option to encode common parameters under Failure.EncodedAttributes
//
// Exposed as: [go.temporal.io/sdk/temporal.DefaultFailureConverter]
type DefaultFailureConverter struct {
	dataConverter          converter.DataConverter
	encodeCommonAttributes bool
}

// NewDefaultFailureConverter creates new instance of DefaultFailureConverter.
//
// Exposed as: [go.temporal.io/sdk/temporal.NewDefaultFailureConverter]
func NewDefaultFailureConverter(opt DefaultFailureConverterOptions) *DefaultFailureConverter {
	_ = "STUB: not implemented"
	return nil
}

// ErrorToFailure converts an error to a Failure
func (dfc *DefaultFailureConverter) ErrorToFailure(err error) *failurepb.Failure {
	_ = "STUB: not implemented"
	return nil
}

// If there was an error converting the original failure, we will ignore it
// since we don't want to fail the entire conversion just because we couldn't convert the original failure.

// Copy the message if present, if not we will keep the default message set above

// All unknown errors are considered to be retryable ApplicationFailureInfo.

// FailureToError converts an Failure to an error
func (dfc *DefaultFailureConverter) FailureToError(failure *failurepb.Failure) error {
	_ = "STUB: not implemented"
	return nil
}

// Copy the original future to pass to the failureHolder

//lint:ignore SA1019 ignore deprecated old operation id

// Check if the message is the default message or if it was set by the user. If it's the default message,
// we will replace it with an empty string to avoid confusion.

// All unknown types are considered to be retryable ApplicationError.
