package serializer

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
)

type (

	// SerializationError is an error type for serialization
	SerializationError struct {
		msg string
	}

	// DeserializationError is an error type for deserialization
	DeserializationError struct {
		msg string
	}

	// UnknownEncodingTypeError is an error type for unknown or unsupported encoding type
	UnknownEncodingTypeError struct {
		encodingType enumspb.EncodingType
	}

	// Marshaler is implemented by objects that can marshal themselves
	Marshaler interface {
		Marshal() ([]byte, error)
	}
)

// SerializeBatchEvents serializes batch events into a datablob proto
func SerializeBatchEvents(events []*historypb.HistoryEvent, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func serializeProto(p Marshaler, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Shouldn't happen, but keeping

// DeserializeBatchEvents deserializes batch events from a datablob proto
func DeserializeBatchEvents(data *commonpb.DataBlob) ([]*historypb.HistoryEvent, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func serialize(input interface{}, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// For backward-compatibility

// NewUnknownEncodingTypeError returns a new instance of encoding type error
func NewUnknownEncodingTypeError(encodingType enumspb.EncodingType) error {
	_ = "STUB: not implemented"
	return nil
}

func (e *UnknownEncodingTypeError) Error() string { _ = "STUB: not implemented"; return "" }

// NewSerializationError returns a SerializationError
func NewSerializationError(msg string) error { _ = "STUB: not implemented"; return nil }

func (e *SerializationError) Error() string { _ = "STUB: not implemented"; return "" }

// NewDeserializationError returns a DeserializationError
func NewDeserializationError(msg string) error { _ = "STUB: not implemented"; return nil }

func (e *DeserializationError) Error() string { _ = "STUB: not implemented"; return "" }

// NewDataBlob creates new blob data
func NewDataBlob(data []byte, encodingType enumspb.EncodingType) *commonpb.DataBlob {
	_ = "STUB: not implemented"
	return nil
}

// DeserializeBlobDataToHistoryEvents deserialize the blob data to history event data
func DeserializeBlobDataToHistoryEvents(
	dataBlobs []*commonpb.DataBlob, filterType enumspb.HistoryEventFilterType,
) (*historypb.History, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
