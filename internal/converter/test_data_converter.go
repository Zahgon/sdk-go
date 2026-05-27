package converter

import (
	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/sdk/converter"
)

const (
	metadataEncodingGob = "binary/gob"
)

// TestDataConverter implements DataConverter using gob.
type TestDataConverter struct{}

// NewTestDataConverter created new instance of TestDataConverter.
func NewTestDataConverter() converter.DataConverter {
	_ = "STUB: not implemented"
	return *new(converter.DataConverter)
}

// ToPayloads converts a list of values.
func (dc *TestDataConverter) ToPayloads(values ...interface{}) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// FromPayloads converts to a list of values of different types.
func (dc *TestDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// ToPayload converts single value to payload.
func (dc *TestDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// FromPayload converts single value from payload.
func (dc *TestDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// ToStrings converts payloads object into human readable strings.
func (dc *TestDataConverter) ToStrings(payloads *commonpb.Payloads) []string {
	_ = "STUB: not implemented"
	return nil
}

// ToString converts payload object into human readable string.
func (dc *TestDataConverter) ToString(payload *commonpb.Payload) string {
	_ = "STUB: not implemented"
	return ""
}
