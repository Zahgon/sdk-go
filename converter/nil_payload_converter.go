package converter

import (
	commonpb "go.temporal.io/api/common/v1"
)

// NilPayloadConverter doesn't set Data field in payload.
type NilPayloadConverter struct {
}

// NewNilPayloadConverter creates new instance of NilPayloadConverter.
func NewNilPayloadConverter() *NilPayloadConverter { _ = "STUB: not implemented"; return nil }

// ToPayload converts single nil value to payload.
func (c *NilPayloadConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// FromPayload converts single nil value from payload.
func (c *NilPayloadConverter) FromPayload(_ *commonpb.Payload, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// ToString converts payload object into human readable string.
func (c *NilPayloadConverter) ToString(*commonpb.Payload) string {
	_ = "STUB: not implemented"

	// Encoding returns MetadataEncodingNil.
	return ""
}

func (c *NilPayloadConverter) Encoding() string { _ = "STUB: not implemented"; return "" }
