package converter

import (
	commonpb "go.temporal.io/api/common/v1"
)

// ByteSlicePayloadConverter pass through []byte to Data field in payload.
type ByteSlicePayloadConverter struct {
}

// NewByteSlicePayloadConverter creates new instance of ByteSlicePayloadConverter.
func NewByteSlicePayloadConverter() *ByteSlicePayloadConverter {
	_ = "STUB: not implemented"
	return nil
}

// ToPayload converts single []byte value to payload.
func (c *ByteSlicePayloadConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// FromPayload converts single []byte value from payload.
func (c *ByteSlicePayloadConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// Must be a []byte.

// ToString converts payload object into human readable string.
func (c *ByteSlicePayloadConverter) ToString(payload *commonpb.Payload) string {
	_ = "STUB: not implemented"
	return ""
}

// Encoding returns MetadataEncodingBinary.
func (c *ByteSlicePayloadConverter) Encoding() string { _ = "STUB: not implemented"; return "" }
