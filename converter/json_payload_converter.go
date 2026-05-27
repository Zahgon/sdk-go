package converter

import (
	commonpb "go.temporal.io/api/common/v1"
)

// JSONPayloadConverter converts to/from JSON.
type JSONPayloadConverter struct {
}

// NewJSONPayloadConverter creates a new instance of JSONPayloadConverter.
func NewJSONPayloadConverter() *JSONPayloadConverter { _ = "STUB: not implemented"; return nil }

// ToPayload converts a single value to a payload.
func (c *JSONPayloadConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// FromPayload converts a single payload to a value.
func (c *JSONPayloadConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// ToString converts a payload object into a human-readable string.
func (c *JSONPayloadConverter) ToString(payload *commonpb.Payload) string {
	_ = "STUB: not implemented"
	return ""
}

// Encoding returns MetadataEncodingJSON.
func (c *JSONPayloadConverter) Encoding() string { _ = "STUB: not implemented"; return "" }
