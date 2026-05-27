package converter

import (
	commonpb "go.temporal.io/api/common/v1"
)

type (
	// CompositeDataConverter applies PayloadConverters in specified order.
	CompositeDataConverter struct {
		payloadConverters map[string]PayloadConverter
		orderedEncodings  []string
	}
)

// NewCompositeDataConverter creates a new instance of CompositeDataConverter from an ordered list of PayloadConverters.
// Order is important here because during serialization the DataConverter will try the PayloadConverters in
// that order until a PayloadConverter returns non nil payload.
// The last PayloadConverter should always serialize the value (JSONPayloadConverter is a good candidate for it).
func NewCompositeDataConverter(payloadConverters ...PayloadConverter) DataConverter {
	_ = "STUB: not implemented"
	return *new(DataConverter)
}

// ToPayloads converts a list of values.
func (dc *CompositeDataConverter) ToPayloads(values ...interface{}) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// FromPayloads converts to a list of values of different types.
func (dc *CompositeDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// ToPayload converts single value to payload.
func (dc *CompositeDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// FromPayload converts single value from payload.
func (dc *CompositeDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// ToString converts payload object into human readable string.
func (dc *CompositeDataConverter) ToString(payload *commonpb.Payload) string {
	_ = "STUB: not implemented"
	return ""
}

// ToStrings converts payloads object into human readable strings.
func (dc *CompositeDataConverter) ToStrings(payloads *commonpb.Payloads) []string {
	_ = "STUB: not implemented"
	return nil
}

func encoding(payload *commonpb.Payload) (string, error) { _ = "STUB: not implemented"; return "", nil }
