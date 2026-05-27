package serializer

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type (
	// JSONPBEncoder is JSON encoder/decoder for protobuf structs and slices of protobuf structs.
	JSONPBEncoder struct {
		opts protojson.MarshalOptions
	}
)

// NewJSONPBEncoder creates a new JSONPBEncoder.
func NewJSONPBEncoder() JSONPBEncoder {
	_ = "STUB: not implemented"
	return *

	// NewJSONPBIndentEncoder creates a new JSONPBEncoder with indent.
	new(JSONPBEncoder)
}

func NewJSONPBIndentEncoder(indent string) JSONPBEncoder {
	_ = "STUB: not implemented"
	return *new(JSONPBEncoder)
}

// Encode protobuf struct to bytes.
func (e JSONPBEncoder) Encode(pb proto.Message) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil,

		// Decode bytes to protobuf struct.
		nil
}

func (e JSONPBEncoder) Decode(data []byte, pb proto.Message) error {
	_ = "STUB: not implemented"
	return nil
}
