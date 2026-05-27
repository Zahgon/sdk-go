package converter

import (
	"encoding/json"

	gogojsonpb "github.com/gogo/protobuf/jsonpb"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/temporalproto"
	"google.golang.org/protobuf/encoding/protojson"
)

// ProtoJSONPayloadConverter converts proto objects to/from JSON.
type ProtoJSONPayloadConverter struct {
	gogoMarshaler                 gogojsonpb.Marshaler
	gogoUnmarshaler               gogojsonpb.Unmarshaler
	protoMarshalOptions           protojson.MarshalOptions
	protoUnmarshalOptions         protojson.UnmarshalOptions
	temporalProtoUnmarshalOptions temporalproto.CustomJSONUnmarshalOptions
	options                       ProtoJSONPayloadConverterOptions
}

// ProtoJSONPayloadConverterOptions represents options for `NewProtoJSONPayloadConverterWithOptions`.
type ProtoJSONPayloadConverterOptions struct {
	// ExcludeProtobufMessageTypes prevents the message type (`my.package.MyMessage`)
	// from being included in the Payload.
	ExcludeProtobufMessageTypes bool

	// AllowUnknownFields will ignore unknown fields when unmarshalling, as opposed to returning an error
	AllowUnknownFields bool

	// UseProtoNames uses proto field name instead of lowerCamelCase name in JSON
	// field names.
	UseProtoNames bool

	// UseEnumNumbers emits enum values as numbers.
	UseEnumNumbers bool

	// EmitUnpopulated specifies whether to emit unpopulated fields.
	EmitUnpopulated bool

	// LegacyTemporalProtoCompat will allow enums serialized as SCREAMING_SNAKE_CASE.
	// Useful for backwards compatibility when migrating a proto message from gogoproto to standard protobuf.
	LegacyTemporalProtoCompat bool
}

var (
	jsonNil, _ = json.Marshal(nil)
)

// NewProtoJSONPayloadConverter creates new instance of `ProtoJSONPayloadConverter`.
func NewProtoJSONPayloadConverter() *ProtoJSONPayloadConverter {
	_ = "STUB: not implemented"
	return nil
}

// NewProtoJSONPayloadConverterWithOptions creates new instance of `ProtoJSONPayloadConverter` with the provided options.
func NewProtoJSONPayloadConverterWithOptions(options ProtoJSONPayloadConverterOptions) *ProtoJSONPayloadConverter {
	_ = "STUB: not implemented"
	return nil
}

// ToPayload converts single proto value to payload.
func (c *ProtoJSONPayloadConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	// Proto golang structs might be generated with 4 different protoc plugin versions:
	//   1. github.com/golang/protobuf - ~v1.3.5 is the most recent pre-APIv2 version of APIv1.
	//   2. github.com/golang/protobuf - ^v1.4.0 is a version of APIv1 implemented in terms of APIv2.
	//   3. google.golang.org/protobuf - ^v1.20.0 is APIv2.
	//   4. github.com/gogo/protobuf - any version.
	// Case 1 is not supported.
	// Cases 2 and 3 implements proto.Message and are the same in this context.
	// Case 4 implements gogoproto.Message.
	// It is important to check for proto.Message first because cases 2 and 3 also implement gogoproto.Message.
	return nil, nil
}

// FromPayload converts single proto value from payload.
func (c *ProtoJSONPayloadConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// If original value is of value type (i.e. commonpb.WorkflowType), create a pointer to it.

// protoValue is for sure of pointer type (i.e. *commonpb.WorkflowType).

// If original value is nil, create new instance.

// type assertion must always succeed

// type assertion must always succeed

// If original value wasn't a pointer then set value back to where valuePtr points to.

// ToString converts payload object into human readable string.
func (c *ProtoJSONPayloadConverter) ToString(payload *commonpb.Payload) string {
	_ = "STUB: not implemented"
	return ""
}

// Encoding returns MetadataEncodingProtoJSON.
func (c *ProtoJSONPayloadConverter) Encoding() string { _ = "STUB: not implemented"; return "" }

func (c *ProtoJSONPayloadConverter) ExcludeProtobufMessageTypes() bool {
	_ = "STUB: not implemented"
	return false
}
