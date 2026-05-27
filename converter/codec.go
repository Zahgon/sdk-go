package converter

import (
	"net/http"

	commonpb "go.temporal.io/api/common/v1"
)

// PayloadCodec is an codec that encodes or decodes the given payloads.
//
// For example, NewZlibCodec returns a PayloadCodec that can be used for
// compression.
// These can be used (and even chained) in NewCodecDataConverter.
type PayloadCodec interface {
	// Encode optionally encodes the given payloads which are guaranteed to never
	// be nil. The parameters must not be mutated.
	Encode([]*commonpb.Payload) ([]*commonpb.Payload, error)

	// Decode optionally decodes the given payloads which are guaranteed to never
	// be nil. The parameters must not be mutated.
	//
	// For compatibility reasons, implementers should take care not to decode
	// payloads that were not previously encoded.
	Decode([]*commonpb.Payload) ([]*commonpb.Payload, error)
}

// ZlibCodecOptions are options for NewZlibCodec. All fields are optional.
type ZlibCodecOptions struct {
	// If true, the zlib codec will encode the contents even if there is no size
	// benefit. Otherwise, the zlib codec will only use the encoded value if it
	// is smaller.
	AlwaysEncode bool
}

type zlibCodec struct{ options ZlibCodecOptions }

// NewZlibCodec creates a PayloadCodec for use in NewCodecDataConverter
// to support zlib payload compression.
//
// While this serves as a reasonable example of a compression encoder, callers
// may prefer alternative compression algorithms for lots of small payloads.
func NewZlibCodec(options ZlibCodecOptions) PayloadCodec {
	_ = "STUB: not implemented"
	return *new(PayloadCodec)
}

func (z *zlibCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Marshal and write

// Only set if smaller than original amount or has option to always encode

func (*zlibCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Only if it's our encoding

// Read all and unmarshal

func decodePayloads(payloads []*commonpb.Payload, codecs []PayloadCodec) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"

	// Iterate forwards decoding
	return nil, nil
}

func encodePayloads(payloads []*commonpb.Payload, codecs []PayloadCodec) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"

	// Iterate backwards encoding
	return nil, nil
}

// CodecDataConverter is a DataConverter that wraps an underlying data
// converter and supports chained encoding of just the payload without regard
// for serialization to/from actual types.
type CodecDataConverter struct {
	parent DataConverter
	codecs []PayloadCodec
}

// NewCodecDataConverter wraps the given parent DataConverter and performs
// encoding/decoding on the payload via the given codecs. When encoding for
// ToPayload(s), the codecs are applied last to first meaning the earlier
// encoders wrap the later ones. When decoding for FromPayload(s) and
// ToString(s), the decoders are applied first to last to reverse the effect.
func NewCodecDataConverter(parent DataConverter, codecs ...PayloadCodec) DataConverter {
	_ = "STUB: not implemented"
	return *new(DataConverter)
}

func (e *CodecDataConverter) encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (e *CodecDataConverter) decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ToPayload implements DataConverter.ToPayload performing encoding on the
// result of the parent's ToPayload call.
func (e *CodecDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ToPayloads implements DataConverter.ToPayloads performing encoding on the
// result of the parent's ToPayloads call.
func (e *CodecDataConverter) ToPayloads(value ...interface{}) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// FromPayload implements DataConverter.FromPayload performing decoding on the
// given payload before sending to the parent FromPayload.
func (e *CodecDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// FromPayloads implements DataConverter.FromPayloads performing decoding on the
// given payloads before sending to the parent FromPayloads.
func (e *CodecDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// ToString implements DataConverter.ToString performing decoding on the given
// payload before sending to the parent ToString.
func (e *CodecDataConverter) ToString(payload *commonpb.Payload) string {
	_ = "STUB: not implemented"
	return ""
}

// ToStrings implements DataConverter.ToStrings using ToString for each value.
func (e *CodecDataConverter) ToStrings(payloads *commonpb.Payloads) []string {
	_ = "STUB: not implemented"
	return nil
}

// Perform decoding one by one here so that we return individual errors

func (e *CodecDataConverter) WithSerializationContext(ctx SerializationContext) DataConverter {
	_ = "STUB: not implemented"
	return *new(DataConverter)
}

const remotePayloadCodecEncodePath = "/encode"
const remotePayloadCodecDecodePath = "/decode"

type codecHTTPHandler struct {
	codecs []PayloadCodec
}

func (e *codecHTTPHandler) encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (e *codecHTTPHandler) decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ServeHTTP implements the http.Handler interface.
func (e *codecHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = "STUB: not implemented"
	return
}

// NewPayloadCodecHTTPHandler creates a http.Handler for a PayloadCodec.
// This can be used to provide a remote data converter.
func NewPayloadCodecHTTPHandler(e ...PayloadCodec) http.Handler {
	_ = "STUB: not implemented"
	return *new(http.Handler)
}

// RemotePayloadCodecOptions are options for RemotePayloadCodec.
// Client is optional.
type RemotePayloadCodecOptions struct {
	Endpoint      string
	ModifyRequest func(*http.Request) error
	Client        http.Client
}

type remotePayloadCodec struct {
	options RemotePayloadCodecOptions
}

// NewRemotePayloadCodec creates a PayloadCodec using the remote endpoint configured by RemotePayloadCodecOptions.
func NewRemotePayloadCodec(options RemotePayloadCodecOptions) PayloadCodec {
	_ = "STUB: not implemented"
	return *new(PayloadCodec)
}

// Encode uses the remote payload codec endpoint to encode payloads.
func (pc *remotePayloadCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Decode uses the remote payload codec endpoint to decode payloads.
func (pc *remotePayloadCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (pc *remotePayloadCodec) encodeOrDecode(endpoint string, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Fields Endpoint, ModifyRequest, Client of RemotePayloadCodecOptions are also
// exposed here in RemoteDataConverterOptions for backwards compatibility.

// RemoteDataConverterOptions are options for NewRemoteDataConverter.
type RemoteDataConverterOptions struct {
	Endpoint      string
	ModifyRequest func(*http.Request) error
	Client        http.Client
}

type remoteDataConverter struct {
	parent       DataConverter
	payloadCodec PayloadCodec
}

// NewRemoteDataConverter wraps the given parent DataConverter and performs
// encoding/decoding on the payload via the remote endpoint.
func NewRemoteDataConverter(parent DataConverter, options RemoteDataConverterOptions) DataConverter {
	_ = "STUB: not implemented"
	return *new(DataConverter)
}

// ToPayload implements DataConverter.ToPayload performing remote encoding on the
// result of the parent's ToPayload call.
func (rdc *remoteDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// ToPayloads implements DataConverter.ToPayloads performing remote encoding on the
// result of the parent's ToPayloads call.
func (rdc *remoteDataConverter) ToPayloads(value ...interface{}) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// FromPayload implements DataConverter.FromPayload performing remote decoding on the
// given payload before sending to the parent FromPayload.
func (rdc *remoteDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// FromPayloads implements DataConverter.FromPayloads performing remote decoding on the
// given payloads before sending to the parent FromPayloads.
func (rdc *remoteDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// ToString implements DataConverter.ToString performing remote decoding on the given
// payload before sending to the parent ToString.
func (rdc *remoteDataConverter) ToString(payload *commonpb.Payload) string {
	_ = "STUB: not implemented"
	return ""
}

// ToStrings implements DataConverter.ToStrings using ToString for each value.
func (rdc *remoteDataConverter) ToStrings(payloads *commonpb.Payloads) []string {
	_ = "STUB: not implemented"
	return nil
}

// Perform decoding one by one here so that we return individual errors
