package extstore

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/proxy"
	sdkpb "go.temporal.io/api/sdk/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const defaultPayloadSizeThreshold = 256 * 1024

// StorageParameters holds the validated, ready-to-use storage configuration
// built from a [ExternalStorage] value via [ExternalStorageToParams].
type StorageParameters struct {
	driverMap            map[string]StorageDriver
	driverSelector       StorageDriverSelector
	payloadSizeThreshold int
}

// IsStorageReference reports whether p is an external-storage reference payload.
// It recognizes both the current protojson format (encoding=json/protobuf,
// messageType=temporal.api.sdk.v1.ExternalStorageReference) and the legacy
// format (encoding=json/external-storage-reference) written by earlier releases.
func IsStorageReference(p *commonpb.Payload) bool { _ = "STUB: not implemented"; return false }

func ExternalStorageToParams(options ExternalStorage) (StorageParameters, error) {
	_ = "STUB: not implemented"
	return *new(StorageParameters), nil
}

// singleDriverSelector is a StorageDriverSelector that always returns the same driver.
type singleDriverSelector struct {
	driver StorageDriver
}

func (s singleDriverSelector) SelectDriver(_ StorageDriverStoreContext, _ *commonpb.Payload) (StorageDriver, error) {
	_ = "STUB: not implemented"
	return *

	// driversEqual compares two StorageDriver interface values. It uses == when
	// the dynamic type is comparable (pointer types, simple value types) and
	// falls back to name equality for non-comparable value types (e.g. structs
	// with map fields).
	new(StorageDriver), nil
}

func driversEqual(a, b StorageDriver) (equal bool) { _ = "STUB: not implemented"; return false }

type StorageOperationCallback interface {
	PayloadBatchCompleted(count int, size int64, duration time.Duration, driverNames []string)
}

type contextKey string

const storageOperationCallbackContextKey contextKey = "storageOperationCallback"

func WithStorageOperationCallback(ctx context.Context, cb StorageOperationCallback) context.Context {
	_ = "STUB: not implemented"
	return *new(context.Context)
}

const storageTargetContextKey contextKey = "storageTarget"

func WithStorageTarget(ctx context.Context, target StorageDriverTargetInfo) context.Context {
	_ = "STUB: not implemented"
	return *new(context.Context)
}

func StorageTargetFromContext(ctx context.Context) StorageDriverTargetInfo {
	_ = "STUB: not implemented"
	return *new(StorageDriverTargetInfo)
}

// metadataEncoding is the key used in payload metadata to identify the encoding
// format. Mirrors converter.MetadataEncoding without importing converter package.
const metadataEncoding = "encoding"

// metadataMessageType is the key used in payload metadata to identify the proto
// message type. Mirrors converter.MetadataMessageType without importing converter package.
const metadataMessageType = "messageType"

// metadataEncodingProtoJSON is the standard protojson encoding value, shared with
// ProtoJSONPayloadConverter. Mirrors converter.MetadataEncodingProtoJSON.
const metadataEncodingProtoJSON = "json/protobuf"

// metadataEncodingStorageRefLegacy is the encoding written by earlier prerelease
// SDK versions. Retained solely for backward-compatible reads.
const metadataEncodingStorageRefLegacy = "json/external-storage-reference"

// legacyStorageReference is the old wire format retained for backward compatibility
// with payloads written by earlier prerelease SDK versions.
type legacyStorageReference struct {
	DriverName  string             `json:"driver_name"`
	DriverClaim StorageDriverClaim `json:"driver_claim"`
}

var (
	// compile-time assertion that ExternalStorageReference implements proto.Message.
	_ proto.Message = (*sdkpb.ExternalStorageReference)(nil)

	// externalStorageReferenceMessageType is the fully-qualified proto message name,
	// derived from the descriptor so it stays in sync with the generated code.
	externalStorageReferenceMessageType = string((*sdkpb.ExternalStorageReference)(nil).ProtoReflect().Descriptor().FullName())

	protoMarshalOptions   = protojson.MarshalOptions{}
	protoUnmarshalOptions = protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}
)

func storageReferenceToPayload(ref *sdkpb.ExternalStorageReference, storedSizeBytes int64) (*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// payloadToStorageReference decodes a storage reference from a payload.
// The current format uses encoding=json/protobuf with the ExternalStorageReference
// message type. The legacy format uses encoding=json/external-storage-reference.
func payloadToStorageReference(p *commonpb.Payload) (*sdkpb.ExternalStorageReference, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

type externalRetrievalVisitor struct {
	params StorageParameters
}

func (v *externalRetrievalVisitor) Visit(ctx *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil,

		// Identify which payloads are storage references and group them by driver.
		nil
}

// No storage drivers configured at all — fail immediately with a clear error
// rather than passing through an unresolved reference.

// Fan out to each driver concurrently. The errgroup context is used as the
// StorageDriverRetrieveContext so a failing driver cancels in-flight siblings.
// Intentionally creating an empty context so the retrieval path cannot use ambient
// information for determing how to retrieve payloads. Drivers should only use information
// from the StorageDriverClaim to retrieve payloads.

func NewExternalRetrievalVisitor(params StorageParameters) PayloadVisitor {
	_ = "STUB: not implemented"
	return *new(PayloadVisitor)
}

type externalStorageVisitor struct {
	params StorageParameters
}

func (v *externalStorageVisitor) Visit(ctx *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Determine which driver (if any) should store each payload.

// Fan out to each driver concurrently. The errgroup context is used as the
// StorageDriverStoreContext so a failing driver cancels in-flight siblings.

func NewExternalStorageVisitor(params StorageParameters) PayloadVisitor {
	_ = "STUB: not implemented"
	return *new(PayloadVisitor)
}

func callDriverSelector(s StorageDriverSelector, ctx StorageDriverStoreContext, p *commonpb.Payload) (driver StorageDriver, err error) {
	_ = "STUB: not implemented"
	return *new(StorageDriver), nil
}

func callDriverStore(d StorageDriver, ctx StorageDriverStoreContext, payloads []*commonpb.Payload) (claims []StorageDriverClaim, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func callDriverRetrieve(d StorageDriver, ctx StorageDriverRetrieveContext, claims []StorageDriverClaim) (payloads []*commonpb.Payload, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}
