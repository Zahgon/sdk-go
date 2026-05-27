// Package converter provides an HTTP handler for a Temporal codec server with
// support for external payload storage.

package converter

import (
	"net/http"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/internal/extstore"
)

const (
	downloadPath = "/download"
)

// PayloadHTTPHandlerOptions configures a [NewPayloadHTTPHandler].
//
// NOTE: Experimental
type PayloadHTTPHandlerOptions struct {
	// PostStorageCodecs are codecs applied after external storage from the
	// perspective of payloads going through a encoding transformation. These are
	// typically the codecs that would be configured in the proxy's codec chain.
	// When encoding, the codecs are applied last to first meaning the earlier
	// codecs wrap the later ones. When decoding, the codecs are applied first
	// to last to reverse the effect.
	//
	// NOTE: Experimental.
	PostStorageCodecs []PayloadCodec

	// PreStorageCodecs are codecs that are applied before external storage,
	// from the perspective of payloads going through an encoding transformation.
	// These are typically the codecs that would be configured in the DataConverter
	// codec chain on a Temporal client. When encoding, the codecs are applied last
	// to first meaning the earlier codecs wrap the later ones. When decoding, the
	// codecs are applied first to last to reverse the effect.
	//
	// NOTE: Experimental.
	PreStorageCodecs []PayloadCodec

	// ExternalStorage configures external payload storage, allowing payloads
	// to be stored and retrieved from external sources if they meet the size
	// threshold and driver selection criteria.
	//
	// NOTE: Experimental.
	ExternalStorage ExternalStorage
}

type payloadHTTPHandler struct {
	postStorageCodecs []PayloadCodec
	preStorageCodecs  []PayloadCodec
	retrievalVisitor  extstore.PayloadVisitor
	storageVisitor    extstore.PayloadVisitor
}

var _ http.Handler = (*payloadHTTPHandler)(nil)

// NewPayloadHTTPHandler creates an [http.Handler] that serves /encode, /decode,
// and /download routes for remote payload transformations.
//
// NOTE: Experimental
func NewPayloadHTTPHandler(options PayloadHTTPHandlerOptions) (http.Handler, error) {
	_ = "STUB: not implemented"
	return *new(http.Handler), nil
}

// ServeHTTP implements [http.Handler].
func (h *payloadHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = "STUB: not implemented"
	return
}

// decode decodes payloads through the post-storage then pre-storage codec chains.
// If preserveStorageRefs=true is set in the query string, storage references are
// returned as-is rather than being retrieved from external storage.
func (h *payloadHTTPHandler) decode(r *http.Request, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// download retrieves payloads from external storage and decodes them through
// the pre-storage codec chain. All input payloads must be storage references.
func (h *payloadHTTPHandler) download(r *http.Request, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// encode encodes payloads through the pre-storage then post-storage codec chains,
// applying external storage as configured. Storage references are returned for
// payloads that meet the size threshold and driver selection criteria.
func (h *payloadHTTPHandler) encode(r *http.Request, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// decodeNonReferences decodes non-storage-reference payloads through the given
// codec chain. Storage references pass through as-is.
func decodeNonReferences(payloads []*commonpb.Payload, codecs []PayloadCodec) ([]*commonpb.Payload, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
