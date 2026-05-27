package nexusclient

import (
	"context"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
)

const (
	// Nexus specific headers.
	headerOperationState     = "nexus-operation-state"
	headerRequestID          = "nexus-request-id"
	headerLink               = "nexus-link"
	headerOperationStartTime = "nexus-operation-start-time"
	headerOperationCloseTime = "nexus-operation-close-time"
	headerRetryable          = "nexus-request-retryable"
)

// Query param for passing a callback URL.
const queryCallbackURL = "callback"

// HTTP status code for failed operation responses.
const statusOperationFailed = http.StatusFailedDependency

func isMediaTypeJSON(contentType string) bool { _ = "STUB: not implemented"; return false }

func prefixStrippedHTTPHeaderToNexusHeader(httpHeader http.Header, prefix string) nexus.Header {
	_ = "STUB: not implemented"
	return *new(nexus.Header)
}

// Nexus headers can only have single values, ignore multiple values.

func addContentHeaderToHTTPHeader(nexusHeader nexus.Header, httpHeader http.Header) http.Header {
	_ = "STUB: not implemented"
	return *new(http.Header)
}

func addCallbackHeaderToHTTPHeader(nexusHeader nexus.Header, httpHeader http.Header) http.Header {
	_ = "STUB: not implemented"
	return *new(http.Header)
}

func addLinksToHTTPHeader(links []nexus.Link, httpHeader http.Header) error {
	_ = "STUB: not implemented"
	return nil
}

func getLinksFromHeader(httpHeader http.Header) ([]nexus.Link, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func addNexusHeaderToHTTPHeader(nexusHeader nexus.Header, httpHeader http.Header) http.Header {
	_ = "STUB: not implemented"
	return *new(http.Header)
}

func addContextTimeoutToHTTPHeader(ctx context.Context, httpHeader http.Header) http.Header {
	_ = "STUB: not implemented"
	return *new(http.Header)
}

const linkTypeKey = "type"

// decodeLink encodes the link to Nexus-Link header value.
// It follows the same format of HTTP Link header: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Link
func encodeLink(link nexus.Link) (string, error) { _ = "STUB: not implemented"; return "", nil }

// decodeLink decodes the Nexus-Link header values.
// It must have the same format of HTTP Link header: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Link
func decodeLink(encodedLink string) (nexus.Link, error) {
	_ = "STUB: not implemented"
	return *new(nexus.Link), nil
}

// must contain at least one semi-colon, and first param must be empty since
// it corresponds to the url part parsed above.

func validateLinkURL(value *url.URL) error { _ = "STUB: not implemented"; return nil }

func validateLinkType(value string) error { _ = "STUB: not implemented"; return nil }

var durationRegexp = regexp.MustCompile(`^(\d+(?:\.\d+)?)(ms|s|m)$`)

func parseDuration(value string) (time.Duration, error) {
	_ = "STUB: not implemented"
	return *new(time.Duration), nil
}

// nolint:forbidigo // code is unreachable due to regex validation

// formatDuration converts a duration into a string representation in millisecond resolution.
func formatDuration(d time.Duration) string { _ = "STUB: not implemented"; return "" }
