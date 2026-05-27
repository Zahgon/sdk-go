// This file is duplicated in temporalio/temporal/components/nexusoperations/link_converter.go
// Any changes here or there must be replicated. This is temporary until the
// temporal repo updates to the most recent SDK version.

package temporalnexus

import (
	"fmt"
	"net/url"
	"regexp"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
)

const (
	urlSchemeTemporalKey = "temporal"
	urlPathNamespaceKey  = "namespace"
	urlPathWorkflowIDKey = "workflowID"
	urlPathRunIDKey      = "runID"
	urlPathTemplate      = "/namespaces/%s/workflows/%s/%s/history"

	linkWorkflowEventReferenceTypeKey = "referenceType"
	linkEventIDKey                    = "eventID"
	linkEventTypeKey                  = "eventType"
	linkRequestIDKey                  = "requestID"
)

var (
	rePatternNamespace  = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathNamespaceKey)
	rePatternWorkflowID = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathWorkflowIDKey)
	rePatternRunID      = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathRunIDKey)
	urlPathRE           = regexp.MustCompile(fmt.Sprintf(
		`^/namespaces/%s/workflows/%s/%s/history$`,
		rePatternNamespace,
		rePatternWorkflowID,
		rePatternRunID,
	))
	eventReferenceType     = string((&commonpb.Link_WorkflowEvent_EventReference{}).ProtoReflect().Descriptor().Name())
	requestIDReferenceType = string((&commonpb.Link_WorkflowEvent_RequestIdReference{}).ProtoReflect().Descriptor().Name())
)

// ConvertLinkWorkflowEventToNexusLink converts a Link_WorkflowEvent type to Nexus Link.
//
// NOTE: Experimental
func ConvertLinkWorkflowEventToNexusLink(we *commonpb.Link_WorkflowEvent) nexus.Link {
	_ = "STUB: not implemented"
	return *new(nexus.Link)
}

// ConvertNexusLinkToLinkWorkflowEvent converts a Nexus Link to Link_WorkflowEvent.
//
// NOTE: Experimental
func ConvertNexusLinkToLinkWorkflowEvent(link nexus.Link) (*commonpb.Link_WorkflowEvent, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func convertLinkWorkflowEventEventReferenceToURLQuery(eventRef *commonpb.Link_WorkflowEvent_EventReference) string {
	_ = "STUB: not implemented"
	return ""
}

func convertURLQueryToLinkWorkflowEventEventReference(queryValues url.Values) (*commonpb.Link_WorkflowEvent_EventReference, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func convertLinkWorkflowEventRequestIdReferenceToURLQuery(requestIDRef *commonpb.Link_WorkflowEvent_RequestIdReference) string {
	_ = "STUB: not implemented"
	return ""
}

func convertURLQueryToLinkWorkflowEventRequestIdReference(queryValues url.Values) (*commonpb.Link_WorkflowEvent_RequestIdReference, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
