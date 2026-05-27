package temporalnexus

type operationTokenType int

const (
	operationTokenTypeWorkflowRun = operationTokenType(1)
)

// workflowRunOperationToken is the decoded form of the workflow run operation token.
type workflowRunOperationToken struct {
	// Version of the token, by default we assume we're on version 1, this field is not emitted as part of the output,
	// it's only used to reject newer token versions on load.
	Version int `json:"v,omitempty"`
	// Type of the operation. Must be operationTypeWorkflowRun.
	Type          operationTokenType `json:"t"`
	NamespaceName string             `json:"ns"`
	WorkflowID    string             `json:"wid"`
}

func generateWorkflowRunOperationToken(namespace, workflowID string) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func loadWorkflowRunOperationToken(data string) (workflowRunOperationToken, error) {
	_ = "STUB: not implemented"
	return *new(workflowRunOperationToken), nil
}
