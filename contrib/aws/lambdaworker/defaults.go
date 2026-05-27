package lambdaworker

import (
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

const (
	defaultMaxConcurrentActivityExecutionSize      = 2
	defaultMaxConcurrentWorkflowTaskExecutionSize  = 10
	defaultMaxConcurrentLocalActivityExecutionSize = 2
	defaultMaxConcurrentNexusTaskExecutionSize     = 5
	defaultMaxConcurrentActivityTaskPollers        = 1
	defaultMaxConcurrentWorkflowTaskPollers        = 2
	defaultMaxConcurrentNexusTaskPollers           = 1
	defaultWorkerStopTimeout                       = 5 * time.Second
	defaultShutdownHookBuffer                      = 2 * time.Second
	defaultStickyCacheSize                         = 100

	envTaskQueue      = "TEMPORAL_TASK_QUEUE"
	envLambdaTaskRoot = "LAMBDA_TASK_ROOT"
	envConfigFile     = "TEMPORAL_CONFIG_FILE"
	defaultConfigFile = "temporal.toml"
)

// applyLambdaWorkerDefaults sets Lambda-appropriate defaults on the given worker options.
// Zero-valued fields are set to Lambda defaults; non-zero fields (previously set by envconfig or
// user) are left alone.
func applyLambdaWorkerDefaults(opts *worker.Options) { _ = "STUB: not implemented"; return }

// applyLambdaClientDefaults sets Lambda-appropriate defaults on the given client options that are
// available during the init phase (before any invocation). Identity is set later from the Lambda
// invocation context via [buildLambdaIdentity].
func applyLambdaClientDefaults(opts *client.Options) { _ = "STUB: not implemented"; return }

// buildLambdaIdentity constructs an identity string in the form "<requestID>@<functionARN>" from
// the Lambda invocation context.
func buildLambdaIdentity(requestID, functionARN string) string {
	_ = "STUB: not implemented"
	return ""
}

// lambdaDefaultConfigFilePath returns the config file path to use in a Lambda environment. It
// respects TEMPORAL_CONFIG_FILE if set, otherwise defaults to temporal.toml in the Lambda code root
// (LAMBDA_TASK_ROOT). If LAMBDA_TASK_ROOT is not set, it falls back to temporal.toml in the current
// working directory.
func lambdaDefaultConfigFilePath(getenv func(string) string) string {
	_ = "STUB: not implemented"
	return ""
}
