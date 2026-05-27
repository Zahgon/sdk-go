package lambdaworker

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// workerDeps captures external dependencies for testability.
type workerDeps struct {
	dial             func(client.Options) (client.Client, error)
	newWorker        func(client.Client, string, worker.Options) worker.Worker
	startLambda      func(handler any, options ...lambda.Option)
	loadConfig       func() (client.Options, error)
	getenv           func(string) string
	setCacheSize     func(int)
	exit             func(int)
	extractLambdaCtx func(context.Context) (requestID string, functionARN string, ok bool)
}

func defaultDeps() workerDeps { _ = "STUB: not implemented"; return *new(workerDeps) }

// RunWorker starts a Temporal worker inside an AWS Lambda execution environment. It calls the
// configure callback to collect registrations and option overrides, then delegates to the Lambda
// runtime. On each invocation, it dials the Temporal server, starts a worker, polls for tasks
// until the invocation deadline approaches, and then gracefully shuts down the worker and closes
// the client. RunWorker does not return under normal operation.
//
// The version parameter identifies this worker's deployment version. RunWorker always enables
// Worker Deployment Versioning ([worker.DeploymentOptions.UseVersioning] = true). To provide a
// default versioning behavior for workflows that do not specify one at registration time, set
// [worker.DeploymentOptions.DefaultVersioningBehavior] on [Options.WorkerOptions] in the configure
// callback.
//
// You must configure a task queue for the worker to listen on, either via [Options.TaskQueue] or
// the TEMPORAL_TASK_QUEUE environment variable. You must also register one or more Workflows,
// Activities, or Nexus Services by using the registration methods provided by [Options].
//
// On fatal configuration error, it logs to stderr and calls os.Exit(1).
func RunWorker(version worker.WorkerDeploymentVersion, configure func(ctx *Options) error) {
	_ = "STUB: not implemented"
	return
}

// runWorkerInternal contains the core logic with injected dependencies for testability. It returns
// an error instead of calling os.Exit.
func runWorkerInternal(
	version worker.WorkerDeploymentVersion,
	configure func(ctx *Options) error,
	deps workerDeps,
) error {
	_ = "STUB: not implemented"
	return nil
}

// Build per-invocation client options with identity from this invocation's Lambda context.
// A shallow copy is sufficient since only Identity is modified.

// Stop the worker before running shutdown hooks so that hooks (e.g. OTLP telemetry flushes)
// see all spans/metrics emitted during the drain phase.

// Use context.Background because invocationCtx may already be cancelled. No timeout is
// needed — Lambda hard-kills the process at the deadline regardless.
