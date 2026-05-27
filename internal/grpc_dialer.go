package internal

import (
	"context"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
)

type (
	// dialParameters are passed to GRPCDialer and must be used to create gRPC connection.
	dialParameters struct {
		HostPort              string
		UserConnectionOptions ConnectionOptions
		RequiredInterceptors  []grpc.UnaryClientInterceptor
		DefaultServiceConfig  string
	}
)

const (
	// LocalHostPort is a default host:port for worker and client to connect to.
	//
	// Exposed as: [go.temporal.io/sdk/client.DefaultHostPort]
	LocalHostPort = "localhost:7233"

	// defaultServiceConfig is a default gRPC connection service config which enables DNS round-robin between IPs.
	defaultServiceConfig = `{"loadBalancingConfig": [{"round_robin":{}}]}`

	// minConnectTimeout is the minimum amount of time we are willing to give a connection to complete.
	minConnectTimeout = 20 * time.Second

	// attemptSuffix is a suffix added to the metric name for individual call attempts made to the server, which includes retries.
	attemptSuffix = "_attempt"

	// mb is a number of bytes in a megabyte
	mb = 1024 * 1024

	// defaultMaxPayloadSize is a maximum size of the payload that grpc client would allow.
	defaultMaxPayloadSize = 128 * mb

	// defaultKeepAliveTime is the keep alive time if one is not specified.
	defaultKeepAliveTime = 30 * time.Second

	// defaultKeepAliveTimeout is the keep alive timeout if one is not specified.
	defaultKeepAliveTimeout = 15 * time.Second

	// temporalNamespaceHeaderKey is the header key that should contain the target namespace of the request.
	temporalNamespaceHeaderKey = "temporal-namespace"
)

func dial(params dialParameters) (*grpc.ClientConn, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// gRPC maintains connection pool inside grpc.ClientConn.
// This connection pool has auto reconnect feature.
// If connection goes down, gRPC will try to reconnect using exponential backoff strategy:
// https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md.
// Default MaxDelay is 120 seconds which is too high.
// Setting it to retryPollOperationMaxInterval here will correlate with poll reconnect interval.

// gRPC utilizes keep alive mechanism to detect dead connections in case if server didn't close them
// gracefully. Client would ping the server periodically and expect replies withing the specified timeout.
// Learn more by reading https://github.com/grpc/grpc/blob/master/doc/keepalive.md

// Append any user-supplied options

func requiredInterceptors(
	clientOptions *ClientOptions,
	excludeInternalFromRetry *atomic.Bool,
) []grpc.UnaryClientInterceptor {
	_ = "STUB: not implemented"
	return nil
}

// Report aggregated metrics for the call, this is done outside of the retry loop.

// By default the grpc retry interceptor *is disabled*, preventing accidental use of retries.
// We add call options for retry configuration based on the values present in the context.

// Performs retries *IF* retry options are set for the call.

// Prevents retrying grpc message too large errors, while allowing retries of other resource exhausted errors.

// Report metrics for every call made to the server.

// Add credentials interceptor. This is intentionally added after headers
// provider to overwrite anything set there.

// Add namespace provider interceptor

func namespaceProviderInterceptor() grpc.UnaryClientInterceptor {
	_ = "STUB: not implemented"
	return *new(grpc.UnaryClientInterceptor)
}

// Only add namespace if it doesn't already exist

func trafficControllerInterceptor(controller TrafficController) grpc.UnaryClientInterceptor {
	_ = "STUB: not implemented"
	return *new(grpc.UnaryClientInterceptor)
}

// Break execution chain and return an error without sending actual request to the server.

func headersProviderInterceptor(headersProvider HeadersProvider) grpc.UnaryClientInterceptor {
	_ = "STUB: not implemented"
	return *new(grpc.UnaryClientInterceptor)
}

func errorInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	_ = "STUB: not implemented"
	return nil
}
