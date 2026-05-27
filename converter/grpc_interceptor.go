package converter

import (
	"google.golang.org/grpc"
)

// PayloadCodecGRPCClientInterceptorOptions holds interceptor options.
// Currently this is just the list of codecs to use.
type PayloadCodecGRPCClientInterceptorOptions struct {
	Codecs []PayloadCodec
}

// NewPayloadCodecGRPCClientInterceptor returns a GRPC Client Interceptor that will mimic the encoding
// that the SDK system would perform when configured with a matching EncodingDataConverter.
// When combining this with NewFailureGRPCClientInterceptor you should ensure that NewFailureGRPCClientInterceptor is
// before NewPayloadCodecGRPCClientInterceptor in the chain.
//
// Note: This approach does not support use cases that rely on the ContextAware DataConverter interface as
// workflow context is not available at the GRPC level.
func NewPayloadCodecGRPCClientInterceptor(options PayloadCodecGRPCClientInterceptorOptions) (grpc.UnaryClientInterceptor, error) {
	_ = "STUB: not implemented"
	return *new(grpc.UnaryClientInterceptor), nil
}

// NewFailureGRPCClientInterceptorOptions holds interceptor options.
type NewFailureGRPCClientInterceptorOptions struct {
	// DataConverter is optional. If not set the SDK's dataconverter will be used.
	DataConverter DataConverter
	// Whether to Encode attributes. The current implementation requires this be true.
	EncodeCommonAttributes bool
}

// NewFailureGRPCClientInterceptor returns a GRPC Client Interceptor that will mimic the encoding
// that the SDK system would perform when configured with a FailureConverter with the EncodeCommonAttributes option set.
// When combining this with NewPayloadCodecGRPCClientInterceptor you should ensure that NewFailureGRPCClientInterceptor is
// before NewPayloadCodecGRPCClientInterceptor in the chain.
func NewFailureGRPCClientInterceptor(options NewFailureGRPCClientInterceptorOptions) (grpc.UnaryClientInterceptor, error) {
	_ = "STUB: not implemented"
	return *new(grpc.UnaryClientInterceptor), nil
}
