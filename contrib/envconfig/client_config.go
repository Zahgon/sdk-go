// Package envconfig contains utilities to load configuration from files and/or environment variables.
package envconfig

import (
	"context"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

// ClientConfig represents a client config file.
type ClientConfig struct {
	// Profiles, keyed by profile name.
	Profiles map[string]*ClientConfigProfile
}

// ClientConfigProfile is profile-level configuration for a client.
type ClientConfigProfile struct {
	// Client address.
	Address string
	// Client namespace.
	Namespace string
	// Client API key. If present and TLS field is nil or present but without Disabled as true, TLS is defaulted to
	// enabled.
	APIKey string
	// Optional client TLS config.
	TLS *ClientConfigTLS
	// Optional client codec config.
	Codec *ClientConfigCodec
	// Client gRPC metadata (aka headers). When loading from TOML and env var, or writing to TOML, the keys are
	// lowercased and hyphens are replaced with underscores. This is used for deduplicating/overriding too, so manually
	// set values that are not normalized may not get overridden with [ClientConfigProfile.ApplyEnvVars].
	GRPCMeta map[string]string
}

// ClientConfigTLS is TLS configuration for a client.
type ClientConfigTLS struct {
	// If true, TLS is explicitly disabled. If false/unset, whether TLS is enabled or not depends on other factors such
	// as whether this struct is present or nil, and whether API key exists (which enables TLS by default).
	Disabled bool
	// Path to client mTLS certificate. Mutually exclusive with ClientCertData.
	ClientCertPath string
	// PEM bytes for client mTLS certificate. Mutually exclusive with ClientCertPath.
	ClientCertData []byte
	// Path to client mTLS key. Mutually exclusive with ClientKeyData.
	ClientKeyPath string
	// PEM bytes for client mTLS key. Mutually exclusive with ClientKeyPath.
	ClientKeyData []byte
	// Path to server CA cert override. Mutually exclusive with ServerCACertData.
	ServerCACertPath string
	// PEM bytes for server CA cert override. Mutually exclusive with ServerCACertPath.
	ServerCACertData []byte
	// SNI override.
	ServerName string
	// True if host verification should be skipped.
	DisableHostVerification bool
}

// ClientConfigCodec is codec configuration for a client.
type ClientConfigCodec struct {
	// Remote endpoint for the codec.
	Endpoint string
	// Auth for the codec.
	Auth string
}

// ToClientOptionsRequest are options for [ClientConfig.ToClientOptions] and [ClientConfigProfile.ToClientOptions].
type ToClientOptionsRequest struct {
	// If true and a codec is configured, the data converter of the client will point to the codec remotely. Users
	// should usually not set this and rather configure the codec locally. Users should especially not enable this for
	// clients used by workers since they call the codec repeatedly even during workflow replay.
	IncludeRemoteCodec bool
}

// ToClientOptions converts the given profile to client options that can be used to create an SDK client. Defaults to
// "default" profile if profile is empty string. Will fail if profile not found.
func (c *ClientConfig) ToClientOptions(profile string, options ToClientOptionsRequest) (client.Options, error) {
	_ = "STUB: not implemented"
	return *new(client.Options), nil
}

// ToClientOptions converts this profile to client options that can be used to create an SDK client.
func (c *ClientConfigProfile) ToClientOptions(options ToClientOptionsRequest) (client.Options, error) {
	_ = "STUB: not implemented"
	return *new(client.Options), nil
}

func (c *ClientConfigTLS) applyToConnectionOptions(connOpts *client.ConnectionOptions) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *ClientConfigCodec) toDataConverter(namespace string) (converter.DataConverter, error) {
	_ = "STUB: not implemented"
	return *new(converter.DataConverter), nil
}

// NormalizeGRPCMetaKey converts the given key to lowercase and replaces underscores with hyphens.
func NormalizeGRPCMetaKey(k string) string { _ = "STUB: not implemented"; return "" }

type fixedHeaders map[string]string

func (f fixedHeaders) GetHeaders(context.Context) (map[string]string, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
