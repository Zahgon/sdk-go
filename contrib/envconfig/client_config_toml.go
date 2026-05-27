package envconfig

// knownProfileKeys contains the TOML keys recognized for profile configuration.
// This must be kept in sync with tomlClientConfigProfile's TOML tags.
// See TestKnownProfileKeysInSync for validation.
var knownProfileKeys = map[string]bool{
	"address":   true,
	"namespace": true,
	"api_key":   true,
	"tls":       true,
	"codec":     true,
	"grpc_meta": true,
}

// ClientConfigToTOMLOptions are options for [ClientConfig.ToTOML].
type ClientConfigToTOMLOptions struct {
	// Defaults to two-space indent.
	OverrideIndent *string
	// If non-nil, these additional fields will be serialized with each profile.
	// Key is profile name, value is map of field name to field value.
	AdditionalProfileFields map[string]map[string]any
}

// ToTOML converts the client config to TOML. Note, this may not be byte-for-byte exactly what may have been set in
// [ClientConfig.FromTOML].
func (c *ClientConfig) ToTOML(options ClientConfigToTOMLOptions) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Encode to TOML then decode to map for merging additional fields

// Merge additional fields into profiles

// Re-encode with merged data

type ClientConfigFromTOMLOptions struct {
	// If true, will error if there are unrecognized keys.
	Strict bool
	// If non-nil, populated with additional (unrecognized) profile fields.
	// Key is profile name, value is map of field name to field value.
	// This allows callers to preserve custom profile fields without modifying
	// this package. Note, if Strict is true the additional fields will cause an
	// error before they can be captured here.
	AdditionalProfileFields map[string]map[string]any
}

// FromTOML converts from TOML to the client config. This will replace all profiles within, it does not do any form of
// merging.
func (c *ClientConfig) FromTOML(b []byte, options ClientConfigFromTOMLOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// If AdditionalProfileFields is requested, extract unknown profile fields.

// Decode again into raw map to get additional field values

// Skip non-profile undecoded keys (e.g., unknown top-level sections)

type tomlClientConfig struct {
	Profiles map[string]tomlClientConfigProfile `toml:"profile"`
}

func (c *tomlClientConfig) applyToClientConfig(conf *ClientConfig) {
	_ = "STUB: not implemented"
	return
}

func (c *tomlClientConfig) fromClientConfig(conf *ClientConfig) { _ = "STUB: not implemented"; return }

type tomlClientConfigProfile struct {
	Address   string                 `toml:"address,omitempty"`
	Namespace string                 `toml:"namespace,omitempty"`
	APIKey    string                 `toml:"api_key,omitempty"`
	TLS       *tomlClientConfigTLS   `toml:"tls,omitempty"`
	Codec     *tomlClientConfigCodec `toml:"codec,omitempty"`
	GRPCMeta  map[string]string      `toml:"grpc_meta,omitempty"`
}

func (c *tomlClientConfigProfile) toClientConfig() *ClientConfigProfile {
	_ = "STUB: not implemented"
	return nil
}

// gRPC meta keys have to be normalized

func (c *tomlClientConfigProfile) fromClientConfig(conf *ClientConfigProfile) {
	_ = "STUB: not implemented"
	return
}

// gRPC meta keys have to be normalized (we can mutate receiver, it's only used ephemerally)

type tomlClientConfigTLS struct {
	Disabled                bool   `toml:"disabled,omitempty"`
	ClientCertPath          string `toml:"client_cert_path,omitempty"`
	ClientCertData          string `toml:"client_cert_data,omitempty"`
	ClientKeyPath           string `toml:"client_key_path,omitempty"`
	ClientKeyData           string `toml:"client_key_data,omitempty"`
	ServerCACertPath        string `toml:"server_ca_cert_path,omitempty"`
	ServerCACertData        string `toml:"server_ca_cert_data,omitempty"`
	ServerName              string `toml:"server_name,omitempty"`
	DisableHostVerification bool   `toml:"disable_host_verification,omitempty"`
}

func (c *tomlClientConfigTLS) toClientConfig() *ClientConfigTLS {
	_ = "STUB: not implemented"
	return nil
}

// For deep equality, we want empty strings as nil byte slices, not empty byte slices

func (c *tomlClientConfigTLS) fromClientConfig(conf *ClientConfigTLS) {
	_ = "STUB: not implemented"
	return
}

type tomlClientConfigCodec struct {
	Endpoint string `toml:"endpoint,omitempty"`
	Auth     string `toml:"auth,omitempty"`
}

func (c *tomlClientConfigCodec) toClientConfig() *ClientConfigCodec {
	_ = "STUB: not implemented"
	return nil
}

func (c *tomlClientConfigCodec) fromClientConfig(conf *ClientConfigCodec) {
	_ = "STUB: not implemented"
	return
}
