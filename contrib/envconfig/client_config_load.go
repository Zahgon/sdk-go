package envconfig

import (
	"go.temporal.io/sdk/client"
)

// MustLoadDefaultClientOptions invokes [LoadDefaultClientOptions] and panics on error.
func MustLoadDefaultClientOptions() client.Options {
	_ = "STUB: not implemented"
	return *new(client.Options)
}

// LoadDefaultClientOptions loads client options using default information from config files and environment variables.
// This just delegates to [LoadClientOptions] with the default options set. See that function and associated options for
// where/how values are loaded.
func LoadDefaultClientOptions() (client.Options, error) {
	_ = "STUB: not implemented"
	return *new(client.Options), nil
}

// LoadClientOptionsRequest are options for [LoadClientOptions].
type LoadClientOptionsRequest struct {
	// Override the file path to use to load the TOML file for config. Defaults to TEMPORAL_CONFIG_FILE environment
	// variable or if that is unset/empty, defaults to [os.UserConfigDir]/temporalio/temporal.toml. If ConfigFileData is
	// set, this cannot be set and no file loading from disk occurs. Ignored if DisableFile is true.
	ConfigFilePath string

	// TOML data to load for config. If set, this overrides any file loading. Cannot be set if ConfigFilePath is set.
	// Ignored if DisableFile is true.
	ConfigFileData []byte

	// Specific profile to use after file is loaded. Defaults to TEMPORAL_PROFILE environment variable or if that is
	// unset/empty, defaults to "default". If either this or the environment variable are set, load will fail if the
	// profile isn't present in the config. Ignored if DisableFile is true.
	ConfigFileProfile string

	// If true, will error if there are unrecognized keys.
	ConfigFileStrict bool

	// If true, will not do any TOML loading from file or data. This and DisableEnv cannot both be true.
	DisableFile bool

	// If true, will not apply environment variables on top of file config for the client options, but
	// TEMPORAL_CONFIG_FILE and TEMPORAL_PROFILE environment variables may still by used to populate defaults in this
	// options structure.
	DisableEnv bool

	// If true and a codec is configured, the data converter of the client will point to the codec remotely. Users
	// should usually not set this and rather configure the codec locally. Users should especially not enable this for
	// clients used by workers since they call the codec repeatedly even during workflow replay.
	IncludeRemoteCodec bool

	// Override the environment variable lookup. If nil, defaults to [EnvLookupOS].
	EnvLookup EnvLookup
}

// LoadClientOptions loads client options from file and then applies environment variable overrides. This will not fail
// if the config file does not exist. This is effectively a shortcut for [LoadClientConfigProfile] +
// [ClientConfigProfile.ToClientOptions]. See [LoadClientOptionsRequest] and [ClientConfigProfile] on how files and
// environment variables are applied.
func LoadClientOptions(options LoadClientOptionsRequest) (client.Options, error) {
	_ = "STUB: not implemented"
	// Load profile
	return *new(client.Options), nil
}

// Convert to client options

// [LoadClientConfigOptions] are options for [LoadClientConfig].
type LoadClientConfigOptions struct {
	// Override the file path to use to load the TOML file for config. Defaults to TEMPORAL_CONFIG_FILE environment
	// variable or if that is unset/empty, defaults to [os.UserConfigDir]/temporalio/temporal.toml. If ConfigFileData is
	// set, this cannot be set and no file loading from disk occurs.
	ConfigFilePath string

	// TOML data to load for config. If set, this overrides any file loading. Cannot be set if ConfigFilePath is set.
	ConfigFileData []byte

	// If true, will error if there are unrecognized keys.
	ConfigFileStrict bool

	// Override the environment variable lookup (only used to determine which config file to load). If nil,
	// defaults to [EnvLookupOS].
	EnvLookup EnvLookup
}

// LoadClientConfig loads the client configuration structure from TOML. Does not load values from environment variables
// (but may use environment variables to get which config file to load). This will not fail if the file does not exist.
// See [ClientConfig.FromTOML] for details on format.
func LoadClientConfig(options LoadClientConfigOptions) (ClientConfig, error) {
	_ = "STUB: not implemented"
	return *

	// Get which bytes to load from TOML
	new(ClientConfig), nil
}

// Get file name which is either set value, env var, or default path

// Unlike env vars for the config values, empty and unset env var
// for config file path are both treated as unset

// Get the default config file path. If it doesn't exist, the file path will be empty.

// Load file, not exist is ok

// Parse data

// LoadClientConfigProfileOptions are options for [LoadClientConfigProfile].
type LoadClientConfigProfileOptions struct {
	// Override the file path to use to load the TOML file for config. Defaults to TEMPORAL_CONFIG_FILE environment
	// variable or if that is unset/empty, defaults to [os.UserConfigDir]/temporalio/temporal.toml. If ConfigFileData is
	// set, this cannot be set and no file loading from disk occurs. Ignored if DisableFile is true.
	ConfigFilePath string

	// TOML data to load for config. If set, this overrides any file loading. Cannot be set if ConfigFilePath is set.
	// Ignored if DisableFile is true.
	ConfigFileData []byte

	// Specific profile to use after file is loaded. Defaults to TEMPORAL_PROFILE environment variable or if that is
	// unset/empty, defaults to "default". If either this or the environment variable are set, load will fail if the
	// profile isn't present in the config. Ignored if DisableFile is true.
	ConfigFileProfile string

	// If true, will error if there are unrecognized keys.
	ConfigFileStrict bool

	// If true, will not do any TOML loading from file or data. This and DisableEnv cannot both be true.
	DisableFile bool

	// If true, will not apply environment variables on top of file config for the client options, but
	// TEMPORAL_CONFIG_FILE and TEMPORAL_PROFILE environment variables may still by used to populate defaults in this
	// options structure.
	DisableEnv bool

	// Override the environment variable lookup. If nil, defaults to [EnvLookupOS].
	EnvLookup EnvLookup
}

// LoadClientConfigProfile loads a specific client config profile from file and then applies environment variable
// overrides. This will not fail if the config file does not exist. This is effectively a shortcut for
// [LoadClientConfig] + [ClientConfigProfile.ApplyEnvVars]. See [LoadClientOptionsRequest] and [ClientConfigProfile] on
// how files and environment variables are applied.
func LoadClientConfigProfile(options LoadClientConfigProfileOptions) (ClientConfigProfile, error) {
	_ = "STUB: not implemented"
	return *new(ClientConfigProfile), nil
}

// If file is enabled, load it and find just the profile

// Load

// Find user-set profile or use the default. Only fail if the profile was set and not found (if unset and not
// found, that's ok).

// Unlike env vars for the config values, empty and unset env var
// for config file path are both treated as unset

// If env is enabled, apply it

// DefaultConfigFileProfile is the default profile used.
const DefaultConfigFileProfile = "default"

// DefaultConfigFilePath is the default config file path used. It is [os.UserConfigDir]/temporalio/temporal.toml.
// If the path does not exist, fallback to an empty file path.
func DefaultConfigFilePath() string { _ = "STUB: not implemented"; return "" }

// EnvLookup abstracts environment variable lookup for [ClientConfigProfile.ApplyEnvVars]. [EnvLookupOS] is the common
// implementation.
type EnvLookup interface {
	// Environ gets all environment variables in the same manner as [os.Environ].
	Environ() []string
	// Getenv gets a single environment variable in the same manner as [os.LookupEnv].
	LookupEnv(string) (string, bool)
}

type envLookupOS struct{}

// EnvLookupOS implements [EnvLookup] for [os].
var EnvLookupOS EnvLookup = envLookupOS{}

func (envLookupOS) Environ() []string { _ = "STUB: not implemented"; return nil }
func (envLookupOS) LookupEnv(key string) (string, bool) {
	_ = "STUB: not implemented"
	return "",

		// ApplyEnvVars overwrites any values in the profile with environment variables if the environment variables are set and
		// non-empty. If env lookup is nil, defaults to [EnvLookupOS]
		false
}

func (c *ClientConfigProfile) ApplyEnvVars(env EnvLookup) error {
	_ = "STUB: not implemented"
	return nil
}

// GRPC meta requires crawling the envs to find

// Keys have to be normalized

// Empty env vars are not the same as unset. Unset will leave the
// meta key unchanged, but empty removes it.

func envVarToBool(val string) (v bool, ok bool) { _ = "STUB: not implemented"; return false, false }
