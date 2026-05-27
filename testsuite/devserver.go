package testsuite

import (
	"context"
	"io"
	"os/exec"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
)

// CachedDownload is the cached download configuration for the dev server binary.
type CachedDownload struct {
	// Which version to download, by default the latest version compatible with the SDK will be downloaded.
	// Acceptable values are specific release versions (e.g v0.3.0), "default", and "latest".
	Version string
	// Destination directory or the user temp directory if unset.
	DestDir string
}

// DevServerOptions configures the dev server process.
type DevServerOptions struct {
	// Existing path on the filesystem for the executable.
	ExistingPath string
	// Download the executable if not already there.
	CachedDownload CachedDownload
	// Client options used to create a client for the dev server.
	// The provided Namespace or the "default" namespace is automatically registered on startup.
	// If HostPort is provided, the host and port will be used to bind the server, otherwise the server will bind to
	// localhost and obtain a free port.
	ClientOptions *client.Options
	// SQLite DB filename if persisting or non-persistent if none.
	DBFilename string
	// Whether to enable the UI.
	EnableUI bool
	// Override UI port if EnableUI is true.
	// If not provided, a free port will be used.
	UIPort string
	// Log format - defaults to "pretty".
	LogFormat string
	// Log level - defaults to "warn".
	LogLevel string
	// Search Attributes to register with the dev server.
	SearchAttributes temporal.SearchAttributes
	// Additional arguments to the dev server.
	ExtraArgs []string
	// Where to redirect stdout and stderr, if nil they will be redirected to the current process.
	Stdout io.Writer
	Stderr io.Writer
}

// DevServer is a Temporal CLI-based dev server process.
type DevServer struct {
	cmd              *exec.Cmd
	client           client.Client
	frontendHostPort string
}

// StartDevServer starts a Temporal CLI dev server process. This may download the server if not already downloaded.
func StartDevServer(ctx context.Context, options DevServerOptions) (*DevServer, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Make sure this is done after downloading to reduce the chance (however slim) that the free port would be used
// up by the time the download completes.

func prepareCommand(options *DevServerOptions, host, port, namespace string) []string {
	_ = "STUB: not implemented"
	return nil
}

func downloadIfNeeded(ctx context.Context, options *DevServerOptions, logger log.Logger) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

// Build path based on version and check if already present

// Build info URL

// Get info

// Download and extract

// We want to download to a temporary file then rename. A better system-wide
// atomic downloader would use a common temp file and check whether it exists
// and wait on it, but doing multiple downloads in racy situations is
// good/simple enough for now.
// Note that we don't use os.TempDir here, instead we use the user provided destination directory which is
// guaranteed to make the rename atomic.

// Chmod it if not Windows

func (opts *DevServerOptions) clientOptionsOrDefault() client.Options {
	_ = "STUB: not implemented"
	return *new(client.Options)
}

// Shallow copy the client options since we intend to overwrite some fields.

func extractTarball(r io.Reader, toExtract string, w io.Writer) error {
	_ = "STUB: not implemented"
	return nil
}

// This can be EOF which means we never found our file

func extractZip(r io.Reader, toExtract string, w io.Writer) error {
	_ = "STUB: not implemented"
	// Instead of using a third party zip streamer, and since Go stdlib doesn't
	// support streaming read, we'll just put the entire archive in memory for now
	return nil
}

// waitServerReady repeatedly attempts to dial the server with given options until it is ready or it is time to give up.
// Returns a connected client created using the provided options.
func waitServerReady(ctx context.Context, options client.Options) (client.Client, error) {
	_ = "STUB: not implemented"
	return *new(client.Client), nil
}

// retryFor retries some function until it returns nil or runs out of attempts. Wait interval between attempts.
func retryFor(ctx context.Context, maxAttempts int, interval time.Duration, cond func() error) error {
	_ = "STUB: not implemented"
	return nil

	// this is used internally, okay to panic
}

// Try again after waiting up to interval.

// Stop the running server and wait for shutdown to complete. Error is propagated from server shutdown.
func (s *DevServer) Stop() error { _ = "STUB: not implemented"; return nil }

// Get a connected client, configured to work with the dev server.
func (s *DevServer) Client() client.Client {
	_ = "STUB: not implemented"

	// FrontendHostPort returns the host:port for this server.
	return *new(client.Client)
}

func (s *DevServer) FrontendHostPort() string { _ = "STUB: not implemented"; return "" }
