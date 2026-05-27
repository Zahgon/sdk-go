package main

import (
	"log"
	"os/exec"

	_ "github.com/BurntSushi/toml"
	_ "github.com/kisielk/errcheck/errcheck"
	_ "honnef.co/go/tools/staticcheck"
)

func main() {
	if err := newBuilder().run(); err != nil {
		log.Fatal(err)
	}
}

const coverageDir = ".build/coverage"

type builder struct {
	thisDir string
	rootDir string
}

func newBuilder() *builder {
	_ = "STUB: not implemented"

	// Find the root directory from this directory
	return nil
}

func (b *builder) run() error { _ = "STUB: not implemented"; return nil }

func (b *builder) check() error {
	_ = "STUB: not implemented"
	// Run go vet
	return nil
}

// Run errcheck

// Run staticcheck

// Run doclink check

func (b *builder) integrationTest() error {
	_ = "STUB: not implemented"
	// Supports some flags
	return nil
}

// Also accept coverage file as env var

// Create coverage dir if doing coverage

// Start dev server if wanted

// Nexus tests use the HTTP port directly
// SDK tests use arbitrary callback URLs, permit that on the server
// Make Nexus tests faster
// Defaults to false until after OSS 1.28 is released

// Run integration test

// Must run in test dir

func (b *builder) mergeCoverageFiles() error {
	_ = "STUB: not implemented"
	// Only arg should be out file
	return nil
}

// Basically we make a new file with a "mode:" line header, then write all
// lines from all files except their "mode:" lines

func (b *builder) unitTest() error {
	_ = "STUB: not implemented"
	// Supports some flags
	return nil
}

// Find every non ./test-prefixed package that has a test file

// Create coverage dir if doing coverage

// Run unit test for each dir

// Run unit test

// Need to run inside directory

func (b *builder) cmdFromRoot(args ...string) *exec.Cmd { _ = "STUB: not implemented"; return nil }

// Forwards stdout/stderr
func (b *builder) runCmd(cmd *exec.Cmd) error { _ = "STUB: not implemented"; return nil }

func (b *builder) getInstalledTool(modPath string) (string, error) {
	_ = "STUB: not implemented"
	// Install
	return "", nil
}

// Get path to installed
