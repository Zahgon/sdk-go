package workflow

import (
	"go/ast"

	"go.temporal.io/sdk/contrib/tools/workflowcheck/determinism"
	"golang.org/x/tools/go/analysis"
)

// DefaultIdentRefs are additional overrides of determinism.DefaultIdentRefs for
// safe Temporal library functions.
var DefaultIdentRefs = determinism.DefaultIdentRefs.Clone().SetAll(determinism.IdentRefs{
	// Reported as non-deterministic because it internally starts a goroutine, so
	// mark deterministic explicitly
	"go.temporal.io/sdk/internal.propagateCancel": false,
	// Reported as non-deterministic because it iterates over a map, so mark
	// deterministic explicitly
	"(*go.temporal.io/sdk/internal.cancelCtx).cancel": false,
	// Reported as non-deterministic because it iterates over a map, just takes
	// the size of the map, so mark deterministic explicitly
	"(go.temporal.io/sdk/internal.SearchAttributes).Size": false,
	// Reported as non-deterministic because it iterates over a map, result is sorted
	// so mark deterministic explicitly
	"go.temporal.io/sdk/internal.DeterministicKeys": false,
	// Reported as non-deterministic because it iterates over a map, result is sorted
	// so mark deterministic explicitly
	"go.temporal.io/sdk/internal.DeterministicKeysFunc": false,
})

// Config is config for NewChecker.
type Config struct {
	// If empty, uses DefaultIdentRefs.
	IdentRefs determinism.IdentRefs
	// If nil, uses log.Printf.
	DebugfFunc func(string, ...interface{})
	// Must be set to true to see advanced debug logs.
	Debug bool
	// Must be set to true to see advanced determinism debug logs.
	DeterminismDebug bool
	// If set, the file and line/col position is present on nested errors.
	IncludePosOnMessage bool
	// If set, the determinism checker will include facts per object
	EnableObjectFacts bool
	// If set, the output uses "->" instead of "\n" as the hierarchy separator.
	SingleLine bool
}

// Checker checks if functions passed RegisterWorkflow are non-deterministic
// based on the results from the checker of the adjacent determinism package.
type Checker struct {
	DebugfFunc          func(string, ...interface{})
	Debug               bool
	IncludePosOnMessage bool
	Determinism         *determinism.Checker
	SingleLine          bool
}

// NewChecker creates a Checker for the given config.
func NewChecker(config Config) *Checker {
	_ = "STUB: not implemented"
	// Set default refs but we don't have to clone since the determinism
	// constructor will do that
	return nil
}

// Default debug

// Build checker

func (c *Checker) debugf(f string, v ...interface{}) { _ = "STUB: not implemented"; return }

// NewAnalyzer creates a Go analysis analyzer that can be used in existing
// tools. There is a -config flag for setting configuration, a -workflow-debug
// flag for enabling debug logs, a -determinism-debug flag for enabling
// determinism debug logs, and a -show-pos flag for showing position on nested
// errors. This analyzer does not have any results but does set the same
// facts as the determinism analyzer (*determinism.NonDeterminisms).
func (c *Checker) NewAnalyzer() *analysis.Analyzer { _ = "STUB: not implemented"; return nil }

// Set flags

// Run executes this checker for the given pass.
func (c *Checker) Run(pass *analysis.Pass) error { _ = "STUB: not implemented"; return nil }

// If it's the workflow package, we assume the entire package is deterministic
// so we don't run a pass on it

// Run determinism pass

// Check every register workflow invocation

// Get ignore map for this file

// Only handle calls with followable function pointers

// Get non-determinisms of that package and check

// One report per reason

// isWorkflowFunc checks if f has workflow.Context as a first parameter.
func isWorkflowFunc(f *ast.FuncDecl, pass *analysis.Pass) (b bool) {
	_ = "STUB: not implemented"
	return false
}

type configFileFlag struct{ checker *determinism.Checker }

func (configFileFlag) String() string { _ = "STUB: not implemented"; return "" }

func (c configFileFlag) Set(flag string) error {
	_ = "STUB: not implemented"
	// Load the file into YAML
	return nil
}

// Apply all the ident refs and skip regexes
