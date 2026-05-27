package determinism

import (
	"go/ast"
	"go/token"
	"go/types"
	"regexp"
	"sync"

	"golang.org/x/tools/go/analysis"
)

// Config is config for NewChecker.
type Config struct {
	// If empty, uses DefaultIdentRefs.
	IdentRefs IdentRefs
	// If file matches any here, it is not checked at all.
	SkipFiles []*regexp.Regexp
	// If nil, uses log.Printf.
	DebugfFunc func(string, ...interface{})
	// Must be set to true to see advanced debug logs.
	Debug bool
	// Whether to export a *NonDeterminisms fact per object.
	EnableObjectFacts bool
	// Map `package -> function names` with functions making any argument deterministic
	AcceptsNonDeterministicParameters map[string][]string
}

// Checker is a checker that can run analysis passes to check for
// non-deterministic code.
type Checker struct{ Config }

// NewChecker creates a Checker for the given config.
func NewChecker(config Config) *Checker {
	_ = "STUB: not implemented"
	// Set default refs and clone
	return nil
}

// Default debug

// Build checker

// NewAnalyzer creates a Go analysis analyzer that can be used in existing
// tools. There is a -set-decl flag for adding ident refs overrides and a
// -determinism-debug flag for enabling debug logs. The result is Result and the
// facts on functions are *NonDeterminisms.
func (c *Checker) NewAnalyzer() *analysis.Analyzer { _ = "STUB: not implemented"; return nil }

// Set flags

func (c *Checker) debugf(f string, v ...interface{}) { _ = "STUB: not implemented"; return }

// Run executes this checker for the given pass and stores the fact.
func (c *Checker) Run(pass *analysis.Pass) (PackageNonDeterminisms, error) {
	_ = "STUB: not implemented"
	return *new(PackageNonDeterminisms), nil
}

// Collect all top-level func decls and their types. Also mark var decls as
// non-deterministic if pattern matches.

// Skip this file if it matches any regex

// Update ignore map

// Collect the decls to check and check vars/iface patterns

// Collect top-level func

// Set top-level vars that match pattern as non-deterministic

// See if the top-level vars match patterns

// See if any interface funcs match patterns

// Only need to match explicitly defined methods

// Build collector and do initial pass for each function async

// Parallelize to the number of CPUs

// If we've filled the channel, wait

// Wait for the rest to finish

// Build facts in second pass

func UpdateIgnoreMap(fset *token.FileSet, f *ast.File, m map[ast.Node]struct{}) {
	_ = "STUB: not implemented"
	// Collect only the ignore comments
	return
}

// Check each comment in list so Godoc and others can be in any order

// Bail if no comments

// Add all present in comment map to ignore map

type collector struct {
	// Concurrency-safe/immutable fields
	checker     *Checker
	pass        *analysis.Pass
	lookupCache *PackageLookupCache
	nonDetVars  map[*types.Var]NonDeterminisms
	ignoreMap   map[ast.Node]struct{}

	funcInfos     map[*types.Func]*funcInfo
	funcInfosLock sync.Mutex
}

type funcInfo struct {
	fn                   *types.Func
	reasons              NonDeterminisms
	samePackageCalls     map[*funcInfo]token.Pos
	samePackageCallsLock sync.Mutex
	factsApplied         bool
}

// Concurrency safe
func (f *funcInfo) addSamePackageCall(callee *funcInfo, pos token.Pos) {
	_ = "STUB: not implemented"
	// Ignore direct recursive calls
	return
}

// Only if not already there so we can capture the first token

func (c *collector) funcInfo(fn *types.Func) *funcInfo { _ = "STUB: not implemented"; return nil }

func (c *collector) externalFuncNonDeterminisms(fn *types.Func) NonDeterminisms {
	_ = "STUB: not implemented"
	return *new(NonDeterminisms)
}

func (c *collector) externalVarNonDeterminisms(v *types.Var) NonDeterminisms {
	_ = "STUB: not implemented"
	return *new(NonDeterminisms)
}

func (c *collector) collectFuncInfo(fn *types.Func, decl *ast.FuncDecl) {
	_ = "STUB: not implemented"
	return

	// If matches a pattern, can eagerly stop here
}

// Walk

// Go no deeper if ignoring

// Get the callee

// If it's in a different package, check externals

// Otherwise, we simply add as a same-package call

// Any go statement is non-deterministic

// Check if ident is for a non-deterministic var

// If it's in a different package, check for external non-determinisms.
// Otherwise check local.

// Map and chan ranges are non-deterministic

// Any send statement is non-deterministic

// If the operator is a receive, it is non-deterministic

func (c *collector) checkRangeType(rangeType types.Type, n ast.Node, fn *types.Func) Reason {
	_ = "STUB: not implemented"
	return *new(Reason)
}

// Expects to be called as second pass after all func infos collected.
func (c *collector) applyFacts() PackageNonDeterminisms {
	_ = "STUB: not implemented"
	return *new(PackageNonDeterminisms)
}

// Just run for each. Even though recursive, likely no benefit from
// parallelizing.

// Export fact if requested

// Add non-deterministic vars to the result set too

// Export fact if requested

// Export package fact

func (c *collector) applyFuncNonDeterminisms(f *funcInfo, p PackageNonDeterminisms) {
	_ = "STUB: not implemented"
	return
}

// Recursively call for same-package calls and then see if they have reasons
// for non-determinism

// If we have reasons, place on package non-det

// PackageLookupCache caches fact lookups across packages.
type PackageLookupCache struct {
	pass                       *analysis.Pass
	packageNonDeterminisms     map[*types.Package]PackageNonDeterminisms
	packageNonDeterminismsLock sync.Mutex
}

// NewPackageLookupCache creates a PackageLookupCache.
func NewPackageLookupCache(pass *analysis.Pass) *PackageLookupCache {
	_ = "STUB: not implemented"
	return nil
}

// PackageNonDeterminisms returns non-determinisms for the package or an empty
// set if none found.
func (p *PackageLookupCache) PackageNonDeterminisms(pkg *types.Package) PackageNonDeterminisms {
	_ = "STUB: not implemented"
	return *new(PackageNonDeterminisms)
}

// The import must also be done under lock because it is not concurrency-safe

// We don't care whether it can be imported, we store in the map either way
// to save future lookups

// PackageNonDeterminismsFromName returns the package for the given name and its
// non-determinisms via PackageNonDeterminisms. The package name must be
// directly imported from the given package in scope. Nil is returned for a
// package that is not found.
func (p *PackageLookupCache) PackageNonDeterminismsFromName(
	pkgInScope *types.Package,
	importedPkg string,
) (*types.Package, PackageNonDeterminisms) {
	_ = "STUB: not implemented"
	// Package must be imported from the one in scope or be the one in scope
	return nil, *new(PackageNonDeterminisms)
}
