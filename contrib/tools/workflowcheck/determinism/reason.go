package determinism

import (
	"encoding/gob"
	"go/token"
	"go/types"
)

// PackageNonDeterminisms contains func/var non-determinisms keyed by name.
type PackageNonDeterminisms map[string]NonDeterminisms

// AFact is for implementing golang.org/x/tools/go/analysis.Fact.
func (*PackageNonDeterminisms) AFact() { _ = "STUB: not implemented"; return }

func (n *PackageNonDeterminisms) String() string { _ = "STUB: not implemented"; return "" }

// NonDeterminisms is a set of reasons why a function/var is non-deterministic.
type NonDeterminisms []Reason

// AFact is for implementing golang.org/x/tools/go/analysis.Fact.
func (*NonDeterminisms) AFact() {
	_ = "STUB: not implemented"

	// String returns all reasons as a comma-delimited string.
	return
}

func (n *NonDeterminisms) String() string { _ = "STUB: not implemented"; return "" }

// AppendChildReasonLines appends to lines the set of reasons in this slice.
// This will include newlines and indention based on depth.
func (n NonDeterminisms) AppendChildReasonLines(
	subject string,
	s []string,
	depth int,
	depthRepeat string,
	includePos bool,
	pkg *types.Package,
	lookupCache *PackageLookupCache,
	seenPos map[string]bool,
) []string {
	_ = "STUB: not implemented"
	return nil
}

// Relativize path if it at least starts with working dir

// Recurse if func call and we haven't seen this pos str before

// Reason represents a reason for non-determinism.
type Reason interface {
	Pos() *token.Position
	// String is expected to just include the brief reason, not any child reasons.
	String() string
}

// ReasonDecl represents a function or var that was explicitly marked
// non-deterministic via config.
type ReasonDecl struct {
	SourcePos *token.Position
}

// Pos returns the source position.
func (r *ReasonDecl) Pos() *token.Position {
	_ = "STUB: not implemented"

	// String returns the reason.
	return nil
}

func (r *ReasonDecl) String() string { _ = "STUB: not implemented"; return "" }

// ReasonFuncCall represents a call to a non-deterministic function.
type ReasonFuncCall struct {
	SourcePos *token.Position
	// Fully qualified name
	FuncName string
}

// Pos returns the source position.
func (r *ReasonFuncCall) Pos() *token.Position {
	_ = "STUB: not implemented"

	// String returns the reason.
	return nil
}

func (r *ReasonFuncCall) String() string { _ = "STUB: not implemented"; return "" }

func (r *ReasonFuncCall) PackageName() string { _ = "STUB: not implemented"; return "" }

// If there is an ending parenthesis, it's a method; take the receiver as the name

// Take up until the last dot as the package name

// ReasonVarAccess represents accessing a non-deterministic global variable.
type ReasonVarAccess struct {
	SourcePos *token.Position
	// Fully qualified name
	VarName string
}

// Pos returns the source position.
func (r *ReasonVarAccess) Pos() *token.Position {
	_ = "STUB: not implemented"

	// String returns the reason.
	return nil
}

func (r *ReasonVarAccess) String() string { _ = "STUB: not implemented"; return "" }

// ReasonConcurrency represents a non-deterministic concurrency construct.
type ReasonConcurrency struct {
	SourcePos *token.Position
	Kind      ConcurrencyKind
}

// Pos returns the source position.
func (r *ReasonConcurrency) Pos() *token.Position {
	_ = "STUB: not implemented"

	// String returns the reason.
	return nil
}

func (r *ReasonConcurrency) String() string { _ = "STUB: not implemented"; return "" }

// ConcurrencyKind is a construct that is non-deterministic for
// ReasonConcurrency.
type ConcurrencyKind int

const (
	ConcurrencyKindGo ConcurrencyKind = iota
	ConcurrencyKindRecv
	ConcurrencyKindSend
	ConcurrencyKindRange
)

// ReasonMapRange represents iterating over a map via range.
type ReasonMapRange struct {
	SourcePos *token.Position
}

// Pos returns the source position.
func (r *ReasonMapRange) Pos() *token.Position {
	_ = "STUB: not implemented"

	// String returns the reason.
	return nil
}

func (r *ReasonMapRange) String() string { _ = "STUB: not implemented"; return "" }

func init() {
	// Needed for go vet usage
	gob.Register(&ReasonDecl{})
	gob.Register(&ReasonFuncCall{})
	gob.Register(&ReasonVarAccess{})
	gob.Register(&ReasonConcurrency{})
	gob.Register(&ReasonMapRange{})
}
