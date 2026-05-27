package main

import (
	"go/ast"
	"log"
	"os"
)

type (
	// command line config params
	config struct {
		rootDir string
		fix     bool
	}
)

var changesNeeded = false

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	if changesNeeded {
		log.Fatal("Changes needed, see previous stdout for which objects. Re-run command with -fix to auto-generate new docs.")
	}
}

func run() error { _ = "STUB: not implemented"; return nil }

// Go through public packages and identify wrappers to internal types/funcs

// TODO: remove

// Go through internal files and match the definitions of private/public pairings

// Traverse the AST of public packages to identify wrappers for internal objects
func processPublic(file *os.File) (map[string]string, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func extractTypeValue(expr ast.Expr) string { _ = "STUB: not implemented"; return "" }

// Do nothing

//fmt.Printf("[WARN] Unsupported type: %T\n", t)

func checkValueSpec(spec *ast.ValueSpec) string {
	_ = "STUB: not implemented"
	// Check if the type of the value spec contains "internal."
	return ""
}

// Check the expressions (values assigned) for "internal."

// Check if a public function is a wrapper around an internal function
func checkFunction(funcDecl *ast.FuncDecl) string {
	_ = "STUB: not implemented"
	// Ensure the function has a body
	return ""
}

// Ensure the body has exactly one statement

// Check if the single statement is a return statement

// Ensure the return statement directly calls an internal function

// Functions that don't return anything

// Check if a call expression is calling an internal function
func isInternalFunctionCall(callExpr *ast.CallExpr) string {
	_ = "STUB: not implemented"
	// Check if the function being called is a SelectorExpr (e.g., "internal.SomeFunction")
	return ""
}

// Check for type assertions like `var _ = internal.SomeType(nil)`
func isTypeAssertion(valueSpec *ast.ValueSpec) bool { _ = "STUB: not implemented"; return false }

func extractPackageName(file *os.File) (string, error) { _ = "STUB: not implemented"; return "", nil }

// Split the line to extract the package name

// Identify type/func definitions in the file and match to any private:public mappings.
// If mapping is identified, check if doc comment exists for such mapping.
func processInternal(cfg config, file *os.File, pairs map[string]map[string]string) error {
	_ = "STUB: not implemented"
	return nil
}

// NOTE: This makes an assumption that Go files are either using just tabs or just spaces.

// Keep track of code block, for when we check a valid definition below,
// gofmt will sometimes format links like "[Visibility]: https://sample.url"
// to the bottom of the doc string.

// Check for old docs links to remove

// Check for new doc links to add

// Find the "Exposed As" line in the doc comment

// Check for new doc pairs

// If there is an existing "Exposed As" docstring

// The last line of commentBlock hasn't been written to newFile yet,
// so check if existingDoclink is that scenario

// Last line of existing docstring hasn't been written yet,
// write that line to newFile, then set the updatedLine to
// be the next line to be written to newFile

// update inFunc after we actually check for doclinks to allow us to check
// a function's definition, without checking anything inside the function

func isValidDefinition(line string, inGroup *string, insideStruct *bool) bool {
	_ = "STUB: not implemented"
	return false
}

// Check if the line starts a grouped definition

// Handle single-line struct, variable, or function definitions

// Checks if `line` is a valid definition, and that definition is for `private`
func isValidDefinitionWithMatch(line, private string, inGroup string, insideStruct bool) bool {
	_ = "STUB: not implemented"
	// Vars with underscores are often used to assert interface validation and
	// do not require docs
	return false
}

// Handle single-line struct, variable, or function definitions
