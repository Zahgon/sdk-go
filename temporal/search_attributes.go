package temporal

import "go.temporal.io/sdk/internal"

type (
	// SearchAttributes represents a collection of typed search attributes. Create with [NewSearchAttributes].
	SearchAttributes = internal.SearchAttributes

	// SearchAttributeUpdate represents a change to SearchAttributes.
	SearchAttributeUpdate = internal.SearchAttributeUpdate

	// SearchAttributeKey represents a typed search attribute key.
	SearchAttributeKey = internal.SearchAttributeKey

	// SearchAttributeKeyString represents a search attribute key for a text attribute type. Create with
	// [NewSearchAttributeKeyString].
	SearchAttributeKeyString = internal.SearchAttributeKeyString

	// SearchAttributeKeyKeyword represents a search attribute key for a keyword attribute type. Create with
	// [NewSearchAttributeKeyKeyword].
	SearchAttributeKeyKeyword = internal.SearchAttributeKeyKeyword

	// SearchAttributeKeyBool represents a search attribute key for a boolean attribute type. Create with
	// [NewSearchAttributeKeyBool].
	SearchAttributeKeyBool = internal.SearchAttributeKeyBool

	// SearchAttributeKeyInt64 represents a search attribute key for a integer attribute type. Create with
	// [NewSearchAttributeKeyInt64].
	SearchAttributeKeyInt64 = internal.SearchAttributeKeyInt64

	// SearchAttributeKeyFloat64 represents a search attribute key for a double attribute type. Create with
	// [NewSearchAttributeKeyFloat64].
	SearchAttributeKeyFloat64 = internal.SearchAttributeKeyFloat64

	// SearchAttributeKeyTime represents a search attribute key for a time attribute type. Create with
	// [NewSearchAttributeKeyTime].
	SearchAttributeKeyTime = internal.SearchAttributeKeyTime

	// SearchAttributeKeyKeywordList represents a search attribute key for a keyword list attribute type. Create with
	// [NewSearchAttributeKeyKeywordList].
	SearchAttributeKeyKeywordList = internal.SearchAttributeKeyKeywordList
)

// NewSearchAttributeKeyString creates a new string-based key.
func NewSearchAttributeKeyString(name string) SearchAttributeKeyString {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyString)
}

// NewSearchAttributeKeyKeyword creates a new keyword-based key.
func NewSearchAttributeKeyKeyword(name string) SearchAttributeKeyKeyword {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyKeyword)
}

// NewSearchAttributeKeyBool creates a new bool-based key.
func NewSearchAttributeKeyBool(name string) SearchAttributeKeyBool {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyBool)
}

// NewSearchAttributeKeyInt64 creates a new int64-based key.
func NewSearchAttributeKeyInt64(name string) SearchAttributeKeyInt64 {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyInt64)
}

// NewSearchAttributeKeyFloat64 creates a new float64-based key.
func NewSearchAttributeKeyFloat64(name string) SearchAttributeKeyFloat64 {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyFloat64)
}

// NewSearchAttributeKeyTime creates a new time-based key.
func NewSearchAttributeKeyTime(name string) SearchAttributeKeyTime {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyTime)
}

// NewSearchAttributeKeyKeywordList creates a new keyword-list-based key.
func NewSearchAttributeKeyKeywordList(name string) SearchAttributeKeyKeywordList {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyKeywordList)
}

// NewSearchAttributes creates a new search attribute collection for the given updates.
func NewSearchAttributes(attributes ...SearchAttributeUpdate) SearchAttributes {
	_ = "STUB: not implemented"
	return *new(SearchAttributes)
}
