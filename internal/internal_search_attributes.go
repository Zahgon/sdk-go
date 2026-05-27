package internal

import (
	"reflect"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/log"
)

type (
	// SearchAttributes represents a collection of typed search attributes
	SearchAttributes struct {
		untypedValue map[SearchAttributeKey]interface{}
	}

	// SearchAttributeUpdate represents a change to SearchAttributes
	SearchAttributeUpdate func(*SearchAttributes)

	// SearchAttributeKey represents a typed search attribute key.
	SearchAttributeKey interface {
		// GetName of the search attribute.
		GetName() string
		// GetValueType of the search attribute.
		GetValueType() enumspb.IndexedValueType
		// GetReflectType of the search attribute.
		GetReflectType() reflect.Type
	}

	baseSearchAttributeKey struct {
		name        string
		valueType   enumspb.IndexedValueType
		reflectType reflect.Type
	}

	// SearchAttributeKeyString represents a search attribute key for a text attribute type.
	SearchAttributeKeyString struct {
		baseSearchAttributeKey
	}

	// SearchAttributeKeyKeyword represents a search attribute key for a keyword attribute type.
	SearchAttributeKeyKeyword struct {
		baseSearchAttributeKey
	}

	// SearchAttributeKeyBool represents a search attribute key for a boolean attribute type.
	SearchAttributeKeyBool struct {
		baseSearchAttributeKey
	}

	// SearchAttributeKeyInt64 represents a search attribute key for a integer attribute type.
	SearchAttributeKeyInt64 struct {
		baseSearchAttributeKey
	}

	// SearchAttributeKeyFloat64 represents a search attribute key for a float attribute type.
	SearchAttributeKeyFloat64 struct {
		baseSearchAttributeKey
	}

	// SearchAttributeKeyTime represents a search attribute key for a date time attribute type.
	SearchAttributeKeyTime struct {
		baseSearchAttributeKey
	}

	// SearchAttributeKeyKeywordList represents a search attribute key for a list of keyword attribute type.
	SearchAttributeKeyKeywordList struct {
		baseSearchAttributeKey
	}
)

// GetName of the search attribute.
func (bk baseSearchAttributeKey) GetName() string {
	_ = "STUB: not implemented"

	// GetValueType of the search attribute.
	return ""
}

func (bk baseSearchAttributeKey) GetValueType() enumspb.IndexedValueType {
	_ = "STUB: not implemented"
	return *

	// GetReflectType of the search attribute.
	new(enumspb.IndexedValueType)
}

func (bk baseSearchAttributeKey) GetReflectType() reflect.Type {
	_ = "STUB: not implemented"
	return *new(reflect.Type)
}

func NewSearchAttributeKeyString(name string) SearchAttributeKeyString {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyString)
}

// ValueSet creates an update to set the value of the attribute.
func (k SearchAttributeKeyString) ValueSet(value string) SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

// ValueUnset creates an update to remove the attribute.
func (k SearchAttributeKeyString) ValueUnset() SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

func NewSearchAttributeKeyKeyword(name string) SearchAttributeKeyKeyword {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyKeyword)
}

// ValueSet creates an update to set the value of the attribute.
func (k SearchAttributeKeyKeyword) ValueSet(value string) SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

// ValueUnset creates an update to remove the attribute.
func (k SearchAttributeKeyKeyword) ValueUnset() SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

func NewSearchAttributeKeyBool(name string) SearchAttributeKeyBool {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyBool)
}

// ValueSet creates an update to set the value of the attribute.
func (k SearchAttributeKeyBool) ValueSet(value bool) SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

// ValueUnset creates an update to remove the attribute.
func (k SearchAttributeKeyBool) ValueUnset() SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

func NewSearchAttributeKeyInt64(name string) SearchAttributeKeyInt64 {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyInt64)
}

// ValueSet creates an update to set the value of the attribute.
func (k SearchAttributeKeyInt64) ValueSet(value int64) SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

// ValueUnset creates an update to remove the attribute.
func (k SearchAttributeKeyInt64) ValueUnset() SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

func NewSearchAttributeKeyFloat64(name string) SearchAttributeKeyFloat64 {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyFloat64)
}

// ValueSet creates an update to set the value of the attribute.
func (k SearchAttributeKeyFloat64) ValueSet(value float64) SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

// ValueUnset creates an update to remove the attribute.
func (k SearchAttributeKeyFloat64) ValueUnset() SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

func NewSearchAttributeKeyTime(name string) SearchAttributeKeyTime {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyTime)
}

// ValueSet creates an update to set the value of the attribute.
func (k SearchAttributeKeyTime) ValueSet(value time.Time) SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

// ValueUnset creates an update to remove the attribute.
func (k SearchAttributeKeyTime) ValueUnset() SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

func NewSearchAttributeKeyKeywordList(name string) SearchAttributeKeyKeywordList {
	_ = "STUB: not implemented"
	return *new(SearchAttributeKeyKeywordList)
}

// ValueSet creates an update to set the value of the attribute.
func (k SearchAttributeKeyKeywordList) ValueSet(values []string) SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

// ValueUnset creates an update to remove the attribute.
func (k SearchAttributeKeyKeywordList) ValueUnset() SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

func NewSearchAttributes(attributes ...SearchAttributeUpdate) SearchAttributes {
	_ = "STUB: not implemented"
	return *new(SearchAttributes)
}

// GetString gets a value for the given key and whether it was present.
func (sa SearchAttributes) GetString(key SearchAttributeKeyString) (string, bool) {
	_ = "STUB: not implemented"
	return "", false
}

// GetKeyword gets a value for the given key and whether it was present.
func (sa SearchAttributes) GetKeyword(key SearchAttributeKeyKeyword) (string, bool) {
	_ = "STUB: not implemented"
	return "", false
}

// GetBool gets a value for the given key and whether it was present.
func (sa SearchAttributes) GetBool(key SearchAttributeKeyBool) (bool, bool) {
	_ = "STUB: not implemented"
	return false, false
}

// GetInt64 gets a value for the given key and whether it was present.
func (sa SearchAttributes) GetInt64(key SearchAttributeKeyInt64) (int64, bool) {
	_ = "STUB: not implemented"
	return 0, false
}

// GetFloat64 gets a value for the given key and whether it was present.
func (sa SearchAttributes) GetFloat64(key SearchAttributeKeyFloat64) (float64, bool) {
	_ = "STUB: not implemented"
	return 0, false
}

// GetTime gets a value for the given key and whether it was present.
func (sa SearchAttributes) GetTime(key SearchAttributeKeyTime) (time.Time, bool) {
	_ = "STUB: not implemented"
	return *new(time.Time), false
}

// GetKeywordList gets a value for the given key and whether it was present.
func (sa SearchAttributes) GetKeywordList(key SearchAttributeKeyKeywordList) ([]string, bool) {
	_ = "STUB: not implemented"
	return nil, false
}

// Return a copy to prevent caller from mutating the underlying value

// ContainsKey gets whether a key is present.
func (sa SearchAttributes) ContainsKey(key SearchAttributeKey) bool {
	_ = "STUB: not implemented"
	return false
}

// Size gets the size of the attribute collection.
func (sa SearchAttributes) Size() int { _ = "STUB: not implemented"; return 0 }

// GetUntypedValues gets a copy of the collection with raw types.
func (sa SearchAttributes) GetUntypedValues() map[SearchAttributeKey]interface{} {
	_ = "STUB: not implemented"
	return nil
}

// Filter out nil values

// Copy creates an update that copies existing values.
//
//workflowcheck:ignore
func (sa SearchAttributes) Copy() SearchAttributeUpdate {
	_ = "STUB: not implemented"
	return *new(SearchAttributeUpdate)
}

// GetUntypedValues returns a copy of the map without nil values
// so the copy won't delete any existing values

func serializeUntypedSearchAttributes(input map[string]interface{}) (*commonpb.SearchAttributes, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// If search attribute value is already of Payload type, then use it directly.
// This allows to copy search attributes from workflow info to child workflow options.

func serializeTypedSearchAttributes(searchAttributes map[SearchAttributeKey]interface{}) (*commonpb.SearchAttributes, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Server does not remove search attributes if they set a type

func serializeSearchAttributes(
	untypedAttributes map[string]interface{},
	typedAttributes SearchAttributes,
) (*commonpb.SearchAttributes, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func convertToTypedSearchAttributes(logger log.Logger, attributes map[string]*commonpb.Payload) SearchAttributes {
	_ = "STUB: not implemented"
	return *new(SearchAttributes)
}

// The type metadata is usually in PascalCase (e.g. "KeywordList") but in
// rare cases may be in SCREAMING_SNAKE_CASE (e.g. "INDEXED_VALUE_TYPE_KEYWORD_LIST").

// For TemporalChangeVersion, we imply the value type
