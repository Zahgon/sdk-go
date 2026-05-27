package internal

import (
	"reflect"

	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/sdk/converter"
)

// encode multiple arguments(arguments to a function).
func encodeArgs(dc converter.DataConverter, args []interface{}) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil, nil

	// decode multiple arguments(arguments to a function).
}

func decodeArgs(dc converter.DataConverter, fnType reflect.Type, data *commonpb.Payloads) (result []reflect.Value, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func decodeArgsToPointerValues(dc converter.DataConverter, fnType reflect.Type, data *commonpb.Payloads) (result []interface{}, err error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func decodeArgsToRawValues(dc converter.DataConverter, fnType reflect.Type, data *commonpb.Payloads) ([]interface{}, error) {
	_ = "STUB: not implemented"
	// Build pointers to results
	return nil, nil
}

// Unmarshal

// Convert results back to non-pointer versions

// Do not set nil pointers

// encode single value(like return parameter).
func encodeArg(dc converter.DataConverter, arg interface{}) (*commonpb.Payloads, error) {
	_ = "STUB: not implemented"
	return nil,

		// decode single value(like return parameter).
		nil
}

func decodeArg(dc converter.DataConverter, data *commonpb.Payloads, valuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

func decodeAndAssignValue(dc converter.DataConverter, from interface{}, toValuePtr interface{}) error {
	_ = "STUB: not implemented"
	return nil
}

// If the value set was a pointer and is the same type as the wanted result,
// instead of panicking because it is not a pointer to a pointer, we will
// just set the pointer
