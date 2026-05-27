package converter

import (
	"reflect"
)

func pointerTo(val interface{}) reflect.Value {
	_ = "STUB: not implemented"
	return *new(reflect.Value)
}

func newOfSameType(val reflect.Value) reflect.Value {
	_ = "STUB: not implemented"
	return *new(reflect.Value)
}

// is value type (i.e. commonpb.WorkflowType)
// is of pointer type (i.e. *commonpb.WorkflowType)
// set newly created value back to passed value

func isInterfaceNil(i interface{}) bool { _ = "STUB: not implemented"; return false }
