package util

import (
	"reflect"
	"regexp"

	commandpb "go.temporal.io/api/command/v1"
	historypb "go.temporal.io/api/history/v1"
)

var privateField = regexp.MustCompile("^[a-z]")

func anyToString(d interface{}) string { _ = "STUB: not implemented"; return "" }

func valueToString(v reflect.Value) string { _ = "STUB: not implemented"; return "" }

// HistoryEventToString convert HistoryEvent to string
func HistoryEventToString(e *historypb.HistoryEvent) string { _ = "STUB: not implemented"; return "" }

// CommandToString convert Command to string
func CommandToString(d *commandpb.Command) string { _ = "STUB: not implemented"; return "" }
