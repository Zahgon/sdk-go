package internal

import (
	protocolpb "go.temporal.io/api/protocol/v1"
)

type eventMsgIndex []*protocolpb.Message

// indexMessagesByEventID creates an index over a set of input messages that
// allows for access to messages with an event ID less than or equal to a
// specific upper bound. The order of messages with the same event ID will be
// preserved.
func indexMessagesByEventID(msgs []*protocolpb.Message) *eventMsgIndex {
	_ = "STUB: not implemented"
	return nil
}

// takeLTE removes and returns the messages in this index that have an event ID
// less than or equal to the input argument.
func (emi *eventMsgIndex) takeLTE(eventID int64) []*protocolpb.Message {
	_ = "STUB: not implemented"
	return nil
}
