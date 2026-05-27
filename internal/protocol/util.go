package protocol

import (
	"errors"

	protocolpb "go.temporal.io/api/protocol/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var ErrProtoNameNotFound = errors.New("protocol name not found")

// NameFromMessage extracts the name of the protocol to which the supplied
// message belongs.
func NameFromMessage(msg *protocolpb.Message) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

// MustMarshalAny serializes a protobuf message into an Any or panics.
func MustMarshalAny(msg proto.Message) *anypb.Any { _ = "STUB: not implemented"; return nil }
