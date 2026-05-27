package util

import (
	"sync"
	"time"
)

// MergeDictoRight copies the contents of src to dest
func MergeDictoRight(src map[string]string, dest map[string]string) {
	_ = "STUB: not implemented"
	return
}

// MergeDicts creates a union of the two dicts
func MergeDicts(dic1 map[string]string, dic2 map[string]string) (resultDict map[string]string) {
	_ = "STUB: not implemented"
	return nil
}

// AwaitWaitGroup calls Wait on the given wait
// Returns true if the Wait() call succeeded before the timeout
// Returns false if the Wait() did not return before the timeout
func AwaitWaitGroup(wg *sync.WaitGroup, timeout time.Duration) bool {
	_ = "STUB: not implemented"
	return false
}

// IsInterfaceNil check if interface is nil
func IsInterfaceNil(i interface{}) bool { _ = "STUB: not implemented"; return false }
