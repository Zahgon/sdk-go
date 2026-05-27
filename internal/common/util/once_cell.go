package util

import "sync"

// A OnceCell attempts to match the semantics of Rust's `OnceCell`, but only stores strings, since that's what's needed
// at the moment. Could be changed to use interface{} to be generic.
type OnceCell struct {
	// Ensures we only call the fetcher one time
	once sync.Once
	// Stores the result of calling fetcher
	value   string
	fetcher func() string
}

// Get fetches the value in the cell, calling the fetcher function if it has not yet been called
func (oc *OnceCell) Get() string { _ = "STUB: not implemented"; return "" }

// PopulatedOnceCell creates an already-initialized cell
func PopulatedOnceCell(value string) OnceCell { _ = "STUB: not implemented"; return *new(OnceCell) }

type fetcher func() string

// LazyOnceCell creates a cell with no initial value, the provided function will be called once and only once the first
// time OnceCell.Get is called
func LazyOnceCell(fetcher fetcher) OnceCell { _ = "STUB: not implemented"; return *new(OnceCell) }
