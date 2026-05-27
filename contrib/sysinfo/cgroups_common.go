package sysinfo

// handleCGroupUpdateError interprets the error from a cgroup stats update.
// Returns (false, nil) if cgroup files don't exist (not in a container),
// (true, err) for real errors, and (true, nil) on success.
func handleCGroupUpdateError(err error) (bool, error) { _ = "STUB: not implemented"; return false, nil }
