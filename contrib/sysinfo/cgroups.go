//go:build linux

package sysinfo

import (
	"time"

	"github.com/containerd/cgroups/v3/cgroup2/stats"
)

func newCGroupInfo() cGroupInfo { _ = "STUB: not implemented"; return *new(cGroupInfo) }

type cGroupInfoImpl struct {
	lastCGroupMemStat *stats.MemoryStat
	cgroupCpuCalc     cgroupCpuCalc
}

func (p *cGroupInfoImpl) Update() (bool, error) {
	_ = "STUB: not implemented"
	return false,

		// Stop updates if not in a container. No need to return the error and log it.
		nil
}

func (p *cGroupInfoImpl) GetLastMemUsage() float64 { _ = "STUB: not implemented"; return 0 }

func (p *cGroupInfoImpl) GetLastCPUUsage() float64 { _ = "STUB: not implemented"; return 0 }

func (p *cGroupInfoImpl) updateCGroupStats() error { _ = "STUB: not implemented"; return nil }

// Only update if a limit has been set

type cgroupCpuCalc struct {
	lastRefresh           time.Time
	lastCpuUsage          uint64
	lastCalculatedPercent float64
}

func (p *cgroupCpuCalc) updateCpuUsage(metrics *stats.Metrics) error {
	_ = "STUB: not implemented"
	// Read CPU quota and period from cpu.max
	return nil
}

// We might simply be in a container with an unset cpu.max in which case we don't want to error

// CPU usage calculation based on delta

// Time passed between this and last check
// Convert to microseconds

// Calculate CPU usage percentage based on the delta

// Update for next call

// readCpuMax reads the cpu.max file to get the CPU quota and period
func readCpuMax(path string) (quota int64, period int64, err error) {
	_ = "STUB: not implemented"
	return 0, 0, nil
}

// Parse the quota (first value)

// Unlimited quota

// Parse the period (second value)
