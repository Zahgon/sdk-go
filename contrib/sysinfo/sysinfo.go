package sysinfo

import (
	"sync"
	"sync/atomic"

	"github.com/shirou/gopsutil/v4/mem"
	"go.temporal.io/sdk/worker"
)

var sysInfoProvider = sync.OnceValue(func() *psUtilSystemInfoSupplier {
	return &psUtilSystemInfoSupplier{
		cGroupInfo: newCGroupInfo(),
	}
})

// SysInfoProvider returns a shared SysInfoProvider using gopsutil.
// Supports cgroup metrics in containerized Linux environments.
func SysInfoProvider() worker.SysInfoProvider {
	_ = "STUB: not implemented"
	return *new(worker.SysInfoProvider)
}

type psUtilSystemInfoSupplier struct {
	mu          sync.Mutex
	lastRefresh atomic.Int64 // UnixNano, atomic for lock-free reads in maybeRefresh

	lastMemStat  *mem.VirtualMemoryStat
	lastCpuUsage float64

	stopTryingToGetCGroupInfo bool
	cGroupInfo                cGroupInfo
}

type cGroupInfo interface {
	// Update requests an update of the cgroup stats. This is a no-op if not in a cgroup. Returns
	// true if cgroup stats should continue to be updated, false if not in a cgroup or the returned
	// error is considered unrecoverable.
	Update() (bool, error)
	// GetLastMemUsage returns last known memory usage as a fraction of the cgroup limit. 0 if not
	// in a cgroup or limit is not set.
	GetLastMemUsage() float64
	// GetLastCPUUsage returns last known CPU usage as a fraction of the cgroup limit. 0 if not in a
	// cgroup or limit is not set.
	GetLastCPUUsage() float64
}

func (p *psUtilSystemInfoSupplier) MemoryUsage(infoContext *worker.SysInfoContext) (float64, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (p *psUtilSystemInfoSupplier) CpuUsage(infoContext *worker.SysInfoContext) (float64, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (p *psUtilSystemInfoSupplier) maybeRefresh(infoContext *worker.SysInfoContext) error {
	_ = "STUB: not implemented"
	return nil
}

// Double check refresh is still needed
