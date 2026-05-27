//go:build !linux

package sysinfo

func newCGroupInfo() cGroupInfo { _ = "STUB: not implemented"; return *new(cGroupInfo) }

type cGroupInfoImpl struct {
}

func (p *cGroupInfoImpl) Update() (bool, error) { _ = "STUB: not implemented"; return false, nil }

func (p *cGroupInfoImpl) GetLastMemUsage() float64 { _ = "STUB: not implemented"; return 0 }

func (p *cGroupInfoImpl) GetLastCPUUsage() float64 { _ = "STUB: not implemented"; return 0 }
