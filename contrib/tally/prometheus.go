package tally

import (
	"github.com/uber-go/tally/v4"
)

// PrometheusSanitizeOptions is the set of sanitize options for
// tally.ScopeOptions to match Prometheus naming rules.
var PrometheusSanitizeOptions = tally.SanitizeOptions{
	NameCharacters:       tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: []rune{'_'}},
	KeyCharacters:        tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: []rune{'_'}},
	ValueCharacters:      tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: []rune{'_'}},
	ReplacementCharacter: tally.DefaultReplacementCharacter,
}

type prometheusNamingScope struct{ scope tally.Scope }

// NewPrometheusNamingScope makes a scope that appends certain strings to names
// to conform to OpenMetrics naming standards. This should be used in addition
// to DefaultPrometheusSanitizeOptions.
func NewPrometheusNamingScope(scope tally.Scope) tally.Scope {
	_ = "STUB: not implemented"
	return *new(tally.Scope)
}

func (p *prometheusNamingScope) Counter(name string) tally.Counter {
	_ = "STUB: not implemented"
	return *new(tally.Counter)
}

func (p *prometheusNamingScope) Gauge(name string) tally.Gauge {
	_ = "STUB: not implemented"
	return *new(tally.Gauge)
}

func (p *prometheusNamingScope) Timer(name string) tally.Timer {
	_ = "STUB: not implemented"
	return *new(tally.Timer)
}

func (p *prometheusNamingScope) Histogram(name string, buckets tally.Buckets) tally.Histogram {
	_ = "STUB: not implemented"
	return *new(tally.Histogram)
}

func (p *prometheusNamingScope) Tagged(tags map[string]string) tally.Scope {
	_ = "STUB: not implemented"
	return *new(tally.Scope)
}

func (p *prometheusNamingScope) SubScope(name string) tally.Scope {
	_ = "STUB: not implemented"
	return *new(tally.Scope)
}

func (p *prometheusNamingScope) Capabilities() tally.Capabilities {
	_ = "STUB: not implemented"
	return *new(tally.Capabilities)
}
