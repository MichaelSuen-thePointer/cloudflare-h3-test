package relay

import (
	"sort"
	"time"
)

type Summary struct {
	Count int     `json:"count"`
	MinMS float64 `json:"min_ms"`
	AvgMS float64 `json:"avg_ms"`
	P50MS float64 `json:"p50_ms"`
	P90MS float64 `json:"p90_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
	MaxMS float64 `json:"max_ms"`
}

func Summarize(durations []time.Duration) Summary {
	if len(durations) == 0 {
		return Summary{}
	}
	xs := append([]time.Duration(nil), durations...)
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	var sum time.Duration
	for _, d := range xs {
		sum += d
	}
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	pct := func(p float64) float64 {
		idx := int(float64(len(xs)-1) * p)
		if idx < 0 {
			idx = 0
		}
		if idx >= len(xs) {
			idx = len(xs) - 1
		}
		return ms(xs[idx])
	}
	return Summary{
		Count: len(xs),
		MinMS: ms(xs[0]),
		AvgMS: ms(sum / time.Duration(len(xs))),
		P50MS: pct(0.50),
		P90MS: pct(0.90),
		P95MS: pct(0.95),
		P99MS: pct(0.99),
		MaxMS: ms(xs[len(xs)-1]),
	}
}
