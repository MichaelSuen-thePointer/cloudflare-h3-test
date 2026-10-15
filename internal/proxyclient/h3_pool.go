package proxyclient

import "time"

const h3PingInterval = 10 * time.Second

type h3Pool = standbyPool[*h3ProviderStream]

func newH3Pool(provider *h3Provider, target int, timeout time.Duration) *h3Pool {
	p := newStandbyPool(target, timeout, h3PingInterval, provider.open, provider.quality, "h3", "open-idle")
	if provider.counters.enabled {
		p.attachTiming = func(d time.Duration) { provider.counters.attachNanos.Add(d.Nanoseconds()) }
	}
	return p
}
