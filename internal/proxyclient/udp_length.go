package proxyclient

import "cloudflare-h3-test/internal/relay"

func validUDPPayload(n int) bool { return n > 0 && n <= relay.MaxFramePayloadBytes }
