package main

import (
	"os"

	"cloudflare-h3-test/internal/proxyserver"
)

func main() {
	proxyserver.Main(os.Args[1:])
}
