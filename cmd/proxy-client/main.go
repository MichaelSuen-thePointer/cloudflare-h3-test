package main

import (
	"os"

	"cloudflare-h3-test/internal/proxyclient"
)

func main() {
	proxyclient.Main(os.Args[1:])
}
