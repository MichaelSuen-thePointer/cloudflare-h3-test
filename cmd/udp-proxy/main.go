package main

import (
	"log"
	"os"
	"strings"

	PluginEnv "cloudflare-h3-test/internal/pluginopts"
	"cloudflare-h3-test/internal/proxyclient"
	"cloudflare-h3-test/internal/proxyserver"
)

func main() {
	args, cliServer := stripServerFlag(os.Args[1:])
	env, err := PluginEnv.LoadFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	server, err := shouldRunServer(cliServer, env)
	if err != nil {
		log.Fatal(err)
	}
	if server {
		proxyserver.Main(args)
		return
	}
	proxyclient.Main(args)
}

func shouldRunServer(cliServer bool, env PluginEnv.Env) (bool, error) {
	sipServer, ok, err := env.Options.Bool("server")
	if err != nil {
		return false, err
	}
	return cliServer || (ok && sipServer), nil
}

func stripServerFlag(args []string) ([]string, bool) {
	out := make([]string, 0, len(args))
	server := false
	for _, arg := range args {
		switch {
		case arg == "-server" || arg == "--server":
			server = true
		case strings.HasPrefix(arg, "-server="):
			server = parseServerFlagValue(strings.TrimPrefix(arg, "-server="))
		case strings.HasPrefix(arg, "--server="):
			server = parseServerFlagValue(strings.TrimPrefix(arg, "--server="))
		default:
			out = append(out, arg)
		}
	}
	return out, server
}

func parseServerFlagValue(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}
