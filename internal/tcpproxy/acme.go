package tcpproxy

import (
	"os"
	"path/filepath"
)

// findACMECertKey mirrors the UDP server's acme.sh locations without changing
// shared behavior. Callers validate host before using it as a path component.
func findACMECertKey(host string) (string, string, bool) {
	home, _ := os.UserHomeDir()
	return findACMECertKeyInHomes(host, acmeHomeCandidates(home, os.Getenv("HOME")), acmeFileExists)
}

func acmeHomeCandidates(userHome, envHome string) []string {
	var homes []string
	seen := map[string]bool{}
	for _, home := range []string{userHome, envHome, "/root"} {
		if home != "" && !seen[home] {
			homes = append(homes, home)
			seen[home] = true
		}
	}
	return homes
}

func findACMECertKeyInHomes(host string, homes []string, exists func(string) bool) (string, string, bool) {
	for _, home := range homes {
		for _, name := range []string{host, host + "_ecc"} {
			dir := filepath.Join(home, ".acme.sh", name)
			cert, key := filepath.Join(dir, "fullchain.cer"), filepath.Join(dir, host+".key")
			if exists(cert) && exists(key) {
				return cert, key, true
			}
		}
	}
	return "", "", false
}

func acmeFileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}
