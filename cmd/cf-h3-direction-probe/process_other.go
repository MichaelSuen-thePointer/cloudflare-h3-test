//go:build !windows

package main

import "os/exec"

func hideProcess(c *exec.Cmd) {}
