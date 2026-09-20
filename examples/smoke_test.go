//go:build integration

package examples_test

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExampleCommands(t *testing.T) {
	bin := t.TempDir()
	buildCtx, cancelBuild := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancelBuild()
	build := exec.CommandContext(buildCtx, "go", "build", "-race", "-mod=readonly", "-o", bin+string(os.PathSeparator),
		"./inventory/server", "./inventory/client", "./serverstream", "./clientstream", "./bidistream", "./peers")
	build.Env = append(os.Environ(), "GOWORK=off")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build examples: %v\n%s", err, output)
	}
	for _, name := range []string{"serverstream", "clientstream", "bidistream", "peers"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, filepath.Join(bin, name))
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("run: %v\n%s", err, output)
			}
			checkDocumentedOutput(t, filepath.Join(name, "README.md"), output)
		})
	}
	for _, secret := range []string{"", "example-test-secret"} {
		name := "inventory"
		if secret != "" {
			name += "_authenticated"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			server := exec.CommandContext(ctx, filepath.Join(bin, "server"), "-addr", "127.0.0.1:0")
			server.Env = append(os.Environ(), "GORPC_EXAMPLE_SECRET="+secret)
			var serverErrors bytes.Buffer
			server.Stderr = &serverErrors
			stdout, err := server.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := server.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			t.Cleanup(func() {
				if !waited {
					_ = server.Process.Kill()
					_ = server.Wait()
				}
				if serverErrors.Len() > 0 {
					t.Log(serverErrors.String())
				}
			})
			line, err := bufio.NewReader(stdout).ReadString('\n')
			if err != nil || !strings.HasPrefix(line, "listening on ") {
				t.Fatalf("server startup: %q, %v", line, err)
			}
			address := strings.TrimSpace(strings.TrimPrefix(line, "listening on "))
			client := exec.CommandContext(ctx, filepath.Join(bin, "client"), "-addr", address)
			client.Env = server.Env
			output, err := client.CombinedOutput()
			if err != nil {
				t.Fatalf("client: %v\n%s", err, output)
			}
			checkDocumentedOutput(t, filepath.Join("inventory", "README.md"), output)
			if err := server.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			err = server.Wait()
			waited = true
			if err != nil {
				t.Fatalf("server shutdown: %v\n%s", err, &serverErrors)
			}
			if serverErrors.Len() != 0 {
				t.Fatalf("server stderr:\n%s", &serverErrors)
			}
		})
	}
}

func checkDocumentedOutput(t *testing.T, path string, output []byte) {
	t.Helper()
	readme, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The displayed output is part of the example's contract too.
	block := "~~~text\n" + string(output) + "~~~"
	if len(output) == 0 || !strings.Contains(string(readme), block) {
		t.Fatalf("output does not match a documented block in %s:\n%s", path, output)
	}
}
