//go:build integration

package gorpc

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

func TestReleasedPeerCompatibility(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	fixture := filepath.Join(root, "testdata", "interop")
	module, err := os.ReadFile(filepath.Join(fixture, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	sums, err := os.ReadFile(filepath.Join(fixture, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	binaries := make(map[string]string)
	for _, version := range []string{"current", "rc2", "rc3"} {
		binary := filepath.Join(t.TempDir(), "peer")
		args := []string{"build", "-mod=readonly", "-o", binary}
		dir := root
		if version != "current" {
			dir = fixture
			modfile := filepath.Join(t.TempDir(), "go.mod")
			versioned := bytes.ReplaceAll(module, []byte("v1.0.0-rc.3"), []byte("v1.0.0-rc."+strings.TrimPrefix(version, "rc")))
			if err := os.WriteFile(modfile, versioned, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(strings.TrimSuffix(modfile, ".mod")+".sum", sums, 0600); err != nil {
				t.Fatal(err)
			}
			args = append(args, "-modfile="+modfile)
		}
		args = append(args, filepath.Join(fixture, "main.go"))
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		build := exec.CommandContext(ctx, "go", args...)
		build.Dir = dir
		build.Env = append(os.Environ(), "GOWORK=off")
		output, err := build.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("build %s: %v\n%s", version, err, output)
		}
		binaries[version] = binary
	}
	for _, versions := range [][2]string{
		{"current", "rc2"}, {"rc2", "current"},
		{"current", "rc3"}, {"rc3", "current"},
		{"current", "current"},
	} {
		t.Run(versions[0]+"_server_"+versions[1]+"_client", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			server := exec.CommandContext(ctx, binaries[versions[0]], "-listen")
			var serverErrors bytes.Buffer
			server.Stderr = &serverErrors
			stdout, err := server.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := server.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = server.Process.Kill()
				_ = server.Wait()
				if serverErrors.Len() > 0 {
					t.Log(serverErrors.String())
				}
			})
			line, err := bufio.NewReader(stdout).ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			client := exec.CommandContext(ctx, binaries[versions[1]], "-address", strings.TrimSpace(line))
			output, err := client.CombinedOutput()
			if err != nil {
				t.Fatalf("client: %v\n%s", err, output)
			}
			if !strings.Contains(string(output), "PASS:") {
				t.Fatalf("missing peer verification: %s", output)
			}
		})
	}
}
