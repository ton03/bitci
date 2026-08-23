package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestVersionCommand(t *testing.T) {
	original := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	defer func() { os.Stdout = original }()
	if err := run([]string{"version"}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(output), "0.0.1-alpha.1\n"; got != want {
		t.Fatalf("version output = %q, want %q", got, want)
	}
}

func TestExternalConfigAndStateDir(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(t.TempDir(), "state")
	outsideCheckout := t.TempDir()
	previousDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(outsideCheckout); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(previousDir)
	if err := run([]string{"submit", "--config", configPath, "--state-dir", stateDir, "unit"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"status", "--state-dir", stateDir}); err != nil {
		t.Fatal(err)
	}
}
