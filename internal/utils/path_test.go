package utils

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestRemoveEmptyDirs(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "altmount-test-remove-dirs")
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	root := filepath.Join(tempDir, "root")
	err = os.MkdirAll(root, 0755)
	if err != nil {
		t.Fatal(err)
	}

	// Create nested empty directories: root/a/b/c
	nested := filepath.Join(root, "a", "b", "c")
	err = os.MkdirAll(nested, 0755)
	if err != nil {
		t.Fatal(err)
	}

	// Remove c, and expect b and a to be removed too
	RemoveEmptyDirs(root, nested)

	// Check if a, b, c were removed
	for _, dir := range []string{"a", "a/b", "a/b/c"} {
		path := filepath.Join(root, dir)
		if _, err := os.Stat(path); err == nil {
			t.Errorf("Expected directory %s to be removed, but it exists", path)
		}
	}

	// Check if root still exists
	if _, err := os.Stat(root); os.IsNotExist(err) {
		t.Error("Expected root directory to exist, but it was removed")
	}

	// Test with non-empty directory
	// root/x/y/z, with root/x/keep.txt
	xDir := filepath.Join(root, "x")
	yDir := filepath.Join(xDir, "y")
	zDir := filepath.Join(yDir, "z")
	err = os.MkdirAll(zDir, 0755)
	if err != nil {
		t.Fatal(err)
	}

	keepFile := filepath.Join(xDir, "keep.txt")
	err = os.WriteFile(keepFile, []byte("keep"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Remove z, and expect y to be removed, but x should stay
	RemoveEmptyDirs(root, zDir)

	if _, err := os.Stat(zDir); err == nil {
		t.Error("Expected zDir to be removed")
	}
	if _, err := os.Stat(yDir); err == nil {
		t.Error("Expected yDir to be removed")
	}
	if _, err := os.Stat(xDir); os.IsNotExist(err) {
		t.Error("Expected xDir to still exist because it contains keep.txt")
	}
	if _, err := os.Stat(keepFile); os.IsNotExist(err) {
		t.Error("Expected keep.txt to still exist")
	}
}

func TestRemoveEmptyDirsRejectsEscapedPaths(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("containment contract uses Unix symlink semantics")
	}

	t.Run("sibling prefix", func(t *testing.T) {
		base := t.TempDir()
		root := filepath.Join(base, "root")
		escaped := filepath.Join(base, "root-escape", "a", "b")
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(escaped, 0o755); err != nil {
			t.Fatal(err)
		}

		RemoveEmptyDirs(root, escaped)

		if _, err := os.Stat(escaped); err != nil {
			t.Errorf("escaped sibling path was mutated: %v", err)
		}
	})

	t.Run("parent symlink", func(t *testing.T) {
		base := t.TempDir()
		root := filepath.Join(base, "root")
		outside := filepath.Join(base, "outside")
		escaped := filepath.Join(outside, "a", "b")
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(escaped, 0o755); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(root, "linked")
		if err := os.Symlink(outside, link); err != nil {
			t.Fatal(err)
		}

		RemoveEmptyDirs(root, filepath.Join(link, "a", "b"))

		if _, err := os.Stat(escaped); err != nil {
			t.Errorf("path reached through parent symlink was mutated: %v", err)
		}
		if _, err := os.Lstat(link); err != nil {
			t.Errorf("parent symlink was mutated: %v", err)
		}
	})
}
