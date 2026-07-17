package utils

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// CheckDirectoryWritable checks if a directory exists and is writable.
// If the directory doesn't exist, it attempts to create it.
func CheckDirectoryWritable(path string) error {
	if path == "" {
		return fmt.Errorf("path cannot be empty")
	}

	// Convert to absolute path for clearer error messages
	absPath, err := filepath.Abs(path)
	if err != nil {
		absPath = path // fallback to original if abs fails
	}

	// Check if path exists
	info, err := os.Stat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Directory doesn't exist, try to create it
			if err := os.MkdirAll(absPath, 0755); err != nil {
				return fmt.Errorf("directory %s does not exist and cannot be created: %w", absPath, err)
			}
		} else {
			return fmt.Errorf("cannot access directory %s: %w", absPath, err)
		}
	} else {
		// Path exists, check if it's a directory
		if !info.IsDir() {
			return fmt.Errorf("path %s exists but is not a directory", absPath)
		}
	}

	// Test write permissions by creating a temporary file
	testFile := filepath.Join(absPath, ".altmount-write-test")
	file, err := os.Create(testFile)
	if err != nil {
		return fmt.Errorf("directory %s is not writable: %w", absPath, err)
	}

	defer file.Close()
	// Write some test data
	_, writeErr := file.Write([]byte("test"))

	// Clean up test file
	os.Remove(testFile)

	if writeErr != nil {
		return fmt.Errorf("directory %s is not writable: %w", absPath, writeErr)
	}

	return nil
}

// RemoveEmptyDirs recursively removes empty parent directories starting from 'path'
// up towards 'root' (exclusive). It stops if it encounters a non-empty directory
// or reaches the root.
func RemoveEmptyDirs(root, path string) {
	_ = RemoveEmptyDirsSafe(root, path)
}

// RemoveEmptyDirsSafe removes empty directories below root while reporting
// authority, traversal, symlink, and filesystem errors.
func RemoveEmptyDirsSafe(root, path string) error {
	if root == "" || path == "" {
		return nil
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(root, path)
	if err != nil || !filepath.IsLocal(relative) {
		return fmt.Errorf("path %q is outside root %q", path, root)
	}
	if relative == "." {
		return nil
	}
	rooted, err := os.OpenRoot(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			info, inspectErr := os.Lstat(root)
			if errors.Is(inspectErr, fs.ErrNotExist) {
				return nil
			}
			if inspectErr != nil {
				return inspectErr
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return fmt.Errorf("root %q is not an unambiguous directory", root)
			}
			return fmt.Errorf("root %q changed while acquiring authority", root)
		}
		return err
	}
	defer rooted.Close()
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("root %q is not an unambiguous directory", root)
	}
	rootInfo, err := rooted.Stat(".")
	if err != nil {
		return err
	}
	if !os.SameFile(info, rootInfo) {
		return fmt.Errorf("root %q changed while acquiring authority", root)
	}

	var parents []string
	missing := false
	for current := relative; current != "."; current = filepath.Dir(current) {
		parents = append(parents, current)
		info, statErr := rooted.Lstat(current)
		if errors.Is(statErr, fs.ErrNotExist) {
			missing = true
			continue
		}
		if statErr != nil {
			return statErr
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("path component %q is not an unambiguous directory", current)
		}
	}
	if missing {
		return nil
	}
	for _, current := range parents {
		if removeErr := rooted.Remove(current); removeErr != nil {
			if errors.Is(removeErr, syscall.ENOTEMPTY) || errors.Is(removeErr, syscall.EEXIST) {
				return nil
			}
			return removeErr
		}
	}
	return nil
}

// JoinAbsPath safely joins a base path with another path (which could be absolute or relative).
// If the second path is absolute and starts with the base path, it returns the second path as is.
// Otherwise, it joins them normally.
func JoinAbsPath(basePath, otherPath string) string {
	if basePath == "" {
		return otherPath
	}

	// Ensure consistent slashes for comparison
	cleanBase := strings.TrimSuffix(filepath.ToSlash(basePath), "/")
	cleanOther := filepath.ToSlash(otherPath)

	// If otherPath is absolute and starts with basePath, don't join
	if filepath.IsAbs(cleanOther) && (cleanOther == cleanBase || strings.HasPrefix(cleanOther, cleanBase+"/")) {
		return filepath.FromSlash(cleanOther)
	}

	// Join them, ensuring otherPath is treated as relative to base
	relOther := strings.TrimPrefix(cleanOther, "/")
	return filepath.Join(basePath, filepath.FromSlash(relOther))
}

// CheckFileDirectoryWritable checks if the directory containing a file path is writable.
func CheckFileDirectoryWritable(filePath string, fileType string) error {
	if filePath == "" {
		return nil // Empty path is valid for some config options (like log file)
	}

	// Get the directory part of the file path
	dir := filepath.Dir(filePath)
	if dir == "" || dir == "." {
		dir = "./" // current directory
	}

	if err := CheckDirectoryWritable(dir); err != nil {
		return fmt.Errorf("%s file directory check failed: %w", fileType, err)
	}

	return nil
}
