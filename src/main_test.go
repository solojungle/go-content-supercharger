package main

import (
	"testing"
	"strings"
)

func TestNewJob(t *testing.T) {
	// Directory doesn't exist
	_, err := NewJob("testfiles/non_existant_directory");
	if !ErrorContains(err, "no such file or directory") {
		t.Errorf("unexpected error: %v", err)
	}

	// Passing non-directory
	_, err = NewJob("testfiles/a.txt");
	if !ErrorContains(err, "is not a directory") {
		t.Errorf("unexpected error: %v", err)
	}

	// Passing empty directory
	_, err = NewJob("testfiles/emptyDir");
	if !ErrorContains(err, "directory is empty") {
		t.Errorf("unexpected error: %v", err)
	}

	// Passing directory
	_, err = NewJob("testfiles");
	if !ErrorContains(err, "") {
		t.Errorf("unexpected error: %v", err)
	}

}

// ErrorContains checks if the error message in out contains the text in
// want.
//
// This is safe when out is nil. Use an empty string for want if you want to
// test that err is nil.
func ErrorContains(out error, want string) bool {
    if out == nil {
        return want == ""
    }
    if want == "" {
        return false
    }
    return strings.Contains(out.Error(), want)
}