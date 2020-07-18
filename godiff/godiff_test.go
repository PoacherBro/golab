package test

import (
	"fmt"
	"testing"

	"github.com/sergi/go-diff/diffmatchpatch"
)

func TestTextDiff1k(t *testing.T) {
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain("abc", "adb", false)
	fmt.Println(dmp.DiffPrettyText(diffs))
}