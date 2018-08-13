package benchmark

import (
	"encoding/hex"
	"fmt"
)

// EncodeString use "fmt.Sprintf"
func EncodeString(b []byte) string {
	return fmt.Sprintf("%x", b)
}

// EncodeToString use "hex.EncodeToString"
func EncodeToString(b []byte) string {
	return hex.EncodeToString(b)
}
