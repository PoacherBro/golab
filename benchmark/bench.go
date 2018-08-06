package benchmark

import (
	"fmt"
	"hex"
)

func EncodeString(b []byte) string {
	return fmt.Sprintf("%x", b)
}

func EncodeToString(b []byte) string {
	return hex.EncodeToString(b)
}

func main() {
	fmt.Println("vim-go")
}
