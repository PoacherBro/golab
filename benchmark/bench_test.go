package benchmark_test

import (
	"testing"

	"github.com/PoacherBro/go-lab/benchmark"
)

var buf = []byte("skdjadialsdgasadasdhsakdjsahlskdjagloqweiqwo")

func BenchmarkEncodeString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmark.EncodeString(buf)
	}
}

func BenchmarkEncodeToString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmark.EncodeToString(buf)
	}
}
