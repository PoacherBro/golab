module github.com/PoacherBro/golab

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DataDog/zstd v1.3.5 // indirect
	github.com/Shopify/sarama v1.20.0
	github.com/Shopify/toxiproxy v2.1.4+incompatible // indirect
	github.com/bsm/sarama-cluster v2.1.15+incompatible
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.1.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/onsi/ginkgo v1.8.0 // indirect
	github.com/onsi/gomega v1.5.0 // indirect
	github.com/pierrec/lz4 v0.0.0-20181005164709-635575b42742 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a // indirect
	github.com/sergi/go-diff v1.0.0
)

replace (
	golang.org/x/net v0.0.0-20180906233101-161cd47e91fd => github.com/golang/net v0.0.0-20180906233101-161cd47e91fd
	golang.org/x/sync v0.0.0-20180314180146-1d60e4601c6f => github.com/golang/sync v0.0.0-20180314180146-1d60e4601c6f
	golang.org/x/sys v0.0.0-20180909124046-d0be0721c37e => github.com/golang/sys v0.0.0-20180909124046-d0be0721c37e
	golang.org/x/text v0.3.0 => github.com/golang/text v0.3.0
)
