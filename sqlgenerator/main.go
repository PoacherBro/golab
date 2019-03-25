package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
)

var (
	configFile = flag.String("c", "", "specify the config file path, example: `./etc/generate_sql.toml`")
	dataFile   = flag.String("data", "", "source data path, example: `./data.txt`")
	outputFile = flag.String("o", "", "output SQL file name, example: `file.sql`")
)

const (
	tableName   = "test"
	sqlTemplate = "INSERT INTO `%s` (id, type) VALUES "
)

type config struct {
	FileOptions *fileOptions `toml:"file_options"`
}

type fileOptions struct {
	BatchCount  int64 `toml:"batch_count"`
	MaxFileSize int64 `toml:"max_file_size"`
}

func main() {
	flag.Parse()
	if len(*configFile) == 0 {
		fmt.Println(fmt.Errorf("No config file specified"))
		return
	}
	if len(*dataFile) == 0 {
		fmt.Println(fmt.Errorf("No source data file specified"))
		return
	}

	cfgData, err := getTomlData(*configFile)
	if err != nil {
		return
	}

	cfg := new(config)
	if _, err := toml.Decode(cfgData, cfg); err != nil {
		fmt.Println(fmt.Errorf("Parse config file error, %v", err))
		return
	}

	g := newGenerator(*dataFile, *outputFile, cfg)
	go func() {
		fmt.Println(fmt.Sprintf("got singal : %v", <-onExit()))
		g.Stop()
	}()
	g.Start()
}

type generator struct {
	sourceDataFile string
	outputSQLFile  string
	cfg            *config
	wg             sync.WaitGroup
	context        context.Context
	cancel         context.CancelFunc
	queue          chan []int64
	closed         bool
}

func newGenerator(dataFile, outputFile string, cfg *config) *generator {
	ctx, c := context.WithCancel(context.Background())
	if len(outputFile) == 0 {
		outputFile = fmt.Sprintf("%s.sql", time.Now().Format("20060102150405"))
	}
	return &generator{
		sourceDataFile: dataFile,
		outputSQLFile:  outputFile,
		cfg:            cfg,
		context:        ctx,
		cancel:         c,
		queue:          make(chan []int64, 256),
		closed:         false,
	}
}

func (g *generator) Start() {
	go g.sourceDataReader()
	go g.sqlResultWriter()
	fmt.Println("SQL generator start...")
}

func (g *generator) Stop() {
	g.closed = true
	g.cancel()
}

func (g *generator) sourceDataReader() {
	g.wg.Add(1)
	file, err := os.Open(*dataFile)
	if err != nil {
		fmt.Println(fmt.Errorf("Open file [%s] with err: %v", *dataFile, err))
		return
	}
	defer func() {
		file.Close()
		close(g.queue)
		g.wg.Done()
	}()

	s := bufio.NewScanner(file)
	s.Split(bufio.ScanLines)

	batchCount := g.cfg.FileOptions.BatchCount
	lines := make([]int64, 0, batchCount)
	for s.Scan() {
		if g.closed {
			return
		}

		line := s.Text()
		i, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			fmt.Println(fmt.Errorf("Unexpected source data: %s, err: %v", line, err))
			return
		}

		lines = append(lines, i)
		if int64(len(lines)) == batchCount {
			g.queue <- lines
			lines = lines[:]
		}
	}
}

func (g *generator) sqlResultWriter() {
	of, err := os.Create(g.outputSQLFile)
	if err != nil {
		fmt.Println(fmt.Errorf("cannot create output file [%s], err: %v", g.outputSQLFile, err))
		g.closed = true
		// consume all data from chan
		for range g.queue {
		}
		return
	}
	defer of.Close()

	w := bufio.NewWriter(of)
	for data := range g.queue {
		sql := generateSQL(data)
		b, err := w.WriteString(fmt.Sprintf("%s\n", sql))
		if err != nil {
			fmt.Println(fmt.Errorf("write the file fail, %v", err))
			return
		}
		fmt.Printf("write SQL %d bytes\n", b)
		w.Flush()
	}
}

func generateSQL(data []int64) string {
	length := len(data)
	if length == 0 {
		return ""
	}
	sqlBuffer := bytes.NewBufferString(fmt.Sprintf(sqlTemplate, tableName))
	for i, d := range data {
		sqlBuffer.WriteString(fmt.Sprintf("(%d, 2)", d))
		if i != length-1 {
			sqlBuffer.WriteString(",")
		}
	}
	return sqlBuffer.String()
}

func newFile(name string, count int) (*os.File, error) {
	names := strings.Split(name, ".")
	if len(names) != 2 {
		return nil, fmt.Errorf("%s is invalid name without file ext", name)
	}
	fileName := name
	ext := names[1]
	if count > 0 {
		fileName = fmt.Sprintf("%s(%d).%s", names[0], count, ext)
	}
	return os.Create(fileName)
}

func onExit() <-chan os.Signal {
	news := make(chan os.Signal, 1)
	signal.Notify(news,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	return news
}

func getTomlData(filePath string) (string, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	return string(data), nil
}
