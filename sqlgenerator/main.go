package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
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
	tableName             = "test"
	sqlTemplate           = "INSERT INTO `%s` (id, type) VALUES "
	defaultOutputFileName = "output.sql"
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
		log.Fatalln("No config file specified")
		return
	}
	if len(*dataFile) == 0 {
		log.Fatalln("No source data file specified")
		return
	}

	cfg := new(config)
	if _, err := toml.DecodeFile(*configFile, cfg); err != nil {
		log.Fatalf("Parse config file error, %v\n", err)
		return
	}

	g, err := newGenerator(*dataFile, *outputFile, cfg)
	if err != nil {
		return
	}
	go func() {
		log.Printf("got singal : %v", <-onExit())
		g.Stop()
	}()

	g.Start()
	g.Wait()
	log.Println("Generator done!")
	time.Sleep(1 * time.Millisecond) // To print all log before exist
}

type generator struct {
	sourceDataFile string
	outputSQLFile  string
	cfg            *config
	maxFileSize    int64
	wg             sync.WaitGroup
	context        context.Context
	cancel         context.CancelFunc
	queue          chan []int64
	closed         bool
	totalWrite     int64
	totalRead      int64
	outputFile     *os.File
	fileNum        int
	fileSize       int64
}

func newGenerator(dataFile, outputFile string, cfg *config) (*generator, error) {
	if len(outputFile) == 0 {
		outputFile = fmt.Sprintf("%s.sql", defaultOutputFileName)
	}

	maxFileSize := cfg.FileOptions.MaxFileSize * 1024 * 1024
	num := 0
	file, err := newFile(outputFile, num)
	if err != nil {
		log.Printf("cannot create output file [%s], err: %v\n", outputFile, err)
		return nil, err
	}

	ctx, c := context.WithCancel(context.Background())
	return &generator{
		sourceDataFile: dataFile,
		outputSQLFile:  outputFile,
		cfg:            cfg,
		context:        ctx,
		cancel:         c,
		queue:          make(chan []int64, 256),
		closed:         false,
		maxFileSize:    maxFileSize,
		fileNum:        num,
		outputFile:     file,
		fileSize:       int64(0),
		totalWrite:     int64(0),
		totalRead:      int64(0),
	}, nil
}

func (g *generator) Start() {
	go g.report()
	go g.sourceDataReader()

	g.wg.Add(1)
	go g.sqlResultWriter()

	log.Println("SQL generator start...")
}

func (g *generator) Stop() {
	g.closed = true
	close(g.queue)
	g.cancel()
}

func (g *generator) Wait() {
	start := time.Now()
	g.wg.Wait()
	if !g.closed {
		g.closed = true
		close(g.queue)
		g.cancel()
	}

	duration := float64(time.Since(start).Nanoseconds() / 1000000 / 1000)
	log.Printf("Finished %d lines with time %.2fs\n", g.totalWrite, duration)
}

func (g *generator) sourceDataReader() {
	file, err := os.Open(*dataFile)
	if err != nil {
		log.Printf("Open file [%s] with err: %v\n", *dataFile, err)
		return
	}
	defer file.Close()

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
			log.Printf("Unexpected source data: %s, err: %v\n", line, err)
			return
		}
		g.totalRead++
		lines = append(lines, i)
		if int64(len(lines)) >= batchCount {
			select {
			case <-g.context.Done():
				return
			case g.queue <- lines:
				lines = lines[:0]
			}
		}
	}
	if len(lines) != 0 {
		g.queue <- lines
	}
	log.Printf("Reader is done, total: %d\n", g.totalRead)
}

func (g *generator) sqlResultWriter() {
	defer func() {
		log.Printf("Writer is done, total: %d\n", g.totalWrite)
		g.wg.Done()
	}()
	for {
		select {
		case <-g.context.Done():
			return
		case data := <-g.queue:
			g.totalWrite += int64(len(data))
			sql := generateSQL(data)
			err := g.write(sql)
			if err != nil {
				g.closed = true // exist
				return
			}
			if g.totalWrite == g.totalRead {
				return
			}
		}
	}
}

func (g *generator) write(sql string) error {
	statement := fmt.Sprintf("%s\n", sql)
	data := []byte(statement)
	size := int64(len(data))
	if size+g.fileSize > g.maxFileSize {
		if err := g.outputFile.Close(); err != nil {
			log.Printf("close file[%s] fail\n", g.outputFile.Name())
			return err
		}

		g.fileNum++
		f, err := newFile(g.outputSQLFile, g.fileNum)
		if err != nil {
			log.Printf("new file [%s - %d] fail (%v)\n", g.outputSQLFile, g.fileNum, err)
			return err
		}
		g.outputFile = f
		g.fileSize = int64(0)
	}
	b, err := g.outputFile.Write(data)
	if err != nil {
		log.Printf("write the file fail, %v\n", err)
		return err
	}
	g.fileSize += int64(b)
	// log.Info("write SQL %d bytes\n", b)
	// g.file.Sync()
	return nil
}

func (g *generator) report() {
	for {
		select {
		case <-g.context.Done():
			return
		case <-time.After(1 * time.Second):
			log.Printf("Processed %d lines\n", g.totalWrite)
		}
	}
}

func generateSQL(data []int64) string {
	length := len(data)
	if length == 0 {
		return ""
	}
	sqlBuffer := bytes.NewBufferString(sqlTemplate)
	for i, d := range data {
		sqlBuffer.WriteString(fmt.Sprintf("(%d, 2)", d))
		if i != length-1 {
			sqlBuffer.WriteString(",")
		} else {
			sqlBuffer.WriteString(";")
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
		fileName = fmt.Sprintf("%s_%d.%s", names[0], count, ext)
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
