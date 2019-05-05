package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

// ReplaceKV 元数据
type ReplaceKV struct {
	Key   string
	Value string
}

var (
	kvFilePath      = flag.String("kvfile", "", "the k-v definition file path")
	replaceFilePath = flag.String("dst", "", "destination file which should be replaced")
)

func main() {
	flag.Parse()
	if len(*kvFilePath) == 0 {
		fmt.Println("No 'kvfile' passed")
		return
	}
	if len(*replaceFilePath) == 0 {
		fmt.Println("No 'dst' passed")
		return
	}

	kvs, err := parseKVFile()
	if err != nil {
		fmt.Println(err)
	}
	replace(kvs)
}

func parseKVFile() ([]*ReplaceKV, error) {
	file, err := os.Open(*kvFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	s := bufio.NewScanner(file)
	s.Split(bufio.ScanLines)
	kvs := make([]*ReplaceKV, 0)
	for s.Scan() {
		line := s.Text()
		kvArr := strings.Split(line, ";")
		if len(kvArr) != 2 {
			fmt.Println("Split line is not correct, line=", line)
			continue
		}
		kvs = append(kvs, &ReplaceKV{
			Key:   kvArr[0],
			Value: kvArr[1],
		})
	}
	return kvs, nil
}

func replace(kvs []*ReplaceKV) {
	data, err := ioutil.ReadFile(*replaceFilePath)
	if err != nil {
		fmt.Println(err)
		return
	}
	content := string(data)
	for _, kv := range kvs {
		content = strings.ReplaceAll(content, kv.Key, kv.Value)
	}
	err = ioutil.WriteFile("output.txt", []byte(content), 0777)
	if err != nil {
		fmt.Println(err)
	}
}
