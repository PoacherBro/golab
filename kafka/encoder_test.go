package kafka_test

import (
	"log"
	"testing"

	"github.com/PoacherBro/golab/kafka"
)

type testStruct struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	test  string
}

func TestJSONEncoder(t *testing.T) {
	data := &testStruct{
		Key:   "testKey",
		Value: "testValue",
	}
	encoder := kafka.JSONEncoder()
	b, err := encoder.Encode(data)
	if err != nil {
		t.Error(err)
	}
	log.Println(b)

	i := &testStruct{}
	err = encoder.Decode(b, i)
	if err != nil {
		t.Error(err)
	}
	log.Println(i)
}

type testStruct1 struct {
	Key    string
	Value1 string
	test   string
}

func TestGobEncoder(t *testing.T) {
	data := &testStruct1{
		Key:    "gobTestKey",
		Value1: "gobTestValue",
		test:   "novisiable", // cannot be decode
	}
	encoder := kafka.GobEncoder()
	b, err := encoder.Encode(data)
	if err != nil {
		t.Error(err)
	}
	log.Println(b)

	v := &testStruct{}
	err = encoder.Decode(b, v)
	if err != nil {
		t.Error(err)
	}
	log.Println(v)

	v1 := &testStruct1{}
	err = encoder.Decode(b, v1)
	if err != nil {
		t.Error(err)
	}
	log.Println(v1)
}
