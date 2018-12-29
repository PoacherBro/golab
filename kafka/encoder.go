package kafka

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

// Encoder define a value encode rule
type Encoder interface {
	Encode(v interface{}) ([]byte, error)
	Decode(data []byte, v interface{}) error
}

var (
	gJSONEncoder = new(jsonEncoder)
	gGobEncoder  = new(gobEncoder)
)

// JSONEncoder implement Encoder interface, for JSON format encode/decode
func JSONEncoder() Encoder {
	return gJSONEncoder
}

// GobEncoder implement Encoder interface, for gob format
func GobEncoder() Encoder {
	return gGobEncoder
}

type jsonEncoder struct{}

func (e *jsonEncoder) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (e *jsonEncoder) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

type gobEncoder struct{}

func (e *gobEncoder) Encode(v interface{}) ([]byte, error) {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func (e *gobEncoder) Decode(data []byte, v interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	return dec.Decode(v)
}
