package zkv

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"io/ioutil"
)

func encode(value interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(value)
	return buf.Bytes(), err
}

func decode(b []byte, value interface{}) error {
	return json.NewDecoder(bytes.NewReader(b)).Decode(&value)
}

func hashInterface(value interface{}) ([]byte, error) {
	valueBytes, err := encode(value)
	if err != nil {
		return nil, err
	}

	return hashBytes(valueBytes), nil
}

func hashBytes(b []byte) []byte {
	bytes := sha256.Sum224(b)

	return bytes[:]

}

func skip(r io.Reader, count int64) (err error) {
	switch r := r.(type) {
	case io.Seeker:
		_, err = r.Seek(count, io.SeekCurrent)
	default:
		_, err = io.CopyN(ioutil.Discard, r, count)
	}

	return err
}
