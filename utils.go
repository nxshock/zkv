package zkv

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"io"
	"os"
)

func encode(value interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(value)
	return buf.Bytes(), err
}

func decode(b []byte, value interface{}) error {
	return gob.NewDecoder(bytes.NewReader(b)).Decode(value)
}

func hashInterface(value interface{}) ([sha256.Size224]byte, error) {
	valueBytes, err := encode(value)
	if err != nil {
		return [sha256.Size224]byte{}, err
	}

	return hashBytes(valueBytes), nil
}

func hashBytes(b []byte) [sha256.Size224]byte {
	return sha256.Sum224(b)
}

func skip(r io.Reader, count int64) (err error) {
	switch r := r.(type) {
	case io.Seeker:
		_, err = r.Seek(count, io.SeekCurrent)
	default:
		_, err = io.CopyN(io.Discard, r, count)
	}

	return err
}

func isFileExists(filePath string) (bool, error) {
	if _, err := os.Stat(filePath); err == nil {
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else {
		return false, err
	}
}
