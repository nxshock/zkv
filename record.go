package zkv

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"io"
)

type RecordType uint8

const (
	RecordTypeSet RecordType = iota + 1
	RecordTypeDelete
)

type Record struct {
	Type       RecordType
	KeyHash    [28]byte
	ValueBytes []byte
}

func newRecordBytes(recordType RecordType, keyHash [sha256.Size224]byte, valueBytes []byte) (*Record, error) {
	record := &Record{
		Type:       recordType,
		KeyHash:    keyHash,
		ValueBytes: valueBytes}

	return record, nil
}

func newRecord(recordType RecordType, key, value interface{}) (*Record, error) {
	keyHash, err := hashInterface(key)
	if err != nil {
		return nil, err
	}

	valueBytes, err := encode(value)
	if err != nil {
		return nil, err
	}

	return newRecordBytes(recordType, keyHash, valueBytes)
}

func (r *Record) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	err := gob.NewEncoder(buf).Encode(r)
	if err != nil {
		return nil, err
	}

	buf2 := new(bytes.Buffer)

	err = binary.Write(buf2, binary.LittleEndian, int64(buf.Len()))
	if err != nil {
		return nil, err
	}

	return append(buf2.Bytes(), buf.Bytes()...), nil
}

func readRecord(r io.Reader) (n int64, record *Record, err error) {
	var recordBytesLen int64
	err = binary.Read(r, binary.LittleEndian, &recordBytesLen)
	if err != nil {
		return 0, nil, err // TODO: вместо нуля должно быть реальное кол-во считанных байт
	}

	recordBytes := make([]byte, int(recordBytesLen))

	_, err = io.ReadAtLeast(r, recordBytes, int(recordBytesLen))
	if err != nil {
		return 0, nil, err // TODO: вместо нуля должно быть реальное кол-во считанных байт
	}

	err = gob.NewDecoder(bytes.NewReader(recordBytes)).Decode(&record)
	if err != nil {
		return 0, nil, err // TODO: вместо нуля должно быть реальное кол-во считанных байт
	}

	return recordBytesLen + 8, record, nil
}
