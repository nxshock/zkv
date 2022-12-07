package zkv

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/klauspost/compress/zstd"
)

type Store struct {
	dataOffset map[string]int64

	filePath string
	offset   int64

	buffer           *bytes.Buffer
	bufferDataOffset map[string]int64

	options Options

	readOrderChan chan struct{}

	mu sync.RWMutex
}

func OpenWithOptions(filePath string, options Options) (*Store, error) {
	options.setDefaults()

	database := &Store{
		dataOffset:       make(map[string]int64),
		bufferDataOffset: make(map[string]int64),
		offset:           0,
		buffer:           new(bytes.Buffer),
		filePath:         filePath,
		options:          options,
		readOrderChan:    make(chan struct{}, int(options.MaxParallelReads))}

	// restore file data
	readF, err := os.Open(filePath)
	if os.IsNotExist(err) {
		// Empty datebase
		return database, nil
	} else if err != nil {
		return nil, fmt.Errorf("open file for indexing: %v", err)
	}
	defer readF.Close()

	decompressor, err := zstd.NewReader(readF)
	if err != nil {
		return nil, fmt.Errorf("decompressor initialization: %v", err)
	}
	defer decompressor.Close()

	offset := int64(0)
	for {
		n, record, err := readRecord(decompressor)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read record error: %v", err)
		}

		switch record.Type {
		case RecordTypeSet:
			database.dataOffset[string(record.KeyHash[:])] = offset
		case RecordTypeDelete:
			delete(database.dataOffset, string(record.KeyHash[:]))
		}

		offset += n
	}

	return database, nil
}

func Open(filePath string) (*Store, error) {
	return OpenWithOptions(filePath, defaultOptions)
}

func (s *Store) Set(key, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.set(key, value)
}

func (s *Store) Get(key, value interface{}) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.get(key, value)
}

func (s *Store) Delete(key interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyHash, err := hashInterface(key)
	if err != nil {
		return err
	}

	record := &Record{
		Type:    RecordTypeDelete,
		KeyHash: keyHash,
	}

	b, err := record.Marshal()
	if err != nil {
		return err
	}

	delete(s.dataOffset, string(record.KeyHash[:]))
	delete(s.bufferDataOffset, string(record.KeyHash[:]))

	_, err = s.buffer.Write(b)
	if err != nil {
		return err
	}

	if s.buffer.Len() > s.options.BufferSize {
		err = s.flush()

		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.flush()
}

func (s *Store) BackupWithOptions(filePath string, newFileOptions Options) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.flush()
	if err != nil {
		return err
	}

	newStore, err := OpenWithOptions(filePath, newFileOptions)
	if err != nil {
		return err
	}

	for keyHashStr := range s.dataOffset {
		var keyHash [sha256.Size224]byte
		copy(keyHash[:], keyHashStr)

		valueBytes, err := s.getGobBytes(keyHash)
		if err != nil {
			newStore.Close()
			return err
		}
		err = newStore.setBytes(keyHash, valueBytes)
		if err != nil {
			newStore.Close()
			return err
		}
	}

	return newStore.Close()
}

func (s *Store) Backup(filePath string) error {
	return s.BackupWithOptions(filePath, defaultOptions)
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.flush()
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) setBytes(keyHash [sha256.Size224]byte, valueBytes []byte) error {
	record, err := newRecordBytes(RecordTypeSet, keyHash, valueBytes)
	if err != nil {
		return err
	}

	b, err := record.Marshal()
	if err != nil {
		return err
	}

	s.bufferDataOffset[string(record.KeyHash[:])] = int64(s.buffer.Len())

	_, err = s.buffer.Write(b)
	if err != nil {
		return err
	}

	if s.buffer.Len() > s.options.BufferSize {
		err = s.flush()

		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) set(key, value interface{}) error {
	record, err := newRecord(RecordTypeSet, key, value)
	if err != nil {
		return err
	}

	b, err := record.Marshal()
	if err != nil {
		return err
	}

	s.bufferDataOffset[string(record.KeyHash[:])] = int64(s.buffer.Len())

	_, err = s.buffer.Write(b)
	if err != nil {
		return err
	}

	if s.buffer.Len() > s.options.BufferSize {
		err = s.flush()

		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) getGobBytes(keyHash [sha256.Size224]byte) ([]byte, error) {
	s.readOrderChan <- struct{}{}
	defer func() { <-s.readOrderChan }()

	offset, exists := s.bufferDataOffset[string(keyHash[:])]
	if exists {
		reader := bytes.NewReader(s.buffer.Bytes())

		err := skip(reader, offset)
		if err != nil {
			return nil, err
		}

		_, record, err := readRecord(reader)
		if err != nil {
			return nil, err
		}

		return record.ValueBytes, nil
	}

	offset, exists = s.dataOffset[string(keyHash[:])]
	if !exists {
		return nil, ErrNotExists
	}

	readF, err := os.Open(s.filePath)
	if err != nil {
		return nil, err
	}
	defer readF.Close()

	decompressor, err := zstd.NewReader(readF)
	if err != nil {
		return nil, err
	}
	defer decompressor.Close()

	err = skip(decompressor, offset)
	if err != nil {
		return nil, err
	}

	_, record, err := readRecord(decompressor)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(record.KeyHash[:], keyHash[:]) {
		expectedHashStr := base64.StdEncoding.EncodeToString(keyHash[:])
		gotHashStr := base64.StdEncoding.EncodeToString(record.KeyHash[:])
		return nil, fmt.Errorf("wrong hash of offset %d: expected %s, got %s", offset, expectedHashStr, gotHashStr)
	}

	return record.ValueBytes, nil

}

func (s *Store) get(key, value interface{}) error {
	s.readOrderChan <- struct{}{}
	defer func() { <-s.readOrderChan }()

	hashToFind, err := hashInterface(key)
	if err != nil {
		return err
	}

	offset, exists := s.bufferDataOffset[string(hashToFind[:])]
	if exists {
		reader := bytes.NewReader(s.buffer.Bytes())

		err = skip(reader, offset)
		if err != nil {
			return err
		}

		_, record, err := readRecord(reader)
		if err != nil {
			return err
		}

		return decode(record.ValueBytes, value)
	}

	offset, exists = s.dataOffset[string(hashToFind[:])]
	if !exists {
		return ErrNotExists
	}

	readF, err := os.Open(s.filePath)
	if err != nil {
		return err
	}
	defer readF.Close()

	decompressor, err := zstd.NewReader(readF)
	if err != nil {
		return err
	}
	defer decompressor.Close()

	err = skip(decompressor, offset)
	if err != nil {
		return err
	}

	_, record, err := readRecord(decompressor)
	if err != nil {
		return err
	}

	if !bytes.Equal(record.KeyHash[:], hashToFind[:]) {
		expectedHashStr := base64.StdEncoding.EncodeToString(hashToFind[:])
		gotHashStr := base64.StdEncoding.EncodeToString(record.KeyHash[:])
		return fmt.Errorf("wrong hash of offset %d: expected %s, got %s", offset, expectedHashStr, gotHashStr)
	}

	return decode(record.ValueBytes, value)
}

func (s *Store) flush() error {
	l := int64(s.buffer.Len())

	f, err := os.OpenFile(s.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open store file: %v", err)
	}

	encoder, err := zstd.NewWriter(f, zstd.WithEncoderLevel(s.options.CompressionLevel))
	if err != nil {
		f.Close()
		return fmt.Errorf("open store file: %v", err)
	}

	_, err = s.buffer.WriteTo(encoder)
	if err != nil {
		return err
	}

	for key, val := range s.bufferDataOffset {
		s.dataOffset[key] = val + s.offset
	}

	s.bufferDataOffset = make(map[string]int64)

	s.offset += l

	err = encoder.Close()
	if err != nil {
		// TODO: truncate file to previous state
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	return nil
}
