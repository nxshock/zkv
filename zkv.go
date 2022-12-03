package zkv

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/klauspost/compress/zstd"
)

type Store struct {
	dataOffset map[string]int64

	file     *os.File
	filePath string
	offset   int64
	encoder  *zstd.Encoder

	buffer           *bytes.Buffer
	bufferDataOffset map[string]int64

	options Options

	readOrderChan chan struct{}

	mu sync.RWMutex
}

func OpenWithOptions(filePath string, options Options) (*Store, error) {
	options.setDefaults()

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("ошибка при открытии файла для записи: %v", err)
	}

	compressor, err := zstd.NewWriter(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("ошибка при инициализации компрессора: %v", err)
	}

	database := &Store{
		dataOffset:       make(map[string]int64),
		bufferDataOffset: make(map[string]int64),
		offset:           0,
		file:             f,
		encoder:          compressor,
		buffer:           new(bytes.Buffer),
		filePath:         filePath,
		options:          options,
		readOrderChan:    make(chan struct{}, int(options.MaxParallelReads))}

	// restore file data
	readF, err := os.Open(filePath)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("ошибка при открытии файла для чтения: %v", err)
	}
	defer readF.Close()

	decompressor, err := zstd.NewReader(readF)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("ошибка при инициализации декомпрессора: %v", err)
	}
	defer decompressor.Close()

	offset := int64(0)
	for {
		n, record, err := readRecord(decompressor)
		if err == io.EOF {
			break
		}
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("ошибка при чтении записи из файла: %v", err)
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

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.flush()
	if err != nil {
		return err
	}

	err = s.encoder.Close()
	if err != nil {
		return err
	}

	return s.file.Close()
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
		return fmt.Errorf("wrong hash on this offset: expected %s, got %s", base64.StdEncoding.EncodeToString(hashToFind[:]), base64.StdEncoding.EncodeToString(record.KeyHash[:])) // TODO: заменить на константную ошибку
	}

	return decode(record.ValueBytes, value)
}

func (s *Store) flush() error {
	l := int64(s.buffer.Len())

	_, err := s.buffer.WriteTo(s.encoder)
	if err != nil {
		return err
	}

	for key, val := range s.bufferDataOffset {
		s.dataOffset[key] = val + s.offset
	}

	s.bufferDataOffset = make(map[string]int64)

	s.offset += l

	err = s.encoder.Flush()
	if err != nil {
		return err
	}

	return nil
}
