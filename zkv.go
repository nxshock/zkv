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

type Database struct {
	dataOffset map[string]int64
	file       *os.File
	compressor *zstd.Encoder
	filePath   string
	offset     int64

	options Options

	readOrderChan chan struct{}

	mu sync.RWMutex
}

func (db *Database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	err := db.compressor.Close()
	if err != nil {
		return err
	}

	return db.file.Close()
}

func (db *Database) Set(key, value interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	record, err := newRecord(RecordTypeSet, key, value)
	if err != nil {
		return err
	}

	b, err := record.Marshal()
	if err != nil {
		return err
	}

	db.dataOffset[string(record.KeyHash[:])] = db.offset // TODO: удалить хеш и откатить запись в случае ошибки

	_, err = db.compressor.Write(b)
	if err != nil {
		return err
	}

	db.offset += int64(len(b)) // TODO: удалить хеш и откатить запись в случае ошибки

	return nil
}

func (db *Database) Get(key, value interface{}) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.readOrderChan <- struct{}{}
	defer func() { <-db.readOrderChan }()

	hashToFind, err := hashInterface(key)
	if err != nil {
		return err
	}

	offset, exists := db.dataOffset[string(hashToFind[:])]
	if !exists {
		return ErrNotExists
	}

	readF, err := os.Open(db.filePath)
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

func OpenWithOptions(filePath string, options Options) (*Database, error) {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("ошибка при открытии файла для записи: %v", err)
	}

	if options.Validate() != nil {
		return nil, err
	}

	compressor, err := zstd.NewWriter(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("ошибка при инициализации компрессора: %v", err)
	}

	database := &Database{
		dataOffset:    make(map[string]int64),
		offset:        0,
		file:          f,
		compressor:    compressor,
		filePath:      filePath,
		options:       options,
		readOrderChan: make(chan struct{}, int(options.MaxParallelReads))}

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

func Open(filePath string) (*Database, error) {
	return OpenWithOptions(filePath, defaultOptions)
}

func (db *Database) Delete(key interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

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

	delete(db.dataOffset, string(record.KeyHash[:]))

	_, err = db.compressor.Write(b)
	if err != nil {
		return err
	}

	db.offset += int64(len(b))

	return nil
}
