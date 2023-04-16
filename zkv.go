package zkv

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/klauspost/compress/zstd"
)

type Offsets struct {
	BlockOffset  int64
	RecordOffset int64
}

type Store struct {
	dataOffset map[string]Offsets

	filePath string

	buffer           *bytes.Buffer
	bufferDataOffset map[string]int64

	options Options

	readOrderChan chan struct{}

	mu sync.RWMutex
}

func OpenWithOptions(filePath string, options Options) (*Store, error) {
	options.setDefaults()

	store := &Store{
		dataOffset:       make(map[string]Offsets),
		bufferDataOffset: make(map[string]int64),
		buffer:           new(bytes.Buffer),
		filePath:         filePath,
		options:          options,
		readOrderChan:    make(chan struct{}, int(options.MaxParallelReads))}

	if options.useIndexFile {
		idxFile, err := os.Open(filePath + indexFileExt)
		if err == nil {
			err = gob.NewDecoder(idxFile).Decode(&store.dataOffset)
			if err != nil {
				return nil, err
			}

			return store, nil
		}
	}

	exists, err := isFileExists(filePath)
	if err != nil {
		return nil, err
	}

	if !exists {
		return store, nil
	}

	err = store.rebuildIndex()
	if err != nil {
		return nil, err
	}

	return store, nil
}

func Open(filePath string) (*Store, error) {
	options := defaultOptions
	return OpenWithOptions(filePath, options)
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

	if s.buffer.Len() > s.options.MemoryBufferSize {
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

	if s.buffer.Len() > s.options.MemoryBufferSize {
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

	if s.buffer.Len() > s.options.MemoryBufferSize {
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

	offsets, exists := s.dataOffset[string(keyHash[:])]
	if !exists {
		return nil, ErrNotExists
	}

	readF, err := os.Open(s.filePath)
	if err != nil {
		return nil, err
	}
	defer readF.Close()

	_, err = readF.Seek(offsets.BlockOffset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	decompressor, err := zstd.NewReader(readF)
	if err != nil {
		return nil, err
	}
	defer decompressor.Close()

	err = skip(decompressor, offsets.RecordOffset)
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

	b, err := s.getGobBytes(hashToFind)
	if err != nil {
		return err
	}

	return decode(b, value)
}

func (s *Store) flush() error {
	l := int64(s.buffer.Len())

	f, err := os.OpenFile(s.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open store file: %v", err)
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("stat store file: %v", err)
	}

	diskWriteBuffer := bufio.NewWriterSize(f, s.options.DiskBufferSize)

	encoder, err := zstd.NewWriter(diskWriteBuffer, zstd.WithEncoderLevel(s.options.CompressionLevel))
	if err != nil {
		f.Close()
		return fmt.Errorf("init encoder: %v", err)
	}

	_, err = s.buffer.WriteTo(encoder)
	if err != nil {
		return err
	}

	for key, val := range s.bufferDataOffset {
		s.dataOffset[key] = Offsets{BlockOffset: stat.Size(), RecordOffset: val}
	}

	s.bufferDataOffset = make(map[string]int64)

	err = encoder.Close()
	if err != nil {
		// TODO: truncate file to previous state
		return err
	}

	err = diskWriteBuffer.Flush()
	if err != nil {
		// TODO: truncate file to previous state
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	// Update index file only on data update
	if s.options.useIndexFile && l > 0 {
		err = s.saveIndex()
		if err != nil {
			return err
		}
	}

	return nil
}

func readBlock(r *bufio.Reader) (line []byte, n int, err error) {
	delim := []byte{0x28, 0xb5, 0x2f, 0xfd}

	line = make([]byte, len(delim))
	copy(line, delim)

	for {
		s, err := r.ReadBytes(delim[len(delim)-1])
		line = append(line, []byte(s)...)
		if err != nil {
			if bytes.Equal(line, delim) { // contains only magic number
				return []byte{}, 0, err
			} else {
				return line, len(s), err
			}
		}

		if bytes.Equal(line, append(delim, delim...)) { // first block
			line = make([]byte, len(delim))
			copy(line, delim)
			continue
		}

		if bytes.HasSuffix(line, delim) {
			return line[:len(line)-len(delim)], len(s), nil
		}
	}
}

// RebuildIndex renews index from store file
func (s *Store) RebuildIndex() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.rebuildIndex()
	if err != nil {
		return err
	}

	if s.options.useIndexFile {
		return s.saveIndex()
	}

	return nil
}

func (s *Store) rebuildIndex() error {
	f, err := os.Open(s.filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)

	var blockOffset int64

	s.dataOffset = make(map[string]Offsets)

	for {
		l, n, err := readBlock(r)
		if err != nil {
			if err != io.EOF {
				return err
			} else if err == io.EOF && len(l) == 0 {
				break
			}
		}

		dec, err := zstd.NewReader(bytes.NewReader(l))

		var recordOffset int64
		for {
			n, record, err := readRecord(dec)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					return err
				}
			}

			switch record.Type {
			case RecordTypeSet:
				s.dataOffset[string(record.KeyHash[:])] = Offsets{BlockOffset: blockOffset, RecordOffset: recordOffset}
			case RecordTypeDelete:
				delete(s.dataOffset, string(record.KeyHash[:]))
			}
			recordOffset += n
		}

		blockOffset += int64(n)
	}

	idxBuf := new(bytes.Buffer)

	err = gob.NewEncoder(idxBuf).Encode(s.dataOffset)
	if err != nil {
		return err
	}

	err = os.WriteFile(s.filePath+indexFileExt, idxBuf.Bytes(), 0644)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) saveIndex() error {
	f, err := os.OpenFile(s.filePath+indexFileExt, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	err = gob.NewEncoder(f).Encode(s.dataOffset)
	if err != nil {
		return err
	}

	return f.Close()
}
