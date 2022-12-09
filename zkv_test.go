package zkv

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecord(t *testing.T) {
	buf := new(bytes.Buffer)

	var records []Record

	for i := 0; i < 10; i++ {
		record, err := newRecord(RecordTypeSet, i, i)
		assert.NoError(t, err)

		records = append(records, *record)

		b, err := record.Marshal()
		assert.NoError(t, err)

		_, err = buf.Write(b)
		assert.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		_, record, err := readRecord(buf)
		assert.NoError(t, err)

		assert.Equal(t, record.KeyHash, records[i].KeyHash)
		assert.Equal(t, record.ValueBytes, records[i].ValueBytes)
	}
}

func TestReadWriteBasic(t *testing.T) {
	const filePath = "TestReadWriteBasic.zkv"
	const recordCount = 100
	defer os.Remove(filePath)

	db, err := Open(filePath)
	assert.NoError(t, err)

	for i := 1; i <= recordCount; i++ {
		err = db.Set(i, i)
		assert.NoError(t, err)
	}

	assert.Len(t, db.dataOffset, 0)
	assert.Len(t, db.bufferDataOffset, recordCount)

	for i := 1; i <= recordCount; i++ {
		var gotValue int

		err = db.Get(i, &gotValue)
		assert.NoError(t, err)
		assert.Equal(t, i, gotValue)
	}

	err = db.Close()
	assert.NoError(t, err)

	// try to read
	db, err = Open(filePath)
	assert.NoError(t, err)

	assert.Len(t, db.dataOffset, recordCount)

	for i := 1; i <= recordCount; i++ {
		var gotValue int

		err = db.Get(i, &gotValue)
		assert.NoError(t, err)
		assert.Equal(t, i, gotValue)
	}

	err = db.Close()
	assert.NoError(t, err)
}

func TestSmallWrites(t *testing.T) {
	const filePath = "TestSmallWrites.zkv"
	const recordCount = 100
	defer os.Remove(filePath)

	for i := 1; i <= recordCount; i++ {
		db, err := Open(filePath)
		assert.NoError(t, err)

		err = db.Set(i, i)
		assert.NoError(t, err)

		err = db.Close()
		assert.NoError(t, err)
	}

	// try to read

	db, err := Open(filePath)
	assert.NoError(t, err)

	assert.Len(t, db.dataOffset, recordCount)

	for i := 1; i <= recordCount; i++ {
		var gotValue int

		err = db.Get(i, &gotValue)
		assert.NoError(t, err)
		assert.Equal(t, i, gotValue)
	}

	err = db.Close()
	assert.NoError(t, err)
}

func TestDeleteBasic(t *testing.T) {
	const filePath = "TestDeleteBasic.zkv"
	const recordCount = 100
	defer os.Remove(filePath)

	db, err := Open(filePath)
	assert.NoError(t, err)

	for i := 1; i <= recordCount; i++ {
		err = db.Set(i, i)
		assert.NoError(t, err)
	}

	assert.Len(t, db.dataOffset, 0)
	assert.Len(t, db.bufferDataOffset, recordCount)

	err = db.Delete(50)
	assert.NoError(t, err)

	assert.Len(t, db.dataOffset, 0)
	assert.Len(t, db.bufferDataOffset, recordCount-1)

	var value int
	err = db.Get(50, &value)
	assert.Equal(t, 0, value)
	assert.ErrorIs(t, err, ErrNotExists)

	err = db.Close()
	assert.NoError(t, err)

	// try to read
	db, err = Open(filePath)
	assert.NoError(t, err)

	assert.Len(t, db.dataOffset, recordCount-1)
	assert.Len(t, db.bufferDataOffset, 0)

	value = 0
	err = db.Get(50, &value)
	assert.Equal(t, 0, value)
	assert.ErrorIs(t, err, ErrNotExists)

	err = db.Close()
	assert.NoError(t, err)
}

func TestBufferBasic(t *testing.T) {
	const filePath = "TestBuffer.zkv"
	defer os.Remove(filePath)

	db, err := OpenWithOptions(filePath, Options{MemoryBufferSize: 100})
	assert.NoError(t, err)

	err = db.Set(1, make([]byte, 100))
	assert.NoError(t, err)

	assert.NotEqual(t, 0, db.dataOffset)
	assert.Len(t, db.bufferDataOffset, 0)
	assert.Equal(t, 0, db.buffer.Len())

	var gotValue []byte
	err = db.Get(1, &gotValue)
	assert.NoError(t, err)

	assert.Equal(t, make([]byte, 100), gotValue)

	err = db.Close()
	assert.NoError(t, err)
}

func TestBufferRead(t *testing.T) {
	const filePath = "TestBufferRead.zkv"
	const recordCount = 100
	defer os.Remove(filePath)

	db, err := OpenWithOptions(filePath, Options{MemoryBufferSize: 100})
	assert.NoError(t, err)

	for i := 1; i <= recordCount; i++ {
		err = db.Set(i, i)
		assert.NoError(t, err)
	}

	for i := 1; i <= recordCount; i++ {
		var gotValue int

		err = db.Get(i, &gotValue)
		assert.NoError(t, err)
		assert.Equal(t, i, gotValue)
	}

	for i := 1; i <= recordCount; i++ {
		var gotValue int

		err = db.Get(i, &gotValue)
		assert.NoError(t, err)
		assert.Equal(t, i, gotValue)
	}

	err = db.Close()
	assert.NoError(t, err)

	// try to read
	db, err = Open(filePath)
	assert.NoError(t, err)

	assert.Len(t, db.dataOffset, recordCount)

	for i := 1; i <= recordCount; i++ {
		var gotValue int

		err = db.Get(i, &gotValue)
		assert.NoError(t, err)
		assert.Equal(t, i, gotValue)
	}

	err = db.Close()
	assert.NoError(t, err)

}

func TestBackupBasic(t *testing.T) {
	const filePath = "TestBackupBasic.zkv"
	const newFilePath = "TestBackupBasic2.zkv"
	const recordCount = 100
	defer os.Remove(filePath)
	defer os.Remove(newFilePath)

	db, err := Open(filePath)
	assert.NoError(t, err)

	for i := 1; i <= recordCount; i++ {
		err = db.Set(i, i)
		assert.NoError(t, err)
	}

	err = db.Backup(newFilePath)
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	db, err = Open(newFilePath)
	assert.NoError(t, err)

	assert.Len(t, db.dataOffset, recordCount)

	for i := 1; i <= recordCount; i++ {
		var gotValue int

		err = db.Get(i, &gotValue)
		assert.NoError(t, err)
		assert.Equal(t, i, gotValue)
	}

	err = db.Close()
	assert.NoError(t, err)

}

func TestBackupWithDeletedRecords(t *testing.T) {
	const filePath = "TestBackupWithDeletedRecords.zkv"
	const newFilePath = "TestBackupWithDeletedRecords2.zkv"
	const recordCount = 100
	defer os.Remove(filePath)
	defer os.Remove(newFilePath)

	db, err := Open(filePath)
	assert.NoError(t, err)

	for i := 1; i <= recordCount; i++ {
		err = db.Set(i, i)
		assert.NoError(t, err)
	}

	err = db.Flush()
	assert.NoError(t, err)

	for i := 1; i <= recordCount; i++ {
		if i%2 == 1 {
			continue
		}

		err = db.Delete(i)
		assert.NoError(t, err)
	}

	err = db.Backup(newFilePath)
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	db, err = Open(newFilePath)
	assert.NoError(t, err)

	assert.Len(t, db.dataOffset, recordCount/2)

	for i := 1; i <= recordCount; i++ {
		var gotValue int

		err = db.Get(i, &gotValue)
		if i%2 == 0 {
			assert.ErrorIs(t, err, ErrNotExists)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, i, gotValue)
		}
	}

	err = db.Close()
	assert.NoError(t, err)

}
