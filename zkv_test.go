package zkv

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

	assert.Len(t, db.dataOffset, recordCount)

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

	assert.Len(t, db.dataOffset, recordCount)

	err = db.Delete(50)
	assert.NoError(t, err)

	assert.Len(t, db.dataOffset, recordCount-1)
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
	value = 0
	err = db.Get(50, &value)
	assert.Equal(t, 0, value)
	assert.ErrorIs(t, err, ErrNotExists)

	err = db.Close()
	assert.NoError(t, err)
}
