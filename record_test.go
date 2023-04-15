package zkv

import (
	"bytes"
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
