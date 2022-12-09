# zkv

Simple key-value store for single-user applications.

## Pros

* Simple one file structure
* Internal Zstandard compression by [klauspost/compress/zstd](https://github.com/klauspost/compress/tree/master/zstd)
* Threadsafe operations through `sync.RWMutex`

## Cons

* Index stored in memory (`map[key hash (28 bytes)]file offset (int64)`)
* Need to read the whole file on store open to create file index
* No way to recover disk space from deleted records
* Write/Delete operations block Read and each other operations

## Usage

Create or open existing file:

```go
db, err := zkv.Open("path to file")
```

Data operations:

```go
// Write data
err = db.Set(key, value) // key and value can be any of type

// Read data
var value ValueType
err = db.Get(key, &value)

// Delete data
err = db.Delete(key)
```

Other methods:

```go
// Flush data to disk
err = db.Flush()

// Backup data to another file
err = db.Backup("new/file/path")
```

## File structure

Record is `encoding/gob` structure:

| Field      | Description                        | Size     |
| ---------- | ---------------------------------- | -------- |
| Type       | Record type                        | uint8    |
| KeyHash    | Key hash                           | 28 bytes |
| ValueBytes | Value gob-encoded bytes            | variable |

File is log stuctured list of commands:

| Field  | Description              | Size     |
| -------| ------------------------ | -------- |
| Length | Record body bytes length | int64    |
| Body   | Gob-encoded record       | variable |

## TODO

- [ ] Test [seekable zstd streams](https://github.com/SaveTheRbtz/zstd-seekable-format-go)
- [ ] Implement optional separate index file to speedup store initialization
- [ ] Add recovery previous state of store file on write error
