package tsm1

/*
A TSM file is composed for four sections: header, blocks, index and the footer.

┌────────┬────────────────────────────────────┬─────────────┬──────────────┐
│ Header │               Blocks               │    Index    │    Footer    │
│5 bytes │              N bytes               │   N bytes   │   4 bytes    │
└────────┴────────────────────────────────────┴─────────────┴──────────────┘

Header is composed of a magic number to identify the file type and a version
number.

┌───────────────────┐
│      Header       │
├─────────┬─────────┤
│  Magic  │ Version │
│ 4 bytes │ 1 byte  │
└─────────┴─────────┘

Blocks are sequences of pairs of CRC32 and data.  The block data is opaque to the
file.  The CRC32 is used for block level error detection.  The length of the blocks
is stored in the index.

┌───────────────────────────────────────────────────────────┐
│                          Blocks                           │
├───────────────────┬───────────────────┬───────────────────┤
│      Block 1      │      Block 2      │      Block N      │
├─────────┬─────────┼─────────┬─────────┼─────────┬─────────┤
│  CRC    │  Data   │  CRC    │  Data   │  CRC    │  Data   │
│ 4 bytes │ N bytes │ 4 bytes │ N bytes │ 4 bytes │ N bytes │
└─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘

Following the blocks is the index for the blocks in the file.  The index is
composed of a sequence of index entries ordered lexicographically by key and
then by time.  Each index entry starts with a key length and key followed by a
count of the number of blocks in the file.  Each block entry is composed of
the min and max time for the block, the offset into the file where the block
is located and the the size of the block.

The index structure can provide efficient access to all blocks as well as the
ability to determine the cost associated with acessing a given key.  Given a key
and timestamp, we can determine whether a file contains the block for that
timestamp as well as where that block resides and how much data to read to
retrieve the block.  If we know we need to read all or multiple blocks in a
file, we can use the size to determine how much to read in a given IO.

┌────────────────────────────────────────────────────────────────────────────┐
│                                   Index                                    │
├─────────┬─────────┬──────┬───────┬─────────┬─────────┬────────┬────────┬───┤
│ Key Len │   Key   │ Type │ Count │Min Time │Max Time │ Offset │  Size  │...│
│ 2 bytes │ N bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │8 bytes │4 bytes │   │
└─────────┴─────────┴──────┴───────┴─────────┴─────────┴────────┴────────┴───┘

The last section is the footer that stores the offset of the start of the index.

┌─────────┐
│ Footer  │
├─────────┤
│Index Ofs│
│ 8 bytes │
└─────────┘
*/

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	// MagicNumber is written as the first 4 bytes of a data file to
	// identify the file as a tsm1 formatted file
	MagicNumber uint32 = 0x16D116D1

	// Version indicates the version of the TSM file format.
	Version byte = 1

	// Size in bytes of an index entry
	indexEntrySize = 28

	// Size in bytes used to store the count of index entries for a key
	indexCountSize = 2

	// Size in bytes used to store the type of block encoded
	indexTypeSize = 1

	// Max number of blocks for a given key that can exist in a single file
	maxIndexEntries = (1 << (indexCountSize * 8)) - 1

	// max length of a key in an index entry (measurement + tags)
	maxKeyLength = (1 << (2 * 8)) - 1

	// The threshold amount data written before we periodically fsync a TSM file.  This helps avoid
	// long pauses due to very large fsyncs at the end of writing a TSM file.
	fsyncEvery = 25 * 1024 * 1024
)

var (
	//ErrNoValues is returned when TSMWriter.WriteIndex is called and there are no values to write.
	ErrNoValues = fmt.Errorf("no values written")

	// ErrTSMClosed is returned when performing an operation against a closed TSM file.
	ErrTSMClosed = fmt.Errorf("tsm file closed")

	// ErrMaxKeyLengthExceeded is returned when attempting to write a key that is too long.
	ErrMaxKeyLengthExceeded = fmt.Errorf("max key length exceeded")

	// ErrMaxBlocksExceeded is returned when attempting to write a block past the allowed number.
	ErrMaxBlocksExceeded = fmt.Errorf("max blocks exceeded")
)

// TSMWriter writes TSM formatted key and values.
type TSMWriter interface {
	// Write writes a new block for key containing and values.  Writes append
	// blocks in the order that the Write function is called.  The caller is
	// responsible for ensuring keys and blocks are sorted appropriately.
	// Values are encoded as a full block.  The caller is responsible for
	// ensuring a fixed number of values are encoded in each block as well as
	// ensuring the Values are sorted. The first and last timestamp values are
	// used as the minimum and maximum values for the index entry.
	Write(key []byte, values Values) error

	// WriteBlock writes a new block for key containing the bytes in block.  WriteBlock appends
	// blocks in the order that the WriteBlock function is called.  The caller is
	// responsible for ensuring keys and blocks are sorted appropriately, and that the
	// block and index information is correct for the block.  The minTime and maxTime
	// timestamp values are used as the minimum and maximum values for the index entry.
	WriteBlock(key []byte, minTime, maxTime int64, block []byte) error

	WriteBlockForGroup(groupKey []byte, minTime, maxTime int64, sequenceKeys [][]byte, sequenceCacheBlocks []*sequenceCacheBlock) error

	// WriteIndex finishes the TSM write streams and writes the index.
	WriteIndex() error

	WriteIndexForGroup() error

	// Flushes flushes all pending changes to the underlying file resources.
	Flush() error

	// Close closes any underlying file resources.
	Close() error

	// Size returns the current size in bytes of the file.
	Size() uint32

	Remove() error

	DataSize() uint32
}

// IndexWriter writes a TSMIndex.
type IndexWriter interface {
	// Add records a new block entry for a key in the index.
	Add(key []byte, blockType byte, minTime, maxTime int64, offset int64, size uint32)

	// Entries returns all index entries for a key.
	Entries(key []byte) []IndexEntry

	// KeyCount returns the count of unique keys in the index.
	KeyCount() int

	// Size returns the size of a the current index in bytes.
	Size() uint32

	// MarshalBinary returns a byte slice encoded version of the index.
	MarshalBinary() ([]byte, error)

	// WriteTo writes the index contents to a writer.
	WriteTo(w io.Writer) (int64, error)

	Close() error

	Remove() error
}

type GroupIndexWriter interface {
	// Add records a new block entry for a key in the index.
	Add(key []byte, minTime, maxTime int64, offset int64, size uint32)

	KeyCount() int

	WriteTo(w io.Writer) (int64, error)
}

type SequenceIndexWriter interface {
	// Add records a new block entry for a key in the index.
	Add(key []byte, minTime, maxTime int64, offset int64, size uint32)

	KeyCount() int

	WriteTo(w io.Writer) (int64, error)
}

// IndexEntry is the index information for a given block in a TSM file.
type IndexEntry struct {
	// The min and max time of all points stored in the block.
	MinTime, MaxTime int64

	// The absolute position in the file where this block is located.
	Offset int64

	// The size in bytes of the block in the file.
	Size uint32
}

// 组索引项
type GroupIndexEntry struct {
	Length   int64
	GroupKey []byte
	// The min and max time of all points stored in the block.
	MinTime, MaxTime int64

	// The absolute position in the file where this block is located.
	Offset int64

	// The size in bytes of the block in the file.
	Size uint32
}

// 序列索引项
type SequenceIndexEntry struct {
	Length      int64
	SequenceKey []byte
	// The min and max time of all points stored in the block.
	MinTime, MaxTime int64

	timeSlice int
	fieldLen  int
	max       [][]float64
	min       [][]float64
	sum       [][]float64
	valueLen  []int

	// The absolute position in the file where this block is located.
	Offset int64

	// The size in bytes of the block in the file.
	Size uint32
}

// UnmarshalBinary decodes an IndexEntry from a byte slice.
func (e *IndexEntry) UnmarshalBinary(b []byte) error {
	if len(b) < indexEntrySize {
		return fmt.Errorf("unmarshalBinary: short buf: %v < %v", len(b), indexEntrySize)
	}
	e.MinTime = int64(binary.BigEndian.Uint64(b[:8]))
	e.MaxTime = int64(binary.BigEndian.Uint64(b[8:16]))
	e.Offset = int64(binary.BigEndian.Uint64(b[16:24]))
	e.Size = binary.BigEndian.Uint32(b[24:28])
	return nil
}

func (e *GroupIndexEntry) UnmarshalBinary(b []byte) error {
	if len(b) < indexEntrySize {
		return fmt.Errorf("unmarshalBinary: short buf: %v < %v", len(b), indexEntrySize)
	}
	e.MinTime = int64(binary.BigEndian.Uint64(b[:8]))
	e.MaxTime = int64(binary.BigEndian.Uint64(b[8:16]))
	e.Offset = int64(binary.BigEndian.Uint64(b[16:24]))
	e.Size = binary.BigEndian.Uint32(b[24:28])
	return nil
}

func (e *SequenceIndexEntry) UnmarshalBinary(b []byte) error {
	if len(b) < 16+2+2 {
		return nil
	}
	e.MinTime = int64(binary.BigEndian.Uint64(b[:8]))
	e.MaxTime = int64(binary.BigEndian.Uint64(b[8:16]))

	offset := 16
	timeSlice := int(binary.BigEndian.Uint16(b[offset : offset+2]))
	e.timeSlice = timeSlice
	offset += 2

	fieldLen := int(binary.BigEndian.Uint16(b[offset : offset+2]))
	e.fieldLen = fieldLen
	offset += 2
	if len(b) < 2+indexEntrySize+2+(24*fieldLen+2)*timeSlice {
		return fmt.Errorf("unmarshalBinary: short buf: %v < %v", len(b), indexEntrySize)
	}

	e.max = make([][]float64, timeSlice)
	e.min = make([][]float64, timeSlice)
	e.sum = make([][]float64, timeSlice)
	e.valueLen = make([]int, timeSlice)

	for i := 0; i < timeSlice; i++ {
		e.max[i] = make([]float64, fieldLen)
		e.min[i] = make([]float64, fieldLen)
		e.sum[i] = make([]float64, fieldLen)

		for j := 0; j < fieldLen; j++ {
			bits := binary.BigEndian.Uint64(b[offset : offset+8])
			e.min[i][j] = math.Float64frombits(bits)
			offset += 8
		}

		for j := 0; j < fieldLen; j++ {
			bits := binary.BigEndian.Uint64(b[offset : offset+8])
			e.max[i][j] = math.Float64frombits(bits)
			offset += 8
		}

		for j := 0; j < fieldLen; j++ {
			bits := binary.BigEndian.Uint64(b[offset : offset+8])
			e.sum[i][j] = math.Float64frombits(bits)
			offset += 8
		}

		e.valueLen[i] = int(binary.BigEndian.Uint16(b[offset : offset+2]))
		offset += 2
	}
	e.Offset = int64(binary.BigEndian.Uint64(b[offset : offset+8]))
	offset += 8
	e.Size = binary.BigEndian.Uint32(b[offset : offset+4])
	return nil
}

// AppendTo writes a binary-encoded version of IndexEntry to b, allocating
// and returning a new slice, if necessary.
func (e *IndexEntry) AppendTo(b []byte) []byte {
	if len(b) < indexEntrySize {
		if cap(b) < indexEntrySize {
			b = make([]byte, indexEntrySize)
		} else {
			b = b[:indexEntrySize]
		}
	}

	binary.BigEndian.PutUint64(b[:8], uint64(e.MinTime))
	binary.BigEndian.PutUint64(b[8:16], uint64(e.MaxTime))
	binary.BigEndian.PutUint64(b[16:24], uint64(e.Offset))
	binary.BigEndian.PutUint32(b[24:28], uint32(e.Size))

	return b
}

// Contains returns true if this IndexEntry may contain values for the given time.
// The min and max times are inclusive.
func (e *IndexEntry) Contains(t int64) bool {
	return e.MinTime <= t && e.MaxTime >= t
}

func (e *GroupIndexEntry) Contains(t int64) bool {
	return e.MinTime <= t && e.MaxTime >= t
}

// OverlapsTimeRange returns true if the given time ranges are completely within the entry's time bounds.
func (e *IndexEntry) OverlapsTimeRange(min, max int64) bool {
	return e.MinTime <= max && e.MaxTime >= min
}

// String returns a string representation of the entry.
func (e *IndexEntry) String() string {
	return fmt.Sprintf("min=%s max=%s ofs=%d siz=%d",
		time.Unix(0, e.MinTime).UTC(), time.Unix(0, e.MaxTime).UTC(), e.Offset, e.Size)
}

// NewIndexWriter returns a new IndexWriter.
func NewIndexWriter() IndexWriter {
	buf := bytes.NewBuffer(make([]byte, 0, 1024*1024))
	return &directIndex{buf: buf, w: bufio.NewWriter(buf)}
}

func NewGroupIndexWriter() GroupIndexWriter {
	buf := bytes.NewBuffer(make([]byte, 0, 1024*1024))
	return &groupDirectIndex{buf: buf, w: bufio.NewWriter(buf)}
}

// NewIndexWriter returns a new IndexWriter.
func NewDiskIndexWriter(f *os.File) IndexWriter {
	return &directIndex{fd: f, w: bufio.NewWriterSize(f, 1024*1024)}
}

type syncer interface {
	Name() string
	Sync() error
}

// directIndex is a simple in-memory index implementation for a TSM file.  The full index
// must fit in memory. nxl 这是TSM File的索引表示，
type directIndex struct {
	keyCount int
	size     uint32

	// The bytes written count of when we last fsync'd
	lastSync uint32
	fd       *os.File
	buf      *bytes.Buffer

	f syncer

	w *bufio.Writer

	key          []byte
	indexEntries *indexEntries
}

// 直接索引，通过offset定位
type groupDirectIndex struct {
	groupKeyCount int
	size          uint32

	lastSync uint32
	fd       *os.File
	buf      *bytes.Buffer

	f syncer

	w *bufio.Writer

	groupKey        []byte           // 虽然不知道这个key要干嘛，应该是一个key对应一组索引
	groupIndexEntry *GroupIndexEntry // 只需要对应一个
}

func (d *directIndex) Add(key []byte, blockType byte, minTime, maxTime int64, offset int64, size uint32) {
	// Is this the first block being added?
	if len(d.key) == 0 {
		// size of the key stored in the index
		d.size += uint32(2 + len(key))
		// size of the count of entries stored in the index
		d.size += indexCountSize

		d.key = key
		if d.indexEntries == nil {
			d.indexEntries = &indexEntries{}
		}
		d.indexEntries.Type = blockType
		d.indexEntries.entries = append(d.indexEntries.entries, IndexEntry{
			MinTime: minTime,
			MaxTime: maxTime,
			Offset:  offset,
			Size:    size,
		})

		// size of the encoded index entry
		d.size += indexEntrySize
		d.keyCount++
		return
	}

	// See if were still adding to the same series key.
	cmp := bytes.Compare(d.key, key)
	if cmp == 0 {
		// The last block is still this key
		d.indexEntries.entries = append(d.indexEntries.entries, IndexEntry{
			MinTime: minTime,
			MaxTime: maxTime,
			Offset:  offset,
			Size:    size,
		})

		// size of the encoded index entry
		d.size += indexEntrySize

	} else if cmp < 0 {
		d.flush(d.w)
		// We have a new key that is greater than the last one so we need to add
		// a new index block section.

		// size of the key stored in the index
		d.size += uint32(2 + len(key))
		// size of the count of entries stored in the index
		d.size += indexCountSize

		d.key = key
		d.indexEntries.Type = blockType
		d.indexEntries.entries = append(d.indexEntries.entries, IndexEntry{
			MinTime: minTime,
			MaxTime: maxTime,
			Offset:  offset,
			Size:    size,
		})

		// size of the encoded index entry
		d.size += indexEntrySize
		d.keyCount++
	} else {
		// Keys can't be added out of order.
		panic(fmt.Sprintf("keys must be added in sorted order: %s < %s", string(key), string(d.key)))
	}
}

func (d *groupDirectIndex) Add(groupKey []byte, minTime, maxTime int64, offset int64, size uint32) {
	// 没数据则加入
	if len(d.groupKey) == 0 {
		// size of the key stored in the index

		d.groupKey = groupKey
		if d.groupIndexEntry == nil {
			d.groupIndexEntry = &GroupIndexEntry{
				Length:   int64(len(groupKey)), // 存的时候还用用2bit存储
				GroupKey: groupKey,
				MinTime:  minTime,
				MaxTime:  maxTime,
				Offset:   offset,
				Size:     size,
			}
		}
		d.size += uint32(2 + len(groupKey)) // len(key) groupkey
		d.size += indexEntrySize            // maxtime mintime offset size
		//d.size += uint32(len(groupKey))
		d.groupKeyCount++
		return
	}

	// 有数据则刷盘再加入
	d.flush(d.w)
	// We have a new key that is greater than the last one so we need to add
	// a new index block section.
	// size of the key stored in the index
	d.groupKey = groupKey
	d.groupIndexEntry = &GroupIndexEntry{
		Length:   int64(len(groupKey)),
		GroupKey: groupKey,
		MinTime:  minTime,
		MaxTime:  maxTime,
		Offset:   offset,
		Size:     size,
	}
	d.size += uint32(2 + len(groupKey))
	d.size += indexEntrySize
	d.groupKeyCount++
}

func (d *directIndex) entries(key []byte) []IndexEntry {
	if len(d.key) == 0 {
		return nil
	}

	if bytes.Equal(d.key, key) {
		return d.indexEntries.entries
	}

	return nil
}

func (d *directIndex) Entries(key []byte) []IndexEntry {
	return d.entries(key)
}

func (d *directIndex) Entry(key []byte, t int64) *IndexEntry {
	entries := d.entries(key)
	for _, entry := range entries {
		if entry.Contains(t) {
			return &entry
		}
	}
	return nil
}

func (d *directIndex) KeyCount() int {
	return d.keyCount
}

func (d *groupDirectIndex) KeyCount() int {
	return d.groupKeyCount
}

// copyBuffer is the actual implementation of Copy and CopyBuffer.
// if buf is nil, one is allocated.  This is copied from the Go stdlib
// in order to remove the fast path WriteTo calls which circumvent any
// IO throttling as well as to add periodic fsyncs to avoid long stalls.
func copyBuffer(f syncer, dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	if buf == nil {
		buf = make([]byte, 32*1024)
	}
	var lastSync int64
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}

			if written-lastSync > fsyncEvery {
				if err := f.Sync(); err != nil {
					return 0, err
				}
				lastSync = written
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

func (d *directIndex) WriteTo(w io.Writer) (int64, error) {
	if _, err := d.flush(d.w); err != nil {
		return 0, err
	}

	if err := d.w.Flush(); err != nil {
		return 0, err
	}

	if d.fd == nil {
		return copyBuffer(d.f, w, d.buf, nil)
	}

	if _, err := d.fd.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	return io.Copy(w, bufio.NewReaderSize(d.fd, 1024*1024))
}

func (d *groupDirectIndex) WriteTo(w io.Writer) (int64, error) {
	if _, err := d.flush(d.w); err != nil {
		return 0, err
	}

	if err := d.w.Flush(); err != nil {
		return 0, err
	}

	if d.fd == nil {
		return copyBuffer(d.f, w, d.buf, nil)
	}

	if _, err := d.fd.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	return io.Copy(w, bufio.NewReaderSize(d.fd, 1024*1024))
}

// nxl 这个函数是代表一个Index Block：包括header + N个entries
func (d *directIndex) flush(w io.Writer) (int64, error) {
	var (
		n   int
		err error
		buf [5]byte
		N   int64
	)

	if len(d.key) == 0 {
		return 0, nil
	}
	// For each key, individual entries are sorted by time
	key := d.key
	entries := d.indexEntries

	if entries.Len() > maxIndexEntries {
		return N, fmt.Errorf("key '%s' exceeds max index entries: %d > %d", key, entries.Len(), maxIndexEntries)
	}

	if !sort.IsSorted(entries) {
		sort.Sort(entries)
	}

	binary.BigEndian.PutUint16(buf[0:2], uint16(len(key)))
	buf[2] = entries.Type
	binary.BigEndian.PutUint16(buf[3:5], uint16(entries.Len()))

	// Append the key length and key
	if n, err = w.Write(buf[0:2]); err != nil {
		return int64(n) + N, fmt.Errorf("write: writer key length error: %v", err)
	}
	N += int64(n)

	if n, err = w.Write(key); err != nil {
		return int64(n) + N, fmt.Errorf("write: writer key error: %v", err)
	}
	N += int64(n)

	// Append the block type and count
	if n, err = w.Write(buf[2:5]); err != nil {
		return int64(n) + N, fmt.Errorf("write: writer block type and count error: %v", err)
	}
	N += int64(n)

	// Append each index entry for all blocks for this key
	var n64 int64
	if n64, err = entries.WriteTo(w); err != nil {
		return n64 + N, fmt.Errorf("write: writer entries error: %v", err)
	}
	N += n64

	d.key = nil
	d.indexEntries.Type = 0
	d.indexEntries.entries = d.indexEntries.entries[:0]

	// If this is a disk based index and we've written more than the fsync threshold,
	// fsync the data to avoid long pauses later on.
	if d.fd != nil && d.size-d.lastSync > fsyncEvery {
		if err := d.fd.Sync(); err != nil {
			return N, err
		}
		d.lastSync = d.size
	}

	return N, nil

}

func (d *groupDirectIndex) flush(w io.Writer) (int64, error) {
	var (
		err error
		N   int64
	)

	if len(d.groupKey) == 0 {
		return 0, nil
	}

	// 此时已经按groupKey的顺序排好了
	// 把GroupKey的长度编入字节数组
	// Append each index entry for all blocks for this key
	var n64 int64
	if n64, err = d.groupIndexEntry.WriteTo(w); err != nil {
		return n64 + N, fmt.Errorf("write: writer entries error: %v", err)
	}
	N += n64

	d.groupKey = nil
	d.groupIndexEntry = nil

	if d.fd != nil && d.size-d.lastSync > fsyncEvery {
		if err := d.fd.Sync(); err != nil {
			return N, err
		}
		d.lastSync = d.size
	}

	return N, nil

}

func (d *directIndex) MarshalBinary() ([]byte, error) {
	var b bytes.Buffer
	if _, err := d.WriteTo(&b); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (d *directIndex) Size() uint32 {
	return d.size
}

func (d *directIndex) Close() error {
	// Flush anything remaining in the index
	if err := d.w.Flush(); err != nil {
		return err
	}

	if d.fd == nil {
		return nil
	}

	if err := d.fd.Close(); err != nil {
		return err
	}
	return os.Remove(d.fd.Name())
}

// Remove removes the index from any tempory storage
func (d *directIndex) Remove() error {
	if d.fd == nil {
		return nil
	}

	// Close the file handle to prevent leaking.  We ignore the error because
	// we just want to cleanup and remove the file.
	_ = d.fd.Close()

	return os.Remove(d.fd.Name())
}

// tsmWriter writes keys and values in the TSM format
type tsmWriter struct {
	wrapped    io.Writer
	w          *bufio.Writer
	index      IndexWriter
	groupIndex GroupIndexWriter // 这个是所有组索引
	n          int64            // 写入的字节数

	// The bytes written count of when we last fsync'd
	lastSync int64
}

// NewTSMWriter returns a new TSMWriter writing to w.
func NewTSMWriter(w io.Writer) (TSMWriter, error) {
	index := NewIndexWriter()
	groupIndex := NewGroupIndexWriter()
	return &tsmWriter{wrapped: w, w: bufio.NewWriterSize(w, 1024*1024), index: index, groupIndex: groupIndex}, nil
}

// NewTSMWriterWithDiskBuffer returns a new TSMWriter writing to w and will use a disk
// based buffer for the TSM index if possible.
func NewTSMWriterWithDiskBuffer(w io.Writer) (TSMWriter, error) {
	var index IndexWriter
	// Make sure is a File so we can write the temp index alongside it.
	if fw, ok := w.(syncer); ok {
		f, err := os.OpenFile(strings.TrimSuffix(fw.Name(), ".tsm.tmp")+".idx.tmp", os.O_CREATE|os.O_RDWR|os.O_EXCL, 0666)
		if err != nil {
			return nil, err
		}
		index = NewDiskIndexWriter(f)
	} else {
		// w is not a file, just use an inmem index
		index = NewIndexWriter()
	}

	return &tsmWriter{wrapped: w, w: bufio.NewWriterSize(w, 1024*1024), index: index}, nil
}

func (t *tsmWriter) writeHeader() error {
	var buf [5]byte
	binary.BigEndian.PutUint32(buf[0:4], MagicNumber)
	buf[4] = Version

	n, err := t.w.Write(buf[:])
	if err != nil {
		return err
	}
	t.n = int64(n)
	return nil
}

// Write writes a new block containing key and values.
func (t *tsmWriter) Write(key []byte, values Values) error {
	if len(key) > maxKeyLength {
		return ErrMaxKeyLengthExceeded
	}

	// Nothing to write
	if len(values) == 0 {
		return nil
	}

	// Write header only after we have some data to write.
	if t.n == 0 {
		if err := t.writeHeader(); err != nil {
			return err
		}
	}

	block, err := values.Encode(nil)
	if err != nil {
		return err
	}

	blockType, err := BlockType(block)
	if err != nil {
		return err
	}

	var checksum [crc32.Size]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(block))

	_, err = t.w.Write(checksum[:])
	if err != nil {
		return err
	}

	n, err := t.w.Write(block)
	if err != nil {
		return err
	}
	n += len(checksum)

	// Record this block in index
	t.index.Add(key, blockType, values[0].UnixNano(), values[len(values)-1].UnixNano(), t.n, uint32(n))

	// Increment file position pointer
	t.n += int64(n)

	if len(t.index.Entries(key)) >= maxIndexEntries {
		return ErrMaxBlocksExceeded
	}

	return nil
}

// WriteBlock writes block for the given key and time range to the TSM file.  If the write
// exceeds max entries for a given key, ErrMaxBlocksExceeded is returned.  This indicates
// that the index is now full for this key and no future writes to this key will succeed.
func (t *tsmWriter) WriteBlock(key []byte, minTime, maxTime int64, block []byte) error {
	if len(key) > maxKeyLength {
		return ErrMaxKeyLengthExceeded
	}

	// Nothing to write
	if len(block) == 0 {
		return nil
	}

	blockType, err := BlockType(block)
	if err != nil {
		return err
	}

	// Write header only after we have some data to write.
	if t.n == 0 {
		if err := t.writeHeader(); err != nil {
			return err
		}
	}

	var checksum [crc32.Size]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(block))

	_, err = t.w.Write(checksum[:])
	if err != nil {
		return err
	}

	n, err := t.w.Write(block) // 返回写入了多少个字节
	if err != nil {
		return err
	}
	n += len(checksum)

	// Record this block in index 这个函数就是t.n这个位置后面n个字节是什么东西
	t.index.Add(key, blockType, minTime, maxTime, t.n, uint32(n)) // t.n为offset n为size

	// Increment file position pointer (checksum + block len)
	t.n += int64(n)

	// fsync the file periodically to avoid long pauses with very big files.
	if t.n-t.lastSync > fsyncEvery {
		if err := t.sync(); err != nil {
			return err
		}
		t.lastSync = t.n
	}

	if len(t.index.Entries(key)) >= maxIndexEntries {
		return ErrMaxBlocksExceeded
	}

	return nil
}

func (t *tsmWriter) WriteBlockForGroup(groupKey []byte, minTime, maxTime int64, sequenceKeys [][]byte, sequenceCacheBlocks []*sequenceCacheBlock) error {
	if len(groupKey) > maxKeyLength {
		return ErrMaxKeyLengthExceeded
	}

	// Nothing to write
	if len(sequenceKeys) == 0 {
		return nil
	}

	// Write header only after we have some data to write.
	if t.n == 0 {
		if err := t.writeHeader(); err != nil {
			return err
		}
	}

	var checksum [crc32.Size]byte
	// 直接写四个字节 不判断了
	//binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(groupBlock))

	_, err := t.w.Write(checksum[:])
	if err != nil {
		return err
	}

	// 直接写入文件：group内部采用相对定位
	//writeToFile("block.txt", "组 key："+string(groupKey))
	n, err := WriteGroupBlock(t.w, sequenceKeys, sequenceCacheBlocks)

	//n, err := t.w.Write(groupBlock) // 返回写入了多少个字节
	if err != nil {
		return err
	}
	n += len(checksum) // 此时n是数据加上校验码的数据

	// Record this block in index 这个函数就是t.n这个位置后面n个字节是什么东西
	//fmt.Println("组key：", string(groupKey))
	//writeToFile("block.txt", fmt.Sprintf("偏移量 = %d", t.n))
	//writeToFile("block.txt", fmt.Sprintf("大小 = %d\n\n", n))
	//fmt.Println("偏移量： ", t.n)
	//fmt.Println("大小： ", n)
	t.groupIndex.Add(groupKey, minTime, maxTime, t.n, uint32(n)) // t.n为offset n为size

	//if t.w.Available() < n {
	//	t.w.Flush()
	//}

	// Increment file position pointer (checksum + block len)
	t.n += int64(n) // TSM文件的大小加上刚写入的数据块的大小

	// fsync the file periodically to avoid long pauses with very big files.
	if t.n-t.lastSync > fsyncEvery {
		if err := t.sync(); err != nil {
			return err
		}
		t.lastSync = t.n
	}
	return nil
}

func sequenceBlockSize(b [][]byte) int {
	n := 0
	for _, tmp := range b {
		n += len(tmp)
		//writeToFile("block.txt", fmt.Sprintf("index = %d, len = %d", index, len(tmp)))
		//byteString := "["
		//for _, byteUint := range tmp {
		//	byteString += fmt.Sprintf("%d, ", byteUint)
		//}
		//writeToFile("block.txt", fmt.Sprintf("index content = %s", byteString))
	}
	//writeToFile("block.txt", fmt.Sprintf("组block的数据大小 = %d", n))
	return n
}

// TODO:将属于一个组的序列块组装成GroupBlock
// 先写入所有数据块，再写入索引块，再写入最后8字节序列索引的起始位置
func WriteGroupBlock(w *bufio.Writer, keys [][]byte, cacheBlocks []*sequenceCacheBlock) (int, error) {
	//size := groupBlockSize(keys, cacheBlocks) + 8 // 最后的8是索引块的位置
	//buf := make([]byte, size)                     // 装数据的
	// 遍历每个sequenceBlock，记录偏移量和大小，最大最小时间，写入数据之后记录索引

	sequenceIndex := make([]SequenceIndexEntry, 0) // 序列索引

	// 此时cacheBlocks已经按照sequenceKey排好序了
	N := 0 // 写入文件的数据大小

	n := 0 // 记录总大小
	for _, block := range cacheBlocks {
		//writeToFile("block.txt", "序列key："+string(block.sequenceKey))
		index := SequenceIndexEntry{}
		index.Length = int64(len(block.sequenceKey)) // key长度
		index.SequenceKey = block.sequenceKey        // key内容
		index.MinTime = block.minTime
		index.MaxTime = block.maxTime
		index.timeSlice = block.timeSlice
		index.fieldLen = block.width
		index.max = make([][]float64, index.timeSlice)
		index.min = make([][]float64, index.timeSlice)
		index.sum = make([][]float64, index.timeSlice)
		index.valueLen = make([]int, index.timeSlice)
		for i := 0; i < index.timeSlice; i++ {
			index.min[i] = block.min[i]
			index.max[i] = block.max[i]
			index.sum[i] = block.sum[i]
			index.valueLen[i] = block.valueLen[i]
		}
		index.Offset = int64(n) // 这个block的的偏移量
		index.Size = uint32(sequenceBlockSize(block.b)) + uint32(4+4*block.width)

		n += int(index.Size)
		N += int(index.Size) // 后面的4 * (block.width + 1)是数据的相对偏移量
		N += 2 + int(index.Length) + 8 + 8 + 8 + 4 + 2 + 2 + ((8+8+8)*block.width+2)*block.timeSlice
		//writeToFile("block.txt", fmt.Sprintf("序列key的索引长度：%d\n", 2+int(index.Length)+8+8+8+4))
		sequenceIndex = append(sequenceIndex, index)
	}
	//fmt.Println("n=", n)

	// 填充所有的block.b
	for i := 0; i < len(cacheBlocks); i++ {
		// [width个数，每一个field偏移]
		offset := uint32(4 + 4*cacheBlocks[i].width) // groupBlock内部的偏移 第一个总是为时间戳
		var writeOffset [4]byte

		// 写入4字节width宽度
		binary.BigEndian.PutUint32(writeOffset[:], uint32(cacheBlocks[i].width))
		w.Write(writeOffset[:])

		// 写入偏移
		for index, b := range cacheBlocks[i].b {
			if index == 0 { // 0是时间戳数据。不需要偏移
				offset += uint32(len(b))
			} else {
				binary.BigEndian.PutUint32(writeOffset[:], offset)
				w.Write(writeOffset[:])
				offset += uint32(len(b))
			}
		}
		//（2） 写入block数据之前，把偏移量写进去
		for _, b := range cacheBlocks[i].b {
			w.Write(b)
		}
	}

	indexStart := n // 索引起始位置

	// 填充所有的索引：顺序是[]
	buf := make([]byte, 16)
	for i := 0; i < len(cacheBlocks); i++ {
		index := sequenceIndex[i]
		//写入sequenceKey
		binary.BigEndian.PutUint16(buf[0:2], uint16(len(index.SequenceKey)))
		w.Write(buf[0:2])
		w.Write(index.SequenceKey)

		//写入MinTime和MaxTime
		binary.BigEndian.PutUint64(buf[:8], uint64(index.MinTime))
		binary.BigEndian.PutUint64(buf[8:16], uint64(index.MaxTime))
		w.Write(buf[:16])

		//写入时间分片长度
		binary.BigEndian.PutUint16(buf[:2], uint16(index.timeSlice))
		w.Write(buf[:2])

		//写入数组长度
		binary.BigEndian.PutUint16(buf[:2], uint16(index.fieldLen))
		w.Write(buf[:2])

		for i := 0; i < index.timeSlice; i++ {
			// 写入 min 数组
			for _, val := range index.min[i] {
				binary.BigEndian.PutUint64(buf[:8], math.Float64bits(val))
				w.Write(buf[:8])
			}

			// 写入 max 数组
			for _, val := range index.max[i] {
				binary.BigEndian.PutUint64(buf[:8], math.Float64bits(val))
				w.Write(buf[:8])
			}

			// 写入 sum 数组
			for _, val := range index.sum[i] {
				binary.BigEndian.PutUint64(buf[:8], math.Float64bits(val)) // 将浮点数转为字节
				w.Write(buf[:8])
			}

			// 写入 valueLen
			binary.BigEndian.PutUint16(buf[:2], uint16(index.valueLen[i]))
			w.Write(buf[:2])
		}

		// 写入 Offset 和 Size
		binary.BigEndian.PutUint64(buf[:8], uint64(index.Offset))
		binary.BigEndian.PutUint32(buf[8:12], uint32(index.Size))
		w.Write(buf[:12])
	}

	binary.BigEndian.PutUint64(buf[:8], uint64(indexStart))
	w.Write(buf[:8])

	return N + 8, nil
}

func groupBlockSize(keys [][]byte, cacheBlocks []*sequenceCacheBlock) int {
	size := 0
	for _, key := range keys {
		size += (2 + len(key) + 8 + 8 + 8 + 4)
	}

	for _, block := range cacheBlocks {
		for _, b := range block.b {
			size += len(b)
		}
	}
	return size
}

// WriteIndex writes the index section of the file.  If there are no index entries to write,
// this returns ErrNoValues.
func (t *tsmWriter) WriteIndex() error {
	indexPos := t.n

	if t.index.KeyCount() == 0 {
		return ErrNoValues
	}

	// Set the destination file on the index so we can periodically
	// fsync while writing the index.
	if f, ok := t.wrapped.(syncer); ok {
		t.index.(*directIndex).f = f
	}

	// Write the index
	if _, err := t.index.WriteTo(t.w); err != nil {
		return err
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(indexPos))

	// Write the index index position
	_, err := t.w.Write(buf[:])
	return err
}

func (t *tsmWriter) WriteIndexForGroup() error {
	indexPos := t.n

	if t.groupIndex.KeyCount() == 0 {
		return ErrNoValues
	}

	// Set the destination file on the index so we can periodically
	// fsync while writing the index.
	if f, ok := t.wrapped.(syncer); ok {
		t.groupIndex.(*groupDirectIndex).f = f
	}

	// Write the index
	if _, err := t.groupIndex.WriteTo(t.w); err != nil {
		return err
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(indexPos))

	// Write the index index position
	_, err := t.w.Write(buf[:])
	return err
}

func (t *tsmWriter) Flush() error {
	if err := t.w.Flush(); err != nil {
		return err
	}

	return t.sync()
}

func (t *tsmWriter) sync() error {
	// sync is a minimal interface to make sure we can sync the wrapped
	// value. we use a minimal interface to be as robust as possible for
	// syncing these files.
	type sync interface {
		Sync() error
	}

	if f, ok := t.wrapped.(sync); ok {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (t *tsmWriter) Close() error {
	if err := t.Flush(); err != nil {
		return err
	}

	if err := t.index.Close(); err != nil {
		return err
	}

	if c, ok := t.wrapped.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// Remove removes any temporary storage used by the writer.
func (t *tsmWriter) Remove() error {
	if err := t.index.Remove(); err != nil {
		return err
	}

	// nameCloser is the most permissive interface we can close the wrapped
	// value with.
	type nameCloser interface {
		io.Closer
		Name() string
	}

	if f, ok := t.wrapped.(nameCloser); ok {
		// Close the file handle to prevent leaking.  We ignore the error because
		// we just want to cleanup and remove the file.
		_ = f.Close()

		return os.Remove(f.Name())
	}
	return nil
}

func (t *tsmWriter) Size() uint32 {
	return uint32(t.n) + t.index.Size()
}

func (t *tsmWriter) DataSize() uint32 {
	return uint32(t.n)
}

// verifyVersion verifies that the reader's bytes are a TSM byte
// stream of the correct version (1)
func verifyVersion(r io.ReadSeeker) error {
	_, err := r.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("init: failed to seek: %v", err)
	}
	var b [4]byte
	_, err = io.ReadFull(r, b[:])
	if err != nil {
		return fmt.Errorf("init: error reading magic number of file: %v", err)
	}
	if binary.BigEndian.Uint32(b[:]) != MagicNumber {
		return fmt.Errorf("can only read from tsm file")
	}
	_, err = io.ReadFull(r, b[:1])
	if err != nil {
		return fmt.Errorf("init: error reading version: %v", err)
	}
	if b[0] != Version {
		return fmt.Errorf("init: file is version %b. expected %b", b[0], Version)
	}

	return nil
}
