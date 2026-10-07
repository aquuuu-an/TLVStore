package tsm1

import (
	"fmt"
	"github.com/influxdata/influxdb/nxl"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

// ringShards specifies the number of partitions that the hash ring used to
// store the entry mappings contains. It must be a power of 2. From empirical
// testing, a value above the number of cores on the machine does not provide
// any additional benefit. For now we'll set it to the number of cores on the
// largest box we could imagine running influx.
const ringShards = 16

var (
	// ErrSnapshotInProgress is returned if a snapshot is attempted while one is already running.
	ErrSnapshotInProgress = fmt.Errorf("snapshot in progress")
)

// ErrCacheMemorySizeLimitExceeded returns an error indicating an operation
// could not be completed due to exceeding the cache-max-memory-size setting.
func ErrCacheMemorySizeLimitExceeded(n, limit uint64) error {
	return fmt.Errorf("cache-max-memory-size exceeded: (%d/%d)", n, limit)
}

// entry is a set of values and some metadata.
type entry struct {
	mu     sync.RWMutex
	values Values // All stored values.

	// The type of values stored. Read only so doesn't need to be protected by
	// mu.
	vtype byte
}

// ADD:适配多值的模型
type MultiEntry struct {
	mu        sync.RWMutex
	values    MultiValuesList         // 多个数据点
	fieldSet  *tsdb.MeasurementFields // 对应的表结构，属于同一个测量的fieldset一定是一样的
	timeSlice int
	max       [][]float64
	min       [][]float64
	sum       [][]float64
	valueLen  []int
}

// newEntryValues returns a new instance of entry with the given values.  If the
// values are not valid, an error is returned.
func newEntryValues(values []Value) (*entry, error) {
	e := &entry{}
	e.values = make(Values, 0, len(values))
	e.values = append(e.values, values...)

	// No values, don't check types and ordering
	if len(values) == 0 {
		return e, nil
	}

	et := valueType(values[0])
	for _, v := range values {
		// Make sure all the values are the same type
		if et != valueType(v) {
			return nil, tsdb.ErrFieldTypeConflict
		}
	}

	// Set the type of values stored.
	e.vtype = et

	return e, nil
}

func newMultiEntryValues(values MultiValuesList) (*MultiEntry, error) {
	e := &MultiEntry{}
	e.values = make(MultiValuesList, 0, len(values))
	e.values = append(e.values, values...)

	// No values, don't check types and ordering
	if len(values) == 0 {
		return e, nil
	}

	// 跳过类型判断
	//et := valueType(values[0])
	//for _, v := range values {
	//	// Make sure all the values are the same type
	//	if et != valueType(v) {
	//		return nil, tsdb.ErrFieldTypeConflict
	//	}
	//}
	//
	//// Set the type of values stored.
	//e.vtype = et

	return e, nil
}

// add adds the given values to the entry.
func (e *entry) add(values []Value) error {
	if len(values) == 0 {
		return nil // Nothing to do.
	}

	// Are any of the new values the wrong type?
	if e.vtype != 0 {
		for _, v := range values {
			if e.vtype != valueType(v) {
				return tsdb.ErrFieldTypeConflict
			}
		}
	}

	// entry currently has no values, so add the new ones and we're done.
	e.mu.Lock()
	if len(e.values) == 0 {
		e.values = values
		e.vtype = valueType(values[0])
		e.mu.Unlock()
		return nil
	}

	// Append the new values to the existing ones...
	e.values = append(e.values, values...)
	e.mu.Unlock()
	return nil
}

func (e *MultiEntry) add(values []tsdb.MultiValue) error {
	if len(values) == 0 {
		return nil // Nothing to do.
	}
	// 此处要判断数据类型
	// entry currently has no values, so add the new ones and we're done.
	e.mu.Lock()
	if len(e.values) == 0 {
		e.values = values
		e.mu.Unlock()
		return nil
	}

	// Append the new values to the existing ones...
	e.values = append(e.values, values...)
	e.mu.Unlock()
	return nil
}

// deduplicate sorts and orders the entry's values. If values are already deduped and sorted,
// the function does no work and simply returns.
func (e *entry) deduplicate() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.values) <= 1 {
		return
	}
	e.values = e.values.Deduplicate()
}

func (e *MultiEntry) deduplicate() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.values) <= 1 {
		return
	}
	e.values = e.values.Deduplicate()

	startT := e.values[0].Ts
	endT := e.values[len(e.values)-1].Ts

	t1 := time.Unix(0, startT)
	t2 := time.Unix(0, endT)
	hours := t2.Hour() - t1.Hour()
	e.timeSlice = hours + 1
	fieldCount := e.values[0].Width // 每个值的字段数量

	// 初始化 max、min 和 avg 容器
	// 假设每个 values[i] 都有相同的字段数量
	if len(e.max) == 0 {
		e.max = make([][]float64, e.timeSlice)
		e.min = make([][]float64, e.timeSlice)
		e.sum = make([][]float64, e.timeSlice)
		e.valueLen = make([]int, e.timeSlice)

		for i := 0; i < e.timeSlice; i++ {
			e.max[i] = make([]float64, fieldCount)
			e.min[i] = make([]float64, fieldCount)
			e.sum[i] = make([]float64, fieldCount)
		}
	}
	count := make([][]int, e.timeSlice)
	for i := 0; i < e.timeSlice; i++ {
		count[i] = make([]int, fieldCount)
	}

	// 遍历所有 values
	for _, value := range e.values {
		// 计算该值所属的小时
		t := time.Unix(0, value.Ts)
		hourIndex := t.Hour() - t1.Hour()

		// 遍历 value 中的每个字段
		for i, val := range value.Val {
			switch v := val.Value().(type) {
			case int64:
				// 更新最大值和最小值
				if count[hourIndex][i] == 0 || float64(v) > e.max[hourIndex][i] {
					e.max[hourIndex][i] = float64(v)
				}
				if count[hourIndex][i] == 0 || float64(v) < e.min[hourIndex][i] {
					e.min[hourIndex][i] = float64(v)
				}
				// 累计求和
				e.sum[hourIndex][i] += float64(v)
			case float64:
				// 更新最大值和最小值
				if count[hourIndex][i] == 0 || v > e.max[hourIndex][i] {
					e.max[hourIndex][i] = v
				}
				if count[hourIndex][i] == 0 || v < e.min[hourIndex][i] {
					e.min[hourIndex][i] = v
				}
				// 累计求和
				e.sum[hourIndex][i] += v

			default:
				panic(fmt.Sprintf("unsupported value type: %T", v))
			}
			count[hourIndex][i]++
		}
	}
	e.timeSlice = len(e.max)
	e.valueLen = make([]int, e.timeSlice)

	// 计算平均值
	for h := 0; h < e.timeSlice; h++ {
		e.valueLen[h] = count[h][0]
	}
}

// count returns the number of values in this entry.
func (e *entry) count() int {
	e.mu.RLock()
	n := len(e.values)
	e.mu.RUnlock()
	return n
}

func (e *MultiEntry) count() int {
	e.mu.RLock()
	n := len(e.values)
	e.mu.RUnlock()
	return n
}

// filter removes all values with timestamps between min and max inclusive.
func (e *entry) filter(min, max int64) {
	e.mu.Lock()
	if len(e.values) > 1 {
		e.values = e.values.Deduplicate()
	}
	e.values = e.values.Exclude(min, max)
	e.mu.Unlock()
}

// size returns the size of this entry in bytes.
func (e *entry) size() int {
	e.mu.RLock()
	sz := e.values.Size()
	e.mu.RUnlock()
	return sz
}

// InfluxQLType returns for the entry the data type of its values.
func (e *entry) InfluxQLType() (influxql.DataType, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.values.InfluxQLType()
}

// Statistics gathered by the Cache.
const (
	// levels - point in time measures

	statCacheMemoryBytes = "memBytes"      // level: Size of in-memory cache in bytes
	statCacheDiskBytes   = "diskBytes"     // level: Size of on-disk snapshots in bytes
	statSnapshots        = "snapshotCount" // level: Number of active snapshots.
	statCacheAgeMs       = "cacheAgeMs"    // level: Number of milliseconds since cache was last snapshoted at sample time

	// counters - accumulative measures

	statCachedBytes         = "cachedBytes"         // counter: Total number of bytes written into snapshots.
	statWALCompactionTimeMs = "WALCompactionTimeMs" // counter: Total number of milliseconds spent compacting snapshots

	statCacheWriteOK      = "writeOk"
	statCacheWriteErr     = "writeErr"
	statCacheWriteDropped = "writeDropped"
)

// storer is the interface that descibes a cache's store.
type storer interface {
	entry(key []byte) *entry                                                                       // Get an entry by its key.
	write(key []byte, values Values) (bool, error)                                                 // Write an entry to the store.
	add(key []byte, entry *entry)                                                                  // Add a new entry to the store.
	remove(key []byte)                                                                             // Remove an entry from the store.
	keys(sorted bool) [][]byte                                                                     // Return an optionally sorted slice of entry keys.
	apply(f func([]byte, *entry) error) error                                                      // Apply f to all entries in the store in parallel.
	applySerial(f func([]byte, *entry) error) error                                                // Apply f to all entries in serial.
	reset()                                                                                        // Reset the store to an initial unused state.
	split(n int) []storer                                                                          // Split splits the store into n stores
	count() int                                                                                    // Count returns the number of keys in the store
	memSizeAndCount() (keyMemSize uint64, valueMemSize uint64, keyCount uint64, valueCount uint64) // 计算内存占用
}

type storerForGroup interface {
	sequenceMap(groupKey []byte) map[string]*MultiEntry
	entryForGroup(groupKey []byte, sequenceKey []byte) *MultiEntry // Get an entry by its key.
	entryForGroup2(groupKey []byte) map[string]*MultiEntry
	writeForGroup(groupKey []byte, sequenceKey []byte, values MultiValuesList) (bool, bool, error) // Write an entry to the store.
	addForGroup(groupKey []byte, sequenceKey []byte, entry *MultiEntry)                            // Add a new entry to the store.
	removeForGroup(groupKey []byte, sequenceKey []byte)                                            // Remove an entry from the store.
	resetForGroup()                                                                                // Reset the store to an initial unused state.
	splitForGroup(n int) []storerForGroup
	applyForGroup(f func([]byte, []byte, *MultiEntry) error) error // Split splits the store into n stores
	memSizeAndCount() (keyMemSize uint64, valueMemSize uint64, gKeyMemSize uint64, sKeyMemSize uint64,
		groupKeyCount uint64, sequenceKeyCount uint64, valueCount uint64) //
	countForGroup() int
	groupKeys(sorted bool) [][]byte
}

// Cache maintains an in-memory store of Values for a set of keys.
type Cache struct {
	// Due to a bug in atomic  size needs to be the first word in the struct, as
	// that's the only place where you're guaranteed to be 64-bit aligned on a
	// 32 bit system. See: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	size         uint64
	snapshotSize uint64

	mu            sync.RWMutex
	store         storer         // storer is the interface that descibes a cache's store.
	storeForGroup storerForGroup // storer is the interface that descibes a cache's store.
	maxSize       uint64

	// snapshots are the cache objects that are currently being written to tsm files
	// they're kept in memory while flushing so they can be queried along with the cache.
	// they are read only and should never be modified
	snapshot      *Cache
	snapshotting  bool
	SnapshotCount int

	// This number is the number of pending or failed WriteSnaphot attempts since the last successful one.
	snapshotAttempts int

	stats         *CacheStatistics
	lastSnapshot  time.Time
	lastWriteTime time.Time

	// A one time synchronization used to initial the cache with a store.  Since the store can allocate a
	// a large amount memory across shards, we lazily create it.
	initialize       atomic.Value
	initializedCount uint32
}

// NewCache returns an instance of a cache which will use a maximum of maxSize bytes of memory.
// Only used for engine caches, never for snapshots.
func NewCache(maxSize uint64) *Cache {
	c := &Cache{
		maxSize:      maxSize,
		store:        emptyStore{},
		stats:        &CacheStatistics{},
		lastSnapshot: time.Now(),
	}
	c.initialize.Store(&sync.Once{})
	c.UpdateAge()
	c.UpdateCompactTime(0)
	c.updateCachedBytes(0)
	c.updateMemSize(0)
	c.updateSnapshots()
	return c
}

// CacheStatistics hold statistics related to the cache.
type CacheStatistics struct {
	MemSizeBytes        int64
	DiskSizeBytes       int64
	SnapshotCount       int64
	CacheAgeMs          int64
	CachedBytes         int64
	WALCompactionTimeMs int64
	WriteOK             int64
	WriteErr            int64
	WriteDropped        int64
}

// nxl
func (c *Cache) CacheMemSizeAndCount() {
	kms, vms, kc, vc := c.store.memSizeAndCount()
	logFile := nxl.CacheFilePath
	logContent := fmt.Sprintf("+++++++++++++  %d  ++++++++++++\n", c.SnapshotCount) +
		fmt.Sprintf("Cache(自己统计)内存总占用：%d\n", c.size) +
		fmt.Sprintf("Cache Key内存总占用：%d\n", kms) +
		fmt.Sprintf("Cache Value内存总占用：%d\n", vms) +
		fmt.Sprintf("Cache Key数量：%d\n", kc) +
		fmt.Sprintf("Cache Value数量：%d\n", vc) +
		fmt.Sprintf("Cache 统计的count：%d\n", c.Count())

	writeToFile(logFile, logContent)
}

func (c *Cache) CacheMemSizeAndCountForGroup() {
	kms, vms, gkms, skms, gkc, skc, vc := c.storeForGroup.memSizeAndCount()
	logFile := nxl.CacheFilePath
	logContent := fmt.Sprintf("+++++++++++++  %d  ++++++++++++\n", c.SnapshotCount) +
		fmt.Sprintf("Cache(Influxdb自己统计)内存总占用：%d\n", c.size) +
		fmt.Sprintf("Cache Key内存总占用：%d\n", kms) +
		fmt.Sprintf("Cache Value内存总占用：%d\n", vms) +
		fmt.Sprintf("Cache GroupKey内存总占用：%d\n", gkms) +
		fmt.Sprintf("Cache SequenceKey内存总占用：%d\n", skms) +
		fmt.Sprintf("Cache GroupKey数量：%d\n", gkc) +
		fmt.Sprintf("Cache SequenceKey数量：%d\n", skc) +
		fmt.Sprintf("Cache Value数量：%d\n", vc)

	writeToFile(logFile, logContent)
}

func writeToFile(fileName, content string) error {
	// Open the file with write-only mode or create it if it doesn't exist
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write content to the file
	file.WriteString(content + "\n")

	return nil
}

// Statistics returns statistics for periodic monitoring.
func (c *Cache) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "tsm1_cache",
		Tags: tags,
		Values: map[string]interface{}{
			statCacheMemoryBytes:    atomic.LoadInt64(&c.stats.MemSizeBytes),
			statCacheDiskBytes:      atomic.LoadInt64(&c.stats.DiskSizeBytes),
			statSnapshots:           atomic.LoadInt64(&c.stats.SnapshotCount),
			statCacheAgeMs:          atomic.LoadInt64(&c.stats.CacheAgeMs),
			statCachedBytes:         atomic.LoadInt64(&c.stats.CachedBytes),
			statWALCompactionTimeMs: atomic.LoadInt64(&c.stats.WALCompactionTimeMs),
			statCacheWriteOK:        atomic.LoadInt64(&c.stats.WriteOK),
			statCacheWriteErr:       atomic.LoadInt64(&c.stats.WriteErr),
			statCacheWriteDropped:   atomic.LoadInt64(&c.stats.WriteDropped),
		},
	}}
}

// init initializes the cache and allocates the underlying store.  Once initialized,
// the store re-used until Freed.
func (c *Cache) init() {
	if !atomic.CompareAndSwapUint32(&c.initializedCount, 0, 1) {
		return
	}

	c.mu.Lock()
	c.store, _ = newring(ringShards) // 16 返回storer的实现ring
	c.mu.Unlock()
}

func (c *Cache) initForGroup() {
	if !atomic.CompareAndSwapUint32(&c.initializedCount, 0, 1) {
		return
	}

	c.mu.Lock()
	c.storeForGroup, _ = newRingForGroup(ringShards) // 16 返回storer的实现ring
	c.mu.Unlock()
}

// Free releases the underlying store and memory held by the Cache.
func (c *Cache) Free() {
	if !atomic.CompareAndSwapUint32(&c.initializedCount, 1, 0) {
		return
	}

	c.mu.Lock()
	c.store = emptyStore{} // emptyStore{}就是 struct{} 指向空
	c.storeForGroup = emptyStoreForGroup{}
	c.mu.Unlock()
}

// Write writes the set of values for the key to the cache. This function is goroutine-safe.
// It returns an error if the cache will exceed its max size by adding the new values.
func (c *Cache) Write(key []byte, values []Value) error {
	c.init()
	addedSize := uint64(Values(values).Size())

	// Enough room in the cache?
	limit := c.maxSize
	n := c.Size() + addedSize

	// 写不下了返回错误
	if limit > 0 && n > limit {
		atomic.AddInt64(&c.stats.WriteErr, 1)
		return ErrCacheMemorySizeLimitExceeded(n, limit)
	}

	// 调用store的write函数
	newKey, err := c.store.write(key, values)
	if err != nil {
		atomic.AddInt64(&c.stats.WriteErr, 1)
		return err
	}

	if newKey {
		addedSize += uint64(len(key))
	}
	// Update the cache size and the memory size stat.
	c.increaseSize(addedSize)
	c.updateMemSize(int64(addedSize))
	atomic.AddInt64(&c.stats.WriteOK, 1)

	return nil
}

// WriteMulti writes the map of keys and associated values to the cache. This
// function is goroutine-safe. It returns an error if the cache will exceeded
// its max size by adding the new values.  The write attempts to write as many
// values as possible.  If one key fails, the others can still succeed and an
// error will be returned.
// nxl 这里就是真正的把一行数据转换成了可写入的格式
func (c *Cache) WriteMulti(values map[string][]Value) error {
	c.init()
	var addedSize uint64
	for _, v := range values {
		addedSize += uint64(Values(v).Size())
	}

	// Enough room in the cache?
	limit := c.maxSize // maxSize is safe for reading without a lock.
	n := c.Size() + addedSize
	if limit > 0 && n > limit {
		atomic.AddInt64(&c.stats.WriteErr, 1)
		return ErrCacheMemorySizeLimitExceeded(n, limit)
	}

	var werr error
	c.mu.RLock()
	store := c.store
	c.mu.RUnlock()

	// We'll optimistially set size here, and then decrement it for write errors.
	c.increaseSize(addedSize)
	// 写入cache竟然是一条一条写入
	for k, v := range values {
		// nxl 在这里写
		newKey, err := store.write([]byte(k), v) // nxl 在这一行写入storer
		if err != nil {
			// The write failed, hold onto the error and adjust the size delta.
			werr = err
			addedSize -= uint64(Values(v).Size())
			c.decreaseSize(uint64(Values(v).Size()))
		}
		// nxl 判断是不是新的键，如果是新的key值，则也会把key的大小加在缓存里
		if newKey {
			addedSize += uint64(len(k))
			c.increaseSize(uint64(len(k)))
		}
	}

	// Some points in the batch were dropped.  An error is returned so
	// error stat is incremented as well.
	if werr != nil {
		atomic.AddInt64(&c.stats.WriteDropped, 1)
		atomic.AddInt64(&c.stats.WriteErr, 1)
	}

	// Update the memory size stat
	c.updateMemSize(int64(addedSize))
	atomic.AddInt64(&c.stats.WriteOK, 1)

	c.mu.Lock()
	c.lastWriteTime = time.Now()
	c.mu.Unlock()

	return werr
}

func (c *Cache) WriteMultiForGroup(values map[string]map[string]MultiValuesList) error {
	c.initForGroup()
	var addedSize uint64
	// 先统计v的大小
	for _, seqMap := range values {
		for _, v := range seqMap {
			addedSize += uint64(v.Size())
		}
	}

	// Enough room in the cache?
	limit := c.maxSize // maxSize is safe for reading without a lock.
	n := c.Size() + addedSize
	if limit > 0 && n > limit {
		atomic.AddInt64(&c.stats.WriteErr, 1)
		return ErrCacheMemorySizeLimitExceeded(n, limit)
	}

	var werr error
	c.mu.RLock()
	store := c.storeForGroup
	c.mu.RUnlock()

	// We'll optimistially set size here, and then decrement it for write errors.
	c.increaseSize(addedSize)
	// 写入cache竟然是一条一条写入
	for groupKey, seqMap := range values {
		for sequenceKey, v := range seqMap {
			newGroupKey, newSequenceKey, err := store.writeForGroup([]byte(groupKey), []byte(sequenceKey), v) // nxl 在这一行写入storer
			if err != nil {
				// The write failed, hold onto the error and adjust the size delta.
				werr = err
				addedSize -= uint64(v.Size())
				c.decreaseSize(uint64(v.Size()))
			}
			// nxl 判断是不是新的键，如果是新的key值，则也会把key的大小加在缓存里
			if newGroupKey {
				addedSize += uint64(len(groupKey))
				c.increaseSize(uint64(len(groupKey)))
			}
			if newSequenceKey {
				addedSize += uint64(len(sequenceKey))
				c.increaseSize(uint64(len(sequenceKey)))
			}
		}
	}

	// Some points in the batch were dropped.  An error is returned so
	// error stat is incremented as well.
	if werr != nil {
		atomic.AddInt64(&c.stats.WriteDropped, 1)
		atomic.AddInt64(&c.stats.WriteErr, 1)
	}

	// Update the memory size stat
	c.updateMemSize(int64(addedSize))
	atomic.AddInt64(&c.stats.WriteOK, 1)

	c.mu.Lock()
	c.lastWriteTime = time.Now()
	c.mu.Unlock()

	//// 写日志看看
	//c.CacheMemSizeAndCount()
	//

	return werr
}

// Snapshot takes a snapshot of the current cache, adds it to the slice of caches that
// are being flushed, and resets the current cache with new values.
func (c *Cache) Snapshot() (*Cache, error) {
	c.init()

	c.mu.Lock() // 这里访问Cache加锁了
	defer c.mu.Unlock()

	// 如果正在进行快照则返回错误
	if c.snapshotting { // bool类型
		return nil, ErrSnapshotInProgress
	}

	c.snapshotting = true
	c.snapshotAttempts++ // increment the number of times we tried to do this 试图快照的个数

	// 需要快照时，但是此时快照不存在，则新建一个
	// If no snapshot exists, create a new one, otherwise update the existing snapshot
	if c.snapshot == nil {
		store, err := newring(ringShards)
		if err != nil {
			return nil, err
		}

		c.snapshot = &Cache{
			store: store,
		}
	}

	// Did a prior snapshot exist that failed?  If so, return the existing
	// snapshot to retry.
	if c.snapshot.Size() > 0 {
		return c.snapshot, nil
	}

	c.snapshot.store, c.store = c.store, c.snapshot.store // 交换当前快照和Cache的内容，此处会阻塞Cache的写入，优化点
	snapshotSize := c.Size()

	// Save the size of the snapshot on the snapshot cache
	atomic.StoreUint64(&c.snapshot.size, snapshotSize)
	// Save the size of the snapshot on the live cache
	atomic.StoreUint64(&c.snapshotSize, snapshotSize)

	// Reset the cache's store.
	c.store.reset() // reset当前这个Cache
	atomic.StoreUint64(&c.size, 0)
	c.lastSnapshot = time.Now()

	c.updateCachedBytes(snapshotSize) // increment the number of bytes added to the snapshot
	c.updateSnapshots()

	return c.snapshot, nil
}

// 适配组模型的snapshot
func (c *Cache) SnapshotForGroup() (*Cache, error) {
	c.init()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.snapshotting {
		return nil, ErrSnapshotInProgress
	}

	c.snapshotting = true
	c.snapshotAttempts++ // increment the number of times we tried to do this 试图快照的个数

	if c.snapshot == nil {
		store, err := newRingForGroup(ringShards)
		if err != nil {
			return nil, err
		}

		c.snapshot = &Cache{
			storeForGroup: store,
		}
	}

	// Did a prior snapshot exist that failed?  If so, return the existing
	// snapshot to retry.
	if c.snapshot.Size() > 0 {
		return c.snapshot, nil
	}

	c.snapshot.storeForGroup, c.storeForGroup = c.storeForGroup, c.snapshot.storeForGroup // 交换当前快照和Cache的内容，此处会阻塞Cache的写入，优化点
	snapshotSize := c.Size()

	// Save the size of the snapshot on the snapshot cache
	atomic.StoreUint64(&c.snapshot.size, snapshotSize)
	// Save the size of the snapshot on the live cache
	atomic.StoreUint64(&c.snapshotSize, snapshotSize)

	// Reset the cache's store.
	c.storeForGroup.resetForGroup() // reset当前这个Cache
	atomic.StoreUint64(&c.size, 0)
	c.lastSnapshot = time.Now()

	c.updateCachedBytes(snapshotSize) // increment the number of bytes added to the snapshot
	c.updateSnapshots()

	return c.snapshot, nil
}

// Deduplicate sorts the snapshot before returning it. The compactor and any queries
// coming in while it writes will need the values sorted.
func (c *Cache) Deduplicate() {
	c.mu.RLock()
	store := c.store
	c.mu.RUnlock()

	// Apply a function that simply calls deduplicate on each entry in the ring.
	// apply cannot return an error in this invocation.
	_ = store.apply(func(_ []byte, e *entry) error { e.deduplicate(); return nil })
}

func (c *Cache) DeduplicateForGroup() {
	c.mu.RLock()
	store := c.storeForGroup
	c.mu.RUnlock()

	// Apply a function that simply calls deduplicate on each entry in the ring.
	// apply cannot return an error in this invocation.
	_ = store.applyForGroup(func(_ []byte, _ []byte, e *MultiEntry) error { e.deduplicate(); return nil })
}

// ClearSnapshot removes the snapshot cache from the list of flushing caches and
// adjusts the size.
func (c *Cache) ClearSnapshot(success bool) {
	c.init()

	c.mu.RLock() // 获取快照的时候加个锁
	snapStore := c.snapshot.store
	c.mu.RUnlock()

	// reset the snapshot store outside of the write lock
	if success {
		snapStore.reset()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.snapshotting = false

	if success {
		c.snapshotAttempts = 0
		c.updateMemSize(-int64(atomic.LoadUint64(&c.snapshotSize))) // decrement the number of bytes in cache

		// Reset the snapshot to a fresh Cache.
		c.snapshot = &Cache{
			store: c.snapshot.store,
		}

		atomic.StoreUint64(&c.snapshotSize, 0)
		c.updateSnapshots()
	}
}

func (c *Cache) ClearSnapshotForGroup(success bool) {
	c.init()

	c.mu.RLock() // 获取快照的时候加个锁
	snapStore := c.snapshot.storeForGroup
	c.mu.RUnlock()

	// reset the snapshot store outside of the write lock
	if success {
		snapStore.resetForGroup()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.snapshotting = false

	if success {
		c.snapshotAttempts = 0
		c.updateMemSize(-int64(atomic.LoadUint64(&c.snapshotSize))) // decrement the number of bytes in cache

		// Reset the snapshot to a fresh Cache.
		c.snapshot = &Cache{
			storeForGroup: c.snapshot.storeForGroup,
		}

		atomic.StoreUint64(&c.snapshotSize, 0)
		c.updateSnapshots()
	}
}

// Size returns the number of point-calcuated bytes the cache currently uses.
func (c *Cache) Size() uint64 {
	return atomic.LoadUint64(&c.size) + atomic.LoadUint64(&c.snapshotSize)
}

// increaseSize increases size by delta.
func (c *Cache) increaseSize(delta uint64) {
	atomic.AddUint64(&c.size, delta)
}

// decreaseSize decreases size by delta.
func (c *Cache) decreaseSize(delta uint64) {
	// Per sync/atomic docs, bit-flip delta minus one to perform subtraction within AddUint64.
	atomic.AddUint64(&c.size, ^(delta - 1))
}

// MaxSize returns the maximum number of bytes the cache may consume.
func (c *Cache) MaxSize() uint64 {
	return c.maxSize
}

func (c *Cache) Count() int {
	c.mu.RLock()
	n := c.store.count()
	c.mu.RUnlock()
	return n
}

func (c *Cache) CountForGroup() int {
	c.mu.RLock()
	n := c.storeForGroup.countForGroup()
	c.mu.RUnlock()
	return n
}

// Keys returns a sorted slice of all keys under management by the cache.
func (c *Cache) Keys() [][]byte {
	c.mu.RLock()
	store := c.store
	c.mu.RUnlock()
	return store.keys(true)
}

func (c *Cache) GroupKeys() [][]byte {
	c.mu.RLock()
	store := c.storeForGroup
	c.mu.RUnlock()
	return store.groupKeys(true)
}

func (c *Cache) Split(n int) []*Cache {
	if n == 1 {
		return []*Cache{c}
	}

	caches := make([]*Cache, n)
	storers := c.store.split(n)
	for i := 0; i < n; i++ {
		caches[i] = &Cache{
			store: storers[i],
		}
	}
	return caches
}

func (c *Cache) SplitForGroup(n int) []*Cache {
	if n == 1 {
		return []*Cache{c}
	}

	caches := make([]*Cache, n)
	storers := c.storeForGroup.splitForGroup(n)
	for i := 0; i < n; i++ {
		caches[i] = &Cache{
			storeForGroup: storers[i],
		}
	}
	return caches
}

// Type returns the series type for a key.
func (c *Cache) Type(key []byte) (models.FieldType, error) {
	c.mu.RLock()
	e := c.store.entry(key)
	if e == nil && c.snapshot != nil {
		e = c.snapshot.store.entry(key)
	}
	c.mu.RUnlock()

	if e != nil {
		typ, err := e.InfluxQLType()
		if err != nil {
			return models.Empty, tsdb.ErrUnknownFieldType
		}

		switch typ {
		case influxql.Float:
			return models.Float, nil
		case influxql.Integer:
			return models.Integer, nil
		case influxql.Unsigned:
			return models.Unsigned, nil
		case influxql.Boolean:
			return models.Boolean, nil
		case influxql.String:
			return models.String, nil
		}
	}

	return models.Empty, tsdb.ErrUnknownFieldType
}

// Values returns a copy of all values, deduped and sorted, for the given key.
func (c *Cache) Values(key []byte) Values {
	var snapshotEntries *entry

	c.mu.RLock()
	e := c.store.entry(key)
	if c.snapshot != nil {
		snapshotEntries = c.snapshot.store.entry(key)
	}
	c.mu.RUnlock()

	if e == nil {
		if snapshotEntries == nil {
			// No values in hot cache or snapshots.
			return nil
		}
	} else {
		e.deduplicate()
	}

	// Build the sequence of entries that will be returned, in the correct order.
	// Calculate the required size of the destination buffer.
	var entries []*entry
	sz := 0

	if snapshotEntries != nil {
		snapshotEntries.deduplicate() // guarantee we are deduplicated
		entries = append(entries, snapshotEntries)
		sz += snapshotEntries.count()
	}

	if e != nil {
		entries = append(entries, e)
		sz += e.count()
	}

	// Any entries? If not, return.
	if sz == 0 {
		return nil
	}

	// Create the buffer, and copy all hot values and snapshots. Individual
	// entries are sorted at this point, so now the code has to check if the
	// resultant buffer will be sorted from start to finish.
	values := make(Values, sz)
	n := 0
	for _, e := range entries {
		e.mu.RLock()
		n += copy(values[n:], e.values)
		e.mu.RUnlock()
	}
	values = values[:n]
	values = values.Deduplicate()

	return values
}

func (c *Cache) ValuesForGroup2(groupKey []byte, sequenceKeys []string) map[string]MultiValuesList {
	var snapshotEntries map[string]*MultiEntry

	c.mu.RLock()
	e := c.storeForGroup.entryForGroup2(groupKey)
	if c.snapshot != nil {
		snapshotEntries = c.snapshot.storeForGroup.entryForGroup2(groupKey)
	}
	c.mu.RUnlock()

	if e == nil {
		if snapshotEntries == nil {
			// No values in hot cache or snapshots.
			return nil
		}
	} else {
		for _, entry := range e {
			if entry != nil {
				entry.deduplicate()
			}
		}
	}

	// Build the sequence of entries that will be returned, in the correct order.
	// Calculate the required size of the destination buffer.

	//var values map[string]MultiValuesList
	values := make(map[string]MultiValuesList)
	for _, sequenceKey := range sequenceKeys {
		if e[sequenceKey] == nil {
			if snapshotEntries[sequenceKey] == nil {
				return nil
			}
		} else {
			e[sequenceKey].deduplicate()
		}

		// Build the sequence of entries that will be returned, in the correct order.
		// Calculate the required size of the destination buffer.
		var entries []*MultiEntry
		sz := 0

		if snapshotEntries[sequenceKey] != nil {
			snapshotEntries[sequenceKey].deduplicate() // guarantee we are deduplicated
			entries = append(entries, snapshotEntries[sequenceKey])
			sz += snapshotEntries[sequenceKey].count()
		}

		if e[sequenceKey] != nil {
			entries = append(entries, e[sequenceKey])
			sz += e[sequenceKey].count()
		}

		// Any entries? If not, return.
		if sz == 0 {
			values[sequenceKey] = nil
			continue
		}

		// Create the buffer, and copy all hot values and snapshots. Individual
		// entries are sorted at this point, so now the code has to check if the
		// resultant buffer will be sorted from start to finish.
		values[sequenceKey] = make(MultiValuesList, sz)
		n := 0
		for _, e := range entries {
			e.mu.RLock()
			n += copy(values[sequenceKey][n:], e.values)
			e.mu.RUnlock()
		}
		values[sequenceKey] = values[sequenceKey][:n]
		values[sequenceKey] = values[sequenceKey].Deduplicate()
	}
	return values

}

// 根据两个key获取multiValue
func (c *Cache) ValuesForGroup(groupKey, sequenceKey []byte) MultiValuesList {
	var snapshotEntries *MultiEntry

	c.mu.RLock()
	e := c.storeForGroup.entryForGroup(groupKey, sequenceKey)
	if c.snapshot != nil {
		snapshotEntries = c.snapshot.storeForGroup.entryForGroup(groupKey, sequenceKey)
	}
	c.mu.RUnlock()

	if e == nil {
		if snapshotEntries == nil {
			// No values in hot cache or snapshots.
			return nil
		}
	} else {
		e.deduplicate()
	}

	// Build the sequence of entries that will be returned, in the correct order.
	// Calculate the required size of the destination buffer.
	var entries []*MultiEntry
	sz := 0

	if snapshotEntries != nil {
		snapshotEntries.deduplicate() // guarantee we are deduplicated
		entries = append(entries, snapshotEntries)
		sz += snapshotEntries.count()
	}

	if e != nil {
		entries = append(entries, e)
		sz += e.count()
	}

	// Any entries? If not, return.
	if sz == 0 {
		return nil
	}

	// Create the buffer, and copy all hot values and snapshots. Individual
	// entries are sorted at this point, so now the code has to check if the
	// resultant buffer will be sorted from start to finish.
	values := make(MultiValuesList, sz)
	n := 0
	for _, e := range entries {
		e.mu.RLock()
		n += copy(values[n:], e.values)
		e.mu.RUnlock()
	}
	values = values[:n]
	values = values.Deduplicate()

	return values
}

// Delete removes all values for the given keys from the cache.
func (c *Cache) Delete(keys [][]byte) {
	c.DeleteRange(keys, math.MinInt64, math.MaxInt64)
}

// DeleteRange removes the values for all keys containing points
// with timestamps between between min and max from the cache.
//
// TODO(edd): Lock usage could possibly be optimised if necessary.
func (c *Cache) DeleteRange(keys [][]byte, min, max int64) {
	c.init()

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, k := range keys {
		// Make sure key exist in the cache, skip if it does not
		e := c.store.entry(k)
		if e == nil {
			continue
		}

		origSize := uint64(e.size())
		if min == math.MinInt64 && max == math.MaxInt64 {
			c.decreaseSize(origSize + uint64(len(k)))
			c.store.remove(k)
			continue
		}

		e.filter(min, max)
		if e.count() == 0 {
			c.store.remove(k)
			c.decreaseSize(origSize + uint64(len(k)))
			continue
		}

		c.decreaseSize(origSize - uint64(e.size()))
	}
	atomic.StoreInt64(&c.stats.MemSizeBytes, int64(c.Size()))
}

// SetMaxSize updates the memory limit of the cache.
func (c *Cache) SetMaxSize(size uint64) {
	c.mu.Lock()
	c.maxSize = size
	c.mu.Unlock()
}

// values returns the values for the key. It assumes the data is already sorted.
// It doesn't lock the cache but it does read-lock the entry if there is one for the key.
// values should only be used in compact.go in the CacheKeyIterator.
func (c *Cache) values(key []byte) Values {
	e := c.store.entry(key)
	if e == nil {
		return nil
	}
	e.mu.RLock()
	v := e.values
	e.mu.RUnlock()
	return v
}

func (c *Cache) sequenceMap(groupKey []byte) map[string]*MultiEntry {
	e := c.storeForGroup.sequenceMap(groupKey)
	if e == nil {
		return nil
	}
	return e
}

// ApplyEntryFn applies the function f to each entry in the Cache.
// ApplyEntryFn calls f on each entry in turn, within the same goroutine.
// It is safe for use by multiple goroutines.
func (c *Cache) ApplyEntryFn(f func(key []byte, entry *entry) error) error {
	c.mu.RLock()
	store := c.store
	c.mu.RUnlock()
	return store.applySerial(f)
}

// CacheLoader 处理一组 WAL 段文件，并用数据加载缓存
// 包含在这些文件中。提供的文件的处理发生在
// 它们存在于文件切片中的顺序。
// CacheLoader processes a set of WAL segment files, and loads a cache with the data
// contained within those files.  Processing of the supplied files take place in the
// order they exist in the files slice.
type CacheLoader struct {
	files []string

	Logger *zap.Logger
}

// NewCacheLoader returns a new instance of a CacheLoader.
func NewCacheLoader(files []string) *CacheLoader {
	return &CacheLoader{
		files:  files,
		Logger: zap.NewNop(),
	}
}

// Load returns a cache loaded with the data contained within the segment files.
// If, during reading of a segment file, corruption is encountered, that segment
// file is truncated up to and including the last valid byte, and processing
// continues with the next segment file.
func (cl *CacheLoader) Load(cache *Cache) error {

	var r *WALSegmentReader
	for _, fn := range cl.files {
		if err := func() error {
			f, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0666)
			if err != nil {
				return err
			}
			defer f.Close()

			// Log some information about the segments.
			stat, err := os.Stat(f.Name())
			if err != nil {
				return err
			}
			cl.Logger.Info("Reading file", zap.String("path", f.Name()), zap.Int64("size", stat.Size()))

			// Nothing to read, skip it
			if stat.Size() == 0 {
				return nil
			}

			if r == nil {
				r = NewWALSegmentReader(f)
				defer r.Close()
			} else {
				r.Reset(f)
			}

			for r.Next() {
				entry, err := r.Read()
				if err != nil {
					n := r.Count()
					cl.Logger.Info("File corrupt", zap.Error(err), zap.String("path", f.Name()), zap.Int64("pos", n))
					if err := f.Truncate(n); err != nil {
						return err
					}
					break
				}

				switch t := entry.(type) {
				case *WriteWALEntry:
					if err := cache.WriteMulti(t.Values); err != nil {
						return err
					}
				case *DeleteRangeWALEntry:
					cache.DeleteRange(t.Keys, t.Min, t.Max)
				case *DeleteWALEntry:
					cache.Delete(t.Keys)
				}
			}

			return r.Close()
		}(); err != nil {
			return err
		}
	}
	return nil
}

// WithLogger sets the logger on the CacheLoader.
func (cl *CacheLoader) WithLogger(log *zap.Logger) {
	cl.Logger = log.With(zap.String("service", "cacheloader"))
}

func (c *Cache) LastWriteTime() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastWriteTime
}

// UpdateAge updates the age statistic based on the current time.
func (c *Cache) UpdateAge() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ageStat := int64(time.Since(c.lastSnapshot) / time.Millisecond)
	atomic.StoreInt64(&c.stats.CacheAgeMs, ageStat)
}

// UpdateCompactTime updates WAL compaction time statistic based on d.
func (c *Cache) UpdateCompactTime(d time.Duration) {
	atomic.AddInt64(&c.stats.WALCompactionTimeMs, int64(d/time.Millisecond))
}

// updateCachedBytes increases the cachedBytes counter by b.
func (c *Cache) updateCachedBytes(b uint64) {
	atomic.AddInt64(&c.stats.CachedBytes, int64(b))
}

// updateMemSize updates the memSize level by b.
func (c *Cache) updateMemSize(b int64) {
	atomic.AddInt64(&c.stats.MemSizeBytes, b)
}

func valueType(v Value) byte {
	switch v.(type) {
	case FloatValue:
		return 1
	case IntegerValue:
		return 2
	case StringValue:
		return 3
	case BooleanValue:
		return 4
	default:
		return 0
	}
}

// updateSnapshots updates the snapshotsCount and the diskSize levels.
func (c *Cache) updateSnapshots() {
	// Update disk stats
	atomic.StoreInt64(&c.stats.DiskSizeBytes, int64(atomic.LoadUint64(&c.snapshotSize)))
	atomic.StoreInt64(&c.stats.SnapshotCount, int64(c.snapshotAttempts))
}

type emptyStore struct{}
type emptyStoreForGroup struct{}

func (e emptyStore) entry(key []byte) *entry                        { return nil }
func (e emptyStore) write(key []byte, values Values) (bool, error)  { return false, nil }
func (e emptyStore) add(key []byte, entry *entry)                   {}
func (e emptyStore) remove(key []byte)                              {}
func (e emptyStore) keys(sorted bool) [][]byte                      { return nil }
func (e emptyStore) apply(f func([]byte, *entry) error) error       { return nil }
func (e emptyStore) applySerial(f func([]byte, *entry) error) error { return nil }
func (e emptyStore) reset()                                         {}
func (e emptyStore) split(n int) []storer                           { return nil }
func (e emptyStore) count() int                                     { return 0 }
func (e emptyStore) memSizeAndCount() (keyMemSize uint64, valueMemSize uint64, keyCount uint64, valueCount uint64) {
	return
}

func (e emptyStoreForGroup) sequenceMap(groupKey []byte) map[string]*MultiEntry { return nil }
func (e emptyStoreForGroup) entryForGroup(groupKey []byte, sequenceKey []byte) *MultiEntry {
	return nil
} // Get an entry by its key.
func (e emptyStoreForGroup) entryForGroup2(groupKey []byte) map[string]*MultiEntry {
	return nil
} // Get an entry by its key.
func (e emptyStoreForGroup) writeForGroup(groupKey []byte, sequenceKey []byte, values MultiValuesList) (bool, bool, error) {
	return false, false, nil
}                                                                                               // Write an entry to the store.
func (e emptyStoreForGroup) addForGroup(groupKey []byte, sequenceKey []byte, entry *MultiEntry) {} // Add a new entry to the store.
func (e emptyStoreForGroup) removeForGroup(groupKey []byte, sequenceKey []byte)                 {} // Remove an entry from the store.
func (e emptyStoreForGroup) resetForGroup()                                                     {} // Reset the store to an initial unused state.
func (e emptyStoreForGroup) splitForGroup(n int) []storerForGroup                               { return nil }
func (e emptyStoreForGroup) applyForGroup(f func([]byte, []byte, *MultiEntry) error) error {
	return nil
} // Split splits the store into n stores
func (e emptyStoreForGroup) memSizeAndCount() (keyMemSize uint64, valueMemSize uint64, gKeyMemSize uint64, sKeyMemSize uint64,
	groupKeyCount uint64, sequenceKeyCount uint64, valueCount uint64) {
	return 0, 0, 0, 0, 0, 0, 0
}
func (e emptyStoreForGroup) countForGroup() int             { return 0 }
func (e emptyStoreForGroup) groupKeys(sorted bool) [][]byte { return nil }
