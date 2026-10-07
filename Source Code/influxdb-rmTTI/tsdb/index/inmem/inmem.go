/*
Package inmem implements a shared, in-memory index for each database.

The in-memory index is the original index implementation and provides fast
access to index data. However, it also forces high memory usage for large
datasets and can cause OOM errors.

Index is the shared index structure that provides most of the functionality.
However, ShardIndex is a light per-shard wrapper that adapts this original
shared index format to the new per-shard format.
*/
package inmem

import (
	"errors"
	"fmt"
	"github.com/influxdata/influxdb/nxl"
	"github.com/influxdata/roaring"
	"os"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bytesutil"
	"github.com/influxdata/influxdb/pkg/escape"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/estimator/hll"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

// IndexName is the name of this index.
const IndexName = tsdb.InmemIndexName

func init() {
	tsdb.NewInmemIndex = func(name string, sfile *tsdb.SeriesFile) (interface{}, error) { return NewIndex(name, sfile), nil }

	tsdb.RegisterIndex(IndexName, func(id uint64, database, path string, seriesIDSet *tsdb.SeriesIDSet, sfile *tsdb.SeriesFile, opt tsdb.EngineOptions) tsdb.Index {
		return NewShardIndex(id, seriesIDSet, opt)
	})
}

const secondWindow = 3600

// Index is the in memory index of a collection of measurements, time
// series, and their tags. Exported functions are goroutine safe while
// un-exported functions assume the caller will use the appropriate locks.
type Index struct {
	mu sync.RWMutex

	database string
	sfile    *tsdb.SeriesFile
	fieldset *tsdb.MeasurementFieldSet

	// In-memory metadata index, built on load and updated when new series come in
	measurements map[string]*measurement // measurement name to object and index
	series       map[string]*series      // map series key to the Series object

	seriesSketch, seriesTSSketch             estimator.Sketch
	measurementsSketch, measurementsTSSketch estimator.Sketch

	AdjacencyMatrix map[int]map[int]map[int]*EdgeTimeIndex

	indexInitStartTimestamp time.Time
	indexInitEndTimestamp   time.Time

	// Mutex to control rebuilds of the index
	rebuildQueue sync.Mutex
}

type EdgeTimeIndex struct {
	mu sync.RWMutex
	// 最近1小时的秒级环形位图（固定长度 3600 bit）
	secondBitmap []uint64 // 3600/64 = 57 个 uint64
	lastCleaned  int64    // 最后一次“清理/推进”到的秒时间戳（窗口右端时刻）

	// 更粗粒度的历史索引（压缩位图）
	minuteIndex *roaring.Bitmap // 存全局分钟编号：ts/60
	hourIndex   *roaring.Bitmap // 存全局小时编号：ts/3600
}

// NewIndex returns a new initialized Index.
func NewIndex(database string, sfile *tsdb.SeriesFile) *Index {
	index := &Index{
		database:        database,
		sfile:           sfile,
		measurements:    make(map[string]*measurement),
		series:          make(map[string]*series),
		AdjacencyMatrix: make(map[int]map[int]map[int]*EdgeTimeIndex), // 初始化三维邻接矩阵
	}

	index.seriesSketch = hll.NewDefaultPlus()
	index.seriesTSSketch = hll.NewDefaultPlus()
	index.measurementsSketch = hll.NewDefaultPlus()
	index.measurementsTSSketch = hll.NewDefaultPlus()

	return index
}

// 创建单条边的时间索引
func NewEdgeTimeIndex() *EdgeTimeIndex {
	//now := time.Now().Unix()
	return &EdgeTimeIndex{
		secondBitmap: make([]uint64, (secondWindow+63)/64),
		lastCleaned:  0,
		minuteIndex:  roaring.New(),
		hourIndex:    roaring.New(),
	}
}

func setBit(bitmap []uint64, idx int) {
	word := idx / 64
	bit := uint(idx % 64)
	bitmap[word] |= (uint64(1) << bit)
}

func clearBit(bitmap []uint64, idx int) {
	word := idx / 64
	bit := uint(idx % 64)
	bitmap[word] &^= (uint64(1) << bit)
}

func getBit(bitmap []uint64, idx int) bool {
	word := idx / 64
	bit := uint(idx % 64)
	return (bitmap[word] & (uint64(1) << bit)) != 0
}

/*
advance：将“秒级环形窗口”推进到指定时间 now（单位：秒），
清除滚出窗口的槽位，保证环形位图中的每个槽位仅代表最近1小时内的某一秒。
*/
func (e *EdgeTimeIndex) advance(now int64) {
	if now <= e.lastCleaned {
		// 允许乱序数据：如果插入的是过去的 ts，只要在窗口内，不需要推进。
		return
	}
	delta := now - e.lastCleaned
	switch {
	case delta >= secondWindow:
		// 超过整整一个窗口，全部清零更快
		for i := range e.secondBitmap {
			e.secondBitmap[i] = 0
		}
	default:
		// 逐秒清除离开窗口的槽位： (lastCleaned, now] 这些秒各自的索引需要清空
		for s := e.lastCleaned + 1; s <= now; s++ {
			idx := int(s % secondWindow)
			clearBit(e.secondBitmap, idx)
		}
	}
	e.lastCleaned = now
}

/*
AddEvent：插入一条边的到达事件，ts 为时间戳（秒）。
- 推进秒级环形窗口到 ts，清理过期槽位；
- 如果 ts 落在当前窗口 (lastCleaned-3599, lastCleaned] 内，则置位；
- 同时更新分钟/小时/日级压缩位图（用于历史查询）。
*/
func (e *EdgeTimeIndex) AddEvent(ts int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 推进秒级窗口
	e.advance(ts)

	// 写入秒级位图
	windowStart := e.lastCleaned - (secondWindow - 1)
	if ts >= windowStart && ts <= e.lastCleaned {
		setBit(e.secondBitmap, int(ts%secondWindow))
	}

	// 更新历史索引
	e.minuteIndex.Add(uint32(ts / 60))
	e.hourIndex.Add(uint32(ts / 3600))
}

/*
ExistsAt：判断该边在任意时间戳 ts 是否“出现过”
*/
func (e *EdgeTimeIndex) ExistsAt(ts int64) bool {
	// 读锁快速判断是否需要推进
	e.mu.RLock()
	needAdvance := ts > e.lastCleaned
	e.mu.RUnlock()

	if needAdvance {
		e.mu.Lock()
		e.advance(ts)
		e.mu.Unlock()
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.hourIndex.Contains(uint32(ts / 3600)) {
		return false
	}
	if !e.minuteIndex.Contains(uint32(ts / 60)) {
		return false
	}

	windowStart := e.lastCleaned - (secondWindow - 1)
	if ts >= windowStart && ts <= e.lastCleaned {
		return getBit(e.secondBitmap, int(ts%secondWindow))
	}
	return true
}

func (e *EdgeTimeIndex) ExistsRange(startSec, endSec int64) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if startSec > endSec {
		return false
	}

	right := e.lastCleaned
	left := right - secondWindow + 1
	if endSec < left || startSec > right {
		return false
	}

	if startSec < left {
		startSec = left
	}
	if endSec > right {
		endSec = right
	}

	for ts := startSec; ts <= endSec; ts++ {
		idx := int(ts % secondWindow)
		if getBit(e.secondBitmap, idx) {
			return true
		}
	}
	return false
}

/************* AdjacencyMatrix：三维邻接矩阵维护 *************/

// 取出或创建一个位置（srcHash, dstHash, bucket）对应的 EdgeTimeIndex
func (i *Index) getOrCreate(srcHash, dstHash, bucket int) *EdgeTimeIndex {
	i.mu.Lock()
	defer i.mu.Unlock()

	l1, ok := i.AdjacencyMatrix[srcHash]
	if !ok {
		l1 = make(map[int]map[int]*EdgeTimeIndex)
		i.AdjacencyMatrix[srcHash] = l1
	}
	l2, ok := l1[dstHash]
	if !ok {
		l2 = make(map[int]*EdgeTimeIndex)
		l1[dstHash] = l2
	}
	e, ok := l2[bucket]
	if !ok {
		e = NewEdgeTimeIndex()
		l2[bucket] = e
	}
	return e
}

// 插入更新：在 (srcHash, dstHash, bucket) 位置写入一条时间戳 ts 的边事件
func (i *Index) AddEdgeEvent(srcHash, dstHash, bucket int, ts int64) {
	if e := i.get(srcHash, dstHash, bucket); e != nil {
		e.AddEvent(ts)
		return
	}

	e := i.getOrCreate(srcHash, dstHash, bucket)
	e.AddEvent(ts)
}
func (i *Index) get(srcHash, dstHash, bucket int) *EdgeTimeIndex {
	i.mu.RLock()
	defer i.mu.RUnlock()

	l1, ok := i.AdjacencyMatrix[srcHash]
	if !ok {
		return nil
	}
	l2, ok := l1[dstHash]
	if !ok {
		return nil
	}
	e, ok := l2[bucket]
	if !ok {
		return nil
	}
	return e
}

// 查询：该边在时间戳 ts 是否存在
func (i *Index) EdgeExistsAt(srcHash, dstHash, bucket int, ts int64) bool {
	i.mu.RLock()
	defer i.mu.RUnlock()

	l1 := i.AdjacencyMatrix[srcHash]
	if l1 == nil {
		return false
	}
	l2 := l1[dstHash]
	if l2 == nil {
		return false
	}
	e := l2[bucket]
	if e == nil {
		return false
	}
	return e.ExistsAt(ts)
}

func (i *Index) NodeExistsAtRange(srcHash int, start, end int64) bool {
	i.mu.RLock()
	// 1. 获取该源节点对应的所有目标节点层
	l1, ok := i.AdjacencyMatrix[srcHash]
	if !ok {
		i.mu.RUnlock()
		return false
	}

	var indices []*EdgeTimeIndex
	for _, l2 := range l1 {
		for _, e := range l2 {
			if e != nil {
				indices = append(indices, e)
			}
		}
	}
	i.mu.RUnlock()

	// 2. 遍历所有相关的 EdgeTimeIndex，检查是否存在于该时间段内
	// 这里复用了你已经在 EdgeExistsAtRange 中写好的核心逻辑
	for _, e := range indices {
		if i.edgeIndexMatchRange(e, start, end) {
			return true
		}
	}

	return false
}

// 辅助方法：提取 EdgeExistsAtRange 的核心判断逻辑，避免重复代码
func (i *Index) edgeIndexMatchRange(e *EdgeTimeIndex, start, end int64) bool {
	if e == nil {
		return false
	}

	// 时间单位转换逻辑 (保持与你代码中一致)
	toSec := func(ts int64) int64 {
		if ts > 1e18 {
			return ts / 1e9
		} // ns
		if ts > 1e12 {
			return ts / 1e6
		} // ms
		return ts
	}
	startSec, endSec := toSec(start), toSec(end)

	// 1. 检查秒级环形位图 (针对最近1小时窗口)
	if e.ExistsRange(startSec, endSec) {
		return true
	}

	// 2. 检查 Roaring Bitmap 分钟级索引
	startMin, endMin := uint32(startSec/60), uint32(endSec/60)
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 优化：使用 Roaring Bitmap 的 Intersects 或是 Iterator
	// 这里采用你原本的循环判断逻辑
	for m := startMin; m <= endMin; m++ {
		if e.minuteIndex.Contains(m) {
			return true
		}
	}

	return false
}

func (i *Index) EdgeExistsAtRange(srcHash, dstHash, bucket int, start, end int64) bool {
	if srcHash == -1 && dstHash == -1 && bucket == -1 {
		return true
	}
	if start == -9223372036854775806 || end == 9223372036854775806 {
		return true
	}
	if dstHash == -1 && bucket == -1 {
		return i.NodeExistsAtRange(srcHash, start, end)
	}
	// 1. 获取对应的 EdgeTimeIndex
	e := i.get(srcHash, dstHash, bucket)

	if e == nil {
		return false
	}

	// 2. 时间戳单位转换 (假设输入为纳秒，转为秒)
	toSec := func(ts int64) int64 {
		if ts > 1e18 {
			return ts / 1e9
		} // ns
		if ts > 1e12 {
			return ts / 1e6
		} // ms
		return ts
	}
	startSec, endSec := toSec(start), toSec(end)

	e.mu.RLock()
	defer e.mu.RUnlock()

	// 3. 第一层过滤：小时级 (Roaring Bitmap)
	startHour, endHour := uint32(startSec/3600), uint32(endSec/3600)
	hasHour := false
	// 检查 hourIndex 是否包含请求范围内的任何一小时
	for h := startHour; h <= endHour; h++ {
		if e.hourIndex.Contains(h) {
			hasHour = true
			break
		}
	}
	if !hasHour {
		return false
	}

	// 4. 第二层过滤：秒级环形位图 (针对最近1小时窗口)
	// 如果查询范围与当前秒级窗口有交集，调用你已有的 ExistsRange
	if e.ExistsRange(startSec, endSec) {
		return true
	}

	// 5. 第三层过滤：分钟级 (Roaring Bitmap)
	startMin, endMin := uint32(startSec/60), uint32(endSec/60)
	for m := startMin; m <= endMin; m++ {
		if e.minuteIndex.Contains(m) {
			return true
		}
	}

	return false
}
func (i *Index) IndexAnalyseForOld() {
	i.mu.RLock()
	defer i.mu.RUnlock()

	// 计算构建时间
	buildTime := i.indexInitEndTimestamp.Nanosecond() - i.indexInitStartTimestamp.Nanosecond()

	// 计算series所占的内存空间大小
	seriesKeyCount := len(i.series)
	seriesKeySize := int(unsafe.Sizeof(i.series))
	for key, value := range i.series {
		seriesKeySize += len(key)
		seriesKeySize += value.bytes()
	}

	// 计算内存倒排索引的大小
	invertIndexSize := uint64(0)
	invertIndexSize2 := uint64(0)       // 自己带的统计大小的
	invertIndexCardinality := uint64(0) // 倒排列表基数
	invertIndexLength := uint64(0)      // 倒排列表长度
	for _, invert := range i.measurements {
		seriesByID := invert.seriesByID
		for _, value := range seriesByID {
			invertIndexSize += 8
			invertIndexSize += uint64(unsafe.Sizeof(value))
		}

		seriesByTagKeyValue := invert.seriesByTagKeyValue

		for tagKey, tagValue := range seriesByTagKeyValue {
			invertIndexSize += uint64(len(tagKey))
			invertIndexSize += uint64(uint(tagValue.bytes()))
			invertIndexCardinality += uint64(tagValue.Cardinality())
			invertIndexLength += uint64(tagValue.invertListLength())
		}
		invertIndexSize2 += uint64(invert.bytes()) // 自己带的统计大小的
	}

	logFile := nxl.IndexFilePath

	logContent := fmt.Sprintf("[ Database Name : %s]\n", i.database) +
		fmt.Sprintf("Index Series个数：%d\n", seriesKeyCount) +
		fmt.Sprintf("Index Series内存占用：%d\n", seriesKeySize) +
		fmt.Sprintf("Index构建时间：%d\n", buildTime) +
		fmt.Sprintf("倒排索引大小（nxl统计）：%d\n", invertIndexSize) +
		fmt.Sprintf("倒排索引大小（自带）：%d\n", invertIndexSize2) +
		fmt.Sprintf("倒排索引基数：%d\n", invertIndexCardinality) +
		fmt.Sprintf("倒排列表总长度：%d\n", invertIndexLength)

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
	file.WriteString(content)
	return nil
}
func (i *Index) UniqueReferenceID() uintptr {
	return uintptr(unsafe.Pointer(i))
}

// Bytes estimates the memory footprint of this Index, in bytes.
func (i *Index) Bytes() int {
	var b int
	i.mu.RLock()
	b += 24 // mu RWMutex is 24 bytes
	b += int(unsafe.Sizeof(i.database)) + len(i.database)
	// Do not count SeriesFile because it belongs to the code that constructed this Index.
	if i.fieldset != nil {
		b += int(unsafe.Sizeof(i.fieldset)) + i.fieldset.Bytes()
	}
	b += int(unsafe.Sizeof(i.fieldset))
	for k, v := range i.measurements {
		b += int(unsafe.Sizeof(k)) + len(k)
		b += int(unsafe.Sizeof(v)) + v.bytes()
	}
	b += int(unsafe.Sizeof(i.measurements))
	for k, v := range i.series {
		b += int(unsafe.Sizeof(k)) + len(k)
		b += int(unsafe.Sizeof(v)) + v.bytes()
	}
	b += int(unsafe.Sizeof(i.series))
	b += int(unsafe.Sizeof(i.seriesSketch)) + i.seriesSketch.Bytes()
	b += int(unsafe.Sizeof(i.seriesTSSketch)) + i.seriesTSSketch.Bytes()
	b += int(unsafe.Sizeof(i.measurementsSketch)) + i.measurementsSketch.Bytes()
	b += int(unsafe.Sizeof(i.measurementsTSSketch)) + i.measurementsTSSketch.Bytes()
	b += 8 // rebuildQueue Mutex is 8 bytes
	i.mu.RUnlock()
	return b
}

func (i *Index) Type() string      { return IndexName }
func (i *Index) Open() (err error) { return nil }
func (i *Index) Close() error      { return nil }

func (i *Index) WithLogger(*zap.Logger) {}

// Database returns the name of the database the index was initialized with.
func (i *Index) Database() string {
	return i.database
}

// Series returns a series by key.
func (i *Index) Series(key []byte) (*series, error) {
	i.mu.RLock()
	s := i.series[string(key)]
	i.mu.RUnlock()
	return s, nil
}

// SeriesSketches returns the sketches for the series.
func (i *Index) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.seriesSketch.Clone(), i.seriesTSSketch.Clone(), nil
}

// Measurement returns the measurement object from the index by the name
func (i *Index) Measurement(name []byte) (*measurement, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.measurements[string(name)], nil
}

// MeasurementExists returns true if the measurement exists.
func (i *Index) MeasurementExists(name []byte) (bool, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.measurements[string(name)] != nil, nil
}

// MeasurementsSketches returns the sketches for the measurements.
func (i *Index) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.measurementsSketch.Clone(), i.measurementsTSSketch.Clone(), nil
}

// MeasurementsByName returns a list of measurements.
func (i *Index) MeasurementsByName(names [][]byte) ([]*measurement, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	a := make([]*measurement, 0, len(names))
	for _, name := range names {
		if m := i.measurements[string(name)]; m != nil {
			a = append(a, m)
		}
	}
	return a, nil
}

// MeasurementIterator returns an iterator over all measurements in the index.
// MeasurementIterator does not support authorization.
func (i *Index) MeasurementIterator() (tsdb.MeasurementIterator, error) {
	names, err := i.MeasurementNamesByExpr(nil, nil)
	if err != nil {
		return nil, err
	}
	return tsdb.NewMeasurementSliceIterator(names), nil
}

// CreateSeriesListIfNotExists adds the series for the given measurement to the
// index and sets its ID or returns the existing series object
func (i *Index) CreateSeriesListIfNotExists(seriesIDSet *tsdb.SeriesIDSet, measurements map[string]int,
	keys, names [][]byte, tagsSlice []models.Tags, opt *tsdb.EngineOptions, ignoreLimits bool) error {

	// Verify that the series will not exceed limit.
	if !ignoreLimits {
		i.mu.RLock()
		if max := opt.Config.MaxSeriesPerDatabase; max > 0 && len(i.series)+len(keys) > max {
			i.mu.RUnlock()
			return errMaxSeriesPerDatabaseExceeded{limit: opt.Config.MaxSeriesPerDatabase}
		}
		i.mu.RUnlock()
	}

	seriesIDs, err := i.sfile.CreateSeriesListIfNotExists(names, tagsSlice)
	if err != nil {
		return err
	}

	i.mu.RLock()
	// If there is a series for this ID, it's already been added.
	seriesList := make([]*series, len(seriesIDs))
	for j, key := range keys {
		seriesList[j] = i.series[string(key)]
	}
	i.mu.RUnlock()

	var hasNewSeries bool
	for _, ss := range seriesList {
		if ss == nil {
			hasNewSeries = true
			continue
		}

		// This series might need to be added to the local bitset, if the series
		// was created on another shard.
		seriesIDSet.Lock()
		if !seriesIDSet.ContainsNoLock(ss.ID) {
			seriesIDSet.AddNoLock(ss.ID)
			measurements[ss.Measurement.Name]++
		}
		seriesIDSet.Unlock()
	}
	if !hasNewSeries {
		return nil
	}

	// get or create the measurement index
	mms := make([]*measurement, len(names))
	for j, name := range names {
		mms[j] = i.CreateMeasurementIndexIfNotExists(name)
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	// Check for the series again under a write lock
	var newSeriesN int
	for j, key := range keys {
		if seriesList[j] != nil {
			continue
		}

		ss := i.series[string(key)]
		if ss == nil {
			newSeriesN++
			continue
		}
		seriesList[j] = ss

		// This series might need to be added to the local bitset, if the series
		// was created on another shard.
		seriesIDSet.Lock()
		if !seriesIDSet.ContainsNoLock(ss.ID) {
			seriesIDSet.AddNoLock(ss.ID)
			measurements[ss.Measurement.Name]++
		}
		seriesIDSet.Unlock()
	}
	if newSeriesN == 0 {
		return nil
	}

	for j, key := range keys {
		// Note, keys may contain duplicates (e.g., because of points for the same series
		// in the same batch). If the duplicate series are new, the index must
		// be rechecked on each iteration.
		if seriesList[j] != nil || i.series[string(key)] != nil {
			continue
		}

		// set the in memory ID for query processing on this shard
		// The series key and tags are clone to prevent a memory leak
		skey := string(key)
		ss := newSeries(seriesIDs[j], mms[j], skey, tagsSlice[j].Clone())
		i.series[skey] = ss

		mms[j].AddSeries(ss)

		// Add the series to the series sketch.
		i.seriesSketch.Add(key)

		// This series needs to be added to the bitset tracking undeleted series IDs.
		seriesIDSet.Lock()
		seriesIDSet.AddNoLock(seriesIDs[j])
		measurements[mms[j].Name]++
		seriesIDSet.Unlock()
	}

	return nil
}

// CreateMeasurementIndexIfNotExists creates or retrieves an in memory index
// object for the measurement
func (i *Index) CreateMeasurementIndexIfNotExists(name []byte) *measurement {
	name = escape.Unescape(name)

	// See if the measurement exists using a read-lock
	i.mu.RLock()
	m := i.measurements[string(name)]
	if m != nil {
		i.mu.RUnlock()
		return m
	}
	i.mu.RUnlock()

	// Doesn't exist, so lock the index to create it
	i.mu.Lock()
	defer i.mu.Unlock()

	// Make sure it was created in between the time we released our read-lock
	// and acquire the write lock
	m = i.measurements[string(name)]
	if m == nil {
		m = newMeasurement(i.database, string(name))
		i.measurements[string(name)] = m

		// Add the measurement to the measurements sketch.
		i.measurementsSketch.Add([]byte(name))
	}
	return m
}

// HasTagKey returns true if tag key exists.
func (i *Index) HasTagKey(name, key []byte) (bool, error) {
	i.mu.RLock()
	mm := i.measurements[string(name)]
	i.mu.RUnlock()

	if mm == nil {
		return false, nil
	}
	return mm.HasTagKey(string(key)), nil
}

// HasTagValue returns true if tag value exists.
func (i *Index) HasTagValue(name, key, value []byte) (bool, error) {
	i.mu.RLock()
	mm := i.measurements[string(name)]
	i.mu.RUnlock()

	if mm == nil {
		return false, nil
	}
	return mm.HasTagKeyValue(key, value), nil
}

// TagValueN returns the cardinality of a tag value.
func (i *Index) TagValueN(name, key []byte) int {
	i.mu.RLock()
	mm := i.measurements[string(name)]
	i.mu.RUnlock()

	if mm == nil {
		return 0
	}
	return mm.CardinalityBytes(key)
}

// MeasurementTagKeysByExpr returns an ordered set of tag keys filtered by an expression.
func (i *Index) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	i.mu.RLock()
	mm := i.measurements[string(name)]
	i.mu.RUnlock()

	if mm == nil {
		return nil, nil
	}
	return mm.TagKeysByExpr(expr)
}

// TagKeyHasAuthorizedSeries determines if there exists an authorized series for
// the provided measurement name and tag key.
func (i *Index) TagKeyHasAuthorizedSeries(auth query.Authorizer, name []byte, key string) bool {
	i.mu.RLock()
	mm := i.measurements[string(name)]
	i.mu.RUnlock()

	if mm == nil {
		return false
	}

	// TODO(edd): This looks like it's inefficient. Since a series can have multiple
	// tag key/value pairs on it, it's possible that the same unauthorised series
	// will be checked multiple times. It would be more efficient if it were
	// possible to get the set of unique series IDs for a given measurement name
	// and tag key.
	var authorized bool
	mm.SeriesByTagKeyValue(key).Range(func(_ string, sIDs seriesIDs) bool {
		if query.AuthorizerIsOpen(auth) {
			authorized = true
			return false
		}

		for _, id := range sIDs {
			s := mm.SeriesByID(id)
			if s == nil {
				continue
			}

			if auth.AuthorizeSeriesRead(i.database, mm.NameBytes, s.Tags) {
				authorized = true
				return false
			}
		}

		// This tag key/value combination doesn't have any authorised series, so
		// keep checking other tag values.
		return true
	})
	return authorized
}

// MeasurementTagKeyValuesByExpr returns a set of tag values filtered by an expression.
//
// See tsm1.Engine.MeasurementTagKeyValuesByExpr for a fuller description of this
// method.
func (i *Index) MeasurementTagKeyValuesByExpr(auth query.Authorizer, name []byte, keys []string, expr influxql.Expr, keysSorted bool) ([][]string, error) {
	i.mu.RLock()
	mm := i.measurements[string(name)]
	i.mu.RUnlock()

	if mm == nil || len(keys) == 0 {
		return nil, nil
	}

	results := make([][]string, len(keys))

	// If we haven't been provided sorted keys, then we need to sort them.
	if !keysSorted {
		sort.Strings(keys)
	}

	ids, _, _ := mm.WalkWhereForSeriesIds(expr)
	if ids.Len() == 0 && expr == nil {
		for ki, key := range keys {
			values := mm.TagValues(auth, key)
			sort.Strings(values)
			results[ki] = values
		}
		return results, nil
	}

	// This is the case where we have filtered series by some WHERE condition.
	// We only care about the tag values for the keys given the
	// filtered set of series ids.

	keyIdxs := make(map[string]int, len(keys))
	for ki, key := range keys {
		keyIdxs[key] = ki
	}

	resultSet := make([]stringSet, len(keys))
	for i := 0; i < len(resultSet); i++ {
		resultSet[i] = newStringSet()
	}

	// Iterate all series to collect tag values.
	for _, id := range ids {
		s := mm.SeriesByID(id)
		if s == nil {
			continue
		}
		if auth != nil && !auth.AuthorizeSeriesRead(i.database, s.Measurement.NameBytes, s.Tags) {
			continue
		}

		// Iterate the tag keys we're interested in and collect values
		// from this series, if they exist.
		for _, t := range s.Tags {
			if idx, ok := keyIdxs[string(t.Key)]; ok {
				resultSet[idx].add(string(t.Value))
			} else if string(t.Key) > keys[len(keys)-1] {
				// The tag key is > the largest key we're interested in.
				break
			}
		}
	}
	for i, s := range resultSet {
		results[i] = s.list()
	}
	return results, nil
}

// ForEachMeasurementTagKey iterates over all tag keys for a measurement.
func (i *Index) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
	// Ensure we do not hold a lock on the index while fn executes in case fn tries
	// to acquire a lock on the index again.  If another goroutine has Lock, this will
	// deadlock.
	i.mu.RLock()
	mm := i.measurements[string(name)]
	i.mu.RUnlock()

	if mm == nil {
		return nil
	}

	for _, key := range mm.TagKeys() {
		if err := fn([]byte(key)); err != nil {
			return err
		}
	}

	return nil
}

// TagKeyCardinality returns the number of values for a measurement/tag key.
func (i *Index) TagKeyCardinality(name, key []byte) int {
	i.mu.RLock()
	mm := i.measurements[string(name)]
	i.mu.RUnlock()

	if mm == nil {
		return 0
	}
	return mm.CardinalityBytes(key)
}

// TagsForSeries returns the tag map for the passed in series
func (i *Index) TagsForSeries(key string) (models.Tags, error) {
	i.mu.RLock()
	ss := i.series[key]
	i.mu.RUnlock()

	if ss == nil {
		return nil, nil
	}
	return ss.Tags, nil
}

// MeasurementNamesByExpr takes an expression containing only tags and returns a
// list of matching measurement names.
//
// TODO(edd): Remove authorisation from these methods. There shouldn't need to
// be any auth passed down into the index.
func (i *Index) MeasurementNamesByExpr(auth query.Authorizer, expr influxql.Expr) ([][]byte, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	// Return all measurement names if no expression is provided.
	if expr == nil {
		a := make([][]byte, 0, len(i.measurements))
		for _, m := range i.measurements {
			if m.Authorized(auth) {
				a = append(a, m.NameBytes)
			}
		}
		bytesutil.Sort(a)
		return a, nil
	}

	return i.measurementNamesByExpr(auth, expr)
}

func (i *Index) measurementNamesByExpr(auth query.Authorizer, expr influxql.Expr) ([][]byte, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *influxql.BinaryExpr:
		switch e.Op {
		case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
			tag, ok := e.LHS.(*influxql.VarRef)
			if !ok {
				return nil, fmt.Errorf("left side of '%s' must be a tag key", e.Op.String())
			}

			tf := &TagFilter{
				Op:  e.Op,
				Key: tag.Val,
			}

			if influxql.IsRegexOp(e.Op) {
				re, ok := e.RHS.(*influxql.RegexLiteral)
				if !ok {
					return nil, fmt.Errorf("right side of '%s' must be a regular expression", e.Op.String())
				}
				tf.Regex = re.Val
			} else {
				s, ok := e.RHS.(*influxql.StringLiteral)
				if !ok {
					return nil, fmt.Errorf("right side of '%s' must be a tag value string", e.Op.String())
				}
				tf.Value = s.Val
			}

			// Match on name, if specified.
			if tag.Val == "_name" {
				return i.measurementNamesByNameFilter(auth, tf.Op, tf.Value, tf.Regex), nil
			} else if influxql.IsSystemName(tag.Val) {
				return nil, nil
			}

			return i.measurementNamesByTagFilters(auth, tf), nil
		case influxql.OR, influxql.AND:
			lhs, err := i.measurementNamesByExpr(auth, e.LHS)
			if err != nil {
				return nil, err
			}

			rhs, err := i.measurementNamesByExpr(auth, e.RHS)
			if err != nil {
				return nil, err
			}

			if e.Op == influxql.OR {
				return bytesutil.Union(lhs, rhs), nil
			}
			return bytesutil.Intersect(lhs, rhs), nil
		default:
			return nil, fmt.Errorf("invalid tag comparison operator")
		}
	case *influxql.ParenExpr:
		return i.measurementNamesByExpr(auth, e.Expr)
	}
	return nil, fmt.Errorf("%#v", expr)
}

// measurementNamesByNameFilter returns the sorted measurements matching a name.
func (i *Index) measurementNamesByNameFilter(auth query.Authorizer, op influxql.Token, val string, regex *regexp.Regexp) [][]byte {
	var names [][]byte
	for _, m := range i.measurements {
		var matched bool
		switch op {
		case influxql.EQ:
			matched = m.Name == val
		case influxql.NEQ:
			matched = m.Name != val
		case influxql.EQREGEX:
			matched = regex.MatchString(m.Name)
		case influxql.NEQREGEX:
			matched = !regex.MatchString(m.Name)
		}

		if matched && m.Authorized(auth) {
			names = append(names, m.NameBytes)
		}
	}
	bytesutil.Sort(names)
	return names
}

// measurementNamesByTagFilters returns the sorted measurements matching the filters on tag values.
func (i *Index) measurementNamesByTagFilters(auth query.Authorizer, filter *TagFilter) [][]byte {
	// Build a list of measurements matching the filters.
	var names [][]byte
	var tagMatch bool
	var authorized bool

	valEqual := filter.Regex.MatchString
	if filter.Op == influxql.EQ || filter.Op == influxql.NEQ {
		valEqual = func(s string) bool { return filter.Value == s }
	}

	// Iterate through all measurements in the database.
	for _, m := range i.measurements {
		tagVals := m.SeriesByTagKeyValue(filter.Key)
		if tagVals == nil {
			continue
		}

		tagMatch = false
		// Authorization must be explicitly granted when an authorizer is present.
		authorized = query.AuthorizerIsOpen(auth)

		// Check the tag values belonging to the tag key for equivalence to the
		// tag value being filtered on.
		tagVals.Range(func(tv string, seriesIDs seriesIDs) bool {
			if !valEqual(tv) {
				return true // No match. Keep checking.
			}

			tagMatch = true
			if query.AuthorizerIsOpen(auth) {
				return false // No need to continue checking series, there is a match.
			}

			// Is there a series with this matching tag value that is
			// authorized to be read?
			for _, sid := range seriesIDs {
				s := m.SeriesByID(sid)

				// If the series is deleted then it can't be used to authorise against.
				if s != nil && s.Deleted() {
					continue
				}

				if s != nil && auth.AuthorizeSeriesRead(i.database, m.NameBytes, s.Tags) {
					// The Range call can return early as a matching
					// tag value with an authorized series has been found.
					authorized = true
					return false
				}
			}

			// The matching tag value doesn't have any authorized series.
			// Check for other matching tag values if this is a regex check.
			return filter.Op == influxql.EQREGEX
		})

		// For negation operators, to determine if the measurement is authorized,
		// an authorized series belonging to the measurement must be located.
		// Then, the measurement can be added iff !tagMatch && authorized.
		if auth != nil && !tagMatch && (filter.Op == influxql.NEQREGEX || filter.Op == influxql.NEQ) {
			authorized = m.Authorized(auth)
		}

		// tags match | operation is EQ | measurement matches
		// --------------------------------------------------
		//     True   |       True      |      True
		//     True   |       False     |      False
		//     False  |       True      |      False
		//     False  |       False     |      True
		if tagMatch == (filter.Op == influxql.EQ || filter.Op == influxql.EQREGEX) && authorized {
			names = append(names, m.NameBytes)
		}
	}

	bytesutil.Sort(names)
	return names
}

// MeasurementNamesByRegex returns the measurements that match the regex.
func (i *Index) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var matches [][]byte
	for _, m := range i.measurements {
		if re.MatchString(m.Name) {
			matches = append(matches, m.NameBytes)
		}
	}
	return matches, nil
}

// DropMeasurement removes the measurement and all of its underlying
// series from the database index
func (i *Index) DropMeasurement(name []byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.dropMeasurement(string(name))
}

func (i *Index) dropMeasurement(name string) error {
	// Update the tombstone sketch.
	i.measurementsTSSketch.Add([]byte(name))

	m := i.measurements[name]
	if m == nil {
		return nil
	}

	delete(i.measurements, name)
	for _, s := range m.SeriesByIDMap() {
		delete(i.series, s.Key)
		i.seriesTSSketch.Add([]byte(s.Key))
	}
	return nil
}

// DropMeasurementIfSeriesNotExist drops a measurement only if there are no more
// series for the measurment.
func (i *Index) DropMeasurementIfSeriesNotExist(name []byte) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	m := i.measurements[string(name)]
	if m == nil {
		return false, nil
	}

	if m.HasSeries() {
		return false, nil
	}

	return true, i.dropMeasurement(string(name))
}

// DropSeriesGlobal removes the series key and its tags from the index.
func (i *Index) DropSeriesGlobal(key []byte) error {
	if key == nil {
		return nil
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	k := string(key)
	series := i.series[k]
	if series == nil {
		return nil
	}

	// Update the tombstone sketch.
	i.seriesTSSketch.Add([]byte(k))

	// Remove from the index.
	delete(i.series, k)

	// Remove the measurement's reference.
	series.Measurement.DropSeries(series)
	// Mark the series as deleted.
	series.Delete()

	// If the measurement no longer has any series, remove it as well.
	if !series.Measurement.HasSeries() {
		i.dropMeasurement(series.Measurement.Name)
	}

	return nil
}

// TagSets returns a list of tag sets.
func (i *Index) TagSets(shardSeriesIDs *tsdb.SeriesIDSet, name []byte, opt query.IteratorOptions) ([]*query.TagSet, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	isExist := i.EdgeExistsAtRange(opt.SrcHash, opt.DstHash, opt.Fp, opt.StartTime, opt.EndTime)
	if isExist == false {
		return nil, nil
	}

	mm := i.measurements[string(name)]
	if mm == nil {
		return nil, nil
	}

	tagSets, err := mm.TagSets(shardSeriesIDs, opt)
	if err != nil {
		return nil, err
	}

	return tagSets, nil
}

func (i *Index) SeriesKeys() []string {
	i.mu.RLock()
	s := make([]string, 0, len(i.series))
	for k := range i.series {
		s = append(s, k)
	}
	i.mu.RUnlock()
	return s

}

// SetFieldSet sets a shared field set from the engine.
func (i *Index) SetFieldSet(fieldset *tsdb.MeasurementFieldSet) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.fieldset = fieldset
}

// FieldSet returns the assigned fieldset.
func (i *Index) FieldSet() *tsdb.MeasurementFieldSet {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.fieldset
}

// SetFieldName adds a field name to a measurement.
func (i *Index) SetFieldName(measurement []byte, name string) {
	m := i.CreateMeasurementIndexIfNotExists(measurement)
	m.SetFieldName(name)
}

// ForEachMeasurementName iterates over each measurement name.
func (i *Index) ForEachMeasurementName(fn func(name []byte) error) error {
	i.mu.RLock()
	mms := make(measurements, 0, len(i.measurements))
	for _, m := range i.measurements {
		mms = append(mms, m)
	}
	sort.Sort(mms)
	i.mu.RUnlock()

	for _, m := range mms {
		if err := fn(m.NameBytes); err != nil {
			return err
		}
	}
	return nil
}

func (i *Index) MeasurementSeriesIDIterator(name []byte) (tsdb.SeriesIDIterator, error) {
	return i.MeasurementSeriesKeysByExprIterator(name, nil)
}

func (i *Index) TagKeySeriesIDIterator(name, key []byte) (tsdb.SeriesIDIterator, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	m := i.measurements[string(name)]
	if m == nil {
		return nil, nil
	}
	return tsdb.NewSeriesIDSliceIterator([]uint64(m.SeriesIDsByTagKey(key))), nil
}

func (i *Index) TagValueSeriesIDIterator(name, key, value []byte) (tsdb.SeriesIDIterator, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	m := i.measurements[string(name)]
	if m == nil {
		return nil, nil
	}
	return tsdb.NewSeriesIDSliceIterator([]uint64(m.SeriesIDsByTagValue(key, value))), nil
}

func (i *Index) TagKeyIterator(name []byte) (tsdb.TagKeyIterator, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	m := i.measurements[string(name)]
	if m == nil {
		return nil, nil
	}
	keys := m.TagKeys()
	sort.Strings(keys)

	a := make([][]byte, len(keys))
	for i := range a {
		a[i] = []byte(keys[i])
	}
	return tsdb.NewTagKeySliceIterator(a), nil
}

// TagValueIterator provides an iterator over all the tag values belonging to
// series with the provided measurement name and tag key.
//
// TagValueIterator does not currently support authorization.
func (i *Index) TagValueIterator(name, key []byte) (tsdb.TagValueIterator, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	m := i.measurements[string(name)]
	if m == nil {
		return nil, nil
	}
	values := m.TagValues(nil, string(key))
	sort.Strings(values)

	a := make([][]byte, len(values))
	for i := range a {
		a[i] = []byte(values[i])
	}
	return tsdb.NewTagValueSliceIterator(a), nil
}

func (i *Index) MeasurementSeriesKeysByExprIterator(name []byte, condition influxql.Expr) (tsdb.SeriesIDIterator, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	m := i.measurements[string(name)]
	if m == nil {
		return nil, nil
	}

	// Return all series if no condition specified.
	if condition == nil {
		return tsdb.NewSeriesIDSliceIterator([]uint64(m.SeriesIDs())), nil
	}

	// Get series IDs that match the WHERE clause.
	ids, filters, err := m.WalkWhereForSeriesIds(condition)
	if err != nil {
		return nil, err
	}

	// Delete boolean literal true filter expressions.
	// These are returned for `WHERE tagKey = 'tagVal'` type expressions and are okay.
	filters.DeleteBoolLiteralTrues()

	// Check for unsupported field filters.
	// Any remaining filters means there were fields (e.g., `WHERE value = 1.2`).
	if filters.Len() > 0 {
		return nil, errors.New("fields not supported in WHERE clause during deletion")
	}

	return tsdb.NewSeriesIDSliceIterator([]uint64(ids)), nil
}

func (i *Index) MeasurementSeriesKeysByExpr(name []byte, condition influxql.Expr) ([][]byte, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	m := i.measurements[string(name)]
	if m == nil {
		return nil, nil
	}

	// Return all series if no condition specified.
	if condition == nil {
		return m.SeriesKeys(), nil
	}

	// Get series IDs that match the WHERE clause.
	ids, filters, err := m.WalkWhereForSeriesIds(condition)
	if err != nil {
		return nil, err
	}

	// Delete boolean literal true filter expressions.
	// These are returned for `WHERE tagKey = 'tagVal'` type expressions and are okay.
	filters.DeleteBoolLiteralTrues()

	// Check for unsupported field filters.
	// Any remaining filters means there were fields (e.g., `WHERE value = 1.2`).
	if filters.Len() > 0 {
		return nil, errors.New("fields not supported in WHERE clause during deletion")
	}

	return m.SeriesKeysByID(ids), nil
}

// SeriesIDIterator returns an influxql iterator over matching series ids.
func (i *Index) SeriesIDIterator(opt query.IteratorOptions) (tsdb.SeriesIDIterator, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	// Read and sort all measurements.
	mms := make(measurements, 0, len(i.measurements))
	for _, mm := range i.measurements {
		mms = append(mms, mm)
	}
	sort.Sort(mms)

	return &seriesIDIterator{
		database: i.database,
		mms:      mms,
		opt:      opt,
	}, nil
}

// DiskSizeBytes always returns zero bytes, since this is an in-memory index.
func (i *Index) DiskSizeBytes() int64 { return 0 }

// Rebuild recreates the measurement indexes to allow deleted series to be removed
// and garbage collected.
func (i *Index) Rebuild() {
	// Only allow one rebuild at a time.  This will cause all subsequent rebuilds
	// to queue.  The measurement rebuild is idempotent and will not be rebuilt if
	// it does not need to be.
	i.rebuildQueue.Lock()
	defer i.rebuildQueue.Unlock()

	i.ForEachMeasurementName(func(name []byte) error {
		// Measurement never returns an error
		m, _ := i.Measurement(name)
		if m == nil {
			return nil
		}

		i.mu.Lock()
		nm := m.Rebuild()

		i.measurements[string(name)] = nm
		i.mu.Unlock()
		return nil
	})
}
func (i *Index) assignExistingSeries(shardID uint64, seriesIDSet *tsdb.SeriesIDSet, measurements map[string]int,
	keys, names [][]byte, tagsSlice []models.Tags) ([][]byte, [][]byte, []models.Tags) {

	i.mu.RLock()
	var n int
	for j, key := range keys {
		if ss := i.series[string(key)]; ss == nil {
			keys[n] = keys[j]
			names[n] = names[j]
			tagsSlice[n] = tagsSlice[j]
			n++
		} else {
			// Add the existing series to this shard's bitset, since this may
			// be the first time the series is added to this shard.
			if !seriesIDSet.Contains(ss.ID) {
				seriesIDSet.Lock()
				if !seriesIDSet.ContainsNoLock(ss.ID) {
					seriesIDSet.AddNoLock(ss.ID)
					measurements[string(names[j])]++
				}
				seriesIDSet.Unlock()
			}
		}
	}

	i.mu.RUnlock()
	return keys[:n], names[:n], tagsSlice[:n]
}

// assignExistingSeries assigns the existing series to shardID and returns the series, names and tags that
// do not exists yet.
func (i *Index) assignExistingSeries2(shardID uint64, seriesIDSet *tsdb.SeriesIDSet, measurements map[string]int,
	keys, names [][]byte, tagsSlice []models.Tags, ts []int64) ([][]byte, [][]byte, []models.Tags) {

	i.mu.RLock()
	var n int
	for j, key := range keys {
		if ss := i.series[string(key)]; ss == nil {
			keys[n] = keys[j]
			names[n] = names[j]
			tagsSlice[n] = tagsSlice[j]
			n++
		} else {
			// Add the existing series to this shard's bitset, since this may
			// be the first time the series is added to this shard.
			if !seriesIDSet.Contains(ss.ID) {
				seriesIDSet.Lock()
				if !seriesIDSet.ContainsNoLock(ss.ID) {
					seriesIDSet.AddNoLock(ss.ID)
					measurements[string(names[j])]++
				}
				seriesIDSet.Unlock()
			}
		}
	}

	toSec := func(ts int64) int64 {
		switch {
		case ts > 1e18:
			return ts / 1e9
		case ts > 1e12:
			return ts / 1e6
		default:
			return ts
		}
	}

	type edgeEvent struct {
		src, dst, bucket int
		ts               int64
	}
	events := make([]edgeEvent, 0, len(tagsSlice))

	for j, tags := range tagsSlice {
		var srcHash, dstHash, bucket int
		for _, tag := range tags {
			if string(tag.Key) == "source" {
				srcHash, _ = strconv.Atoi(string(tag.Value))
			} else if string(tag.Key) == "destination" {
				dstHash, _ = strconv.Atoi(string(tag.Value))
			} else if string(tag.Key) == "fp" {
				bucket, _ = strconv.Atoi(string(tag.Value))
			}
		}
		events = append(events, edgeEvent{
			src:    srcHash,
			dst:    dstHash,
			bucket: bucket,
			ts:     toSec(ts[j]),
		})

	}

	i.mu.RUnlock()

	for _, e := range events {
		i.AddEdgeEvent(e.src, e.dst, e.bucket, e.ts)
	}
	return keys[:n], names[:n], tagsSlice[:n]
}

// Ensure index implements interface.
var _ tsdb.Index = &ShardIndex{}

// ShardIndex represents a shim between the TSDB index interface and the shared
// in-memory index. This is required because per-shard in-memory indexes will
// grow the heap size too large.
type ShardIndex struct {
	id uint64 // shard id

	*Index // Shared reference to global database-wide index.

	// Bitset storing all undeleted series IDs associated with this shard.
	seriesIDSet *tsdb.SeriesIDSet

	// mapping of measurements to the count of series ids in the set. protected
	// by the seriesIDSet lock.
	measurements map[string]int

	opt tsdb.EngineOptions
}

// DropSeries removes the provided series id from the local bitset that tracks
// series in this shard only.
func (idx *ShardIndex) DropSeries(seriesID uint64, key []byte, _ bool) error {
	// Remove from shard-local bitset if it exists.
	idx.seriesIDSet.Lock()
	if idx.seriesIDSet.ContainsNoLock(seriesID) {
		idx.seriesIDSet.RemoveNoLock(seriesID)

		name := models.ParseName(key)
		if curr := idx.measurements[string(name)]; curr <= 1 {
			delete(idx.measurements, string(name))
		} else {
			idx.measurements[string(name)] = curr - 1
		}
	}
	idx.seriesIDSet.Unlock()
	return nil
}

// DropSeriesList removes the provided series ids from the local bitset that tracks
// series in this shard only.
func (idx *ShardIndex) DropSeriesList(seriesIDs []uint64, keys [][]byte, _ bool) error {
	// All slices must be of equal length.
	if len(seriesIDs) != len(keys) {
		return errors.New("seriesIDs/keys length mismatch in index")
	}
	idx.seriesIDSet.Lock()
	for i, seriesID := range seriesIDs {
		if idx.seriesIDSet.ContainsNoLock(seriesID) {
			idx.seriesIDSet.RemoveNoLock(seriesID)

			name := models.ParseName(keys[i])
			if curr := idx.measurements[string(name)]; curr <= 1 {
				delete(idx.measurements, string(name))
			} else {
				idx.measurements[string(name)] = curr - 1
			}
		}
	}
	idx.seriesIDSet.Unlock()
	return nil
}

// DropMeasurementIfSeriesNotExist drops a measurement only if there are no more
// series for the measurment.
func (idx *ShardIndex) DropMeasurementIfSeriesNotExist(name []byte) (bool, error) {
	idx.seriesIDSet.Lock()
	curr := idx.measurements[string(name)]
	idx.seriesIDSet.Unlock()
	if curr > 0 {
		return false, nil
	}

	// we always report the measurement was dropped if it does not exist in our
	// measurements mapping.
	_, err := idx.Index.DropMeasurementIfSeriesNotExist(name)
	return err == nil, err
}

func (idx *ShardIndex) CreateSeriesListIfNotExists(keys, names [][]byte, tagsSlice []models.Tags) error {
	keys, names, tagsSlice = idx.assignExistingSeries(idx.id, idx.seriesIDSet, idx.measurements, keys, names, tagsSlice)
	if len(keys) == 0 {
		return nil
	}

	var (
		reason      string
		droppedKeys [][]byte
	)

	// Ensure that no tags go over the maximum cardinality.
	if maxValuesPerTag := idx.opt.Config.MaxValuesPerTag; maxValuesPerTag > 0 {
		var n int

	outer:
		for i, name := range names {
			tags := tagsSlice[i]
			for _, tag := range tags {
				// Skip if the tag value already exists.
				if ok, _ := idx.HasTagValue(name, tag.Key, tag.Value); ok {
					continue
				}

				// Read cardinality. Skip if we're below the threshold.
				n := idx.TagValueN(name, tag.Key)
				if n < maxValuesPerTag {
					continue
				}

				if reason == "" {
					reason = fmt.Sprintf("max-values-per-tag limit exceeded (%d/%d): measurement=%q tag=%q value=%q",
						n, maxValuesPerTag, name, string(tag.Key), string(tag.Value))
				}

				droppedKeys = append(droppedKeys, keys[i])
				continue outer
			}

			// Increment success count if all checks complete.
			if n != i {
				keys[n], names[n], tagsSlice[n] = keys[i], names[i], tagsSlice[i]
			}
			n++
		}

		// Slice to only include successful points.
		keys, names, tagsSlice = keys[:n], names[:n], tagsSlice[:n]
	}

	if err := idx.Index.CreateSeriesListIfNotExists(idx.seriesIDSet, idx.measurements, keys, names, tagsSlice, &idx.opt, idx.opt.Config.MaxSeriesPerDatabase == 0); err != nil {
		reason = err.Error()
		droppedKeys = append(droppedKeys, keys...)
	}

	// Report partial writes back to shard.
	if len(droppedKeys) > 0 {
		dropped := len(droppedKeys) // number dropped before deduping
		bytesutil.SortDedup(droppedKeys)
		return tsdb.PartialWriteError{
			Reason:      reason,
			Dropped:     dropped,
			DroppedKeys: droppedKeys,
		}
	}

	return nil
}

// CreateSeriesListIfNotExists creates a list of series if they doesn't exist in bulk.
func (idx *ShardIndex) CreateSeriesListIfNotExists2(keys, names [][]byte, tagsSlice []models.Tags, ts []int64) error {
	keys, names, tagsSlice = idx.assignExistingSeries2(idx.id, idx.seriesIDSet, idx.measurements, keys, names, tagsSlice, ts)
	if len(keys) == 0 {
		return nil
	}

	var (
		reason      string
		droppedKeys [][]byte
	)

	// Ensure that no tags go over the maximum cardinality.
	if maxValuesPerTag := idx.opt.Config.MaxValuesPerTag; maxValuesPerTag > 0 {
		var n int

	outer:
		for i, name := range names {
			tags := tagsSlice[i]
			for _, tag := range tags {
				// Skip if the tag value already exists.
				if ok, _ := idx.HasTagValue(name, tag.Key, tag.Value); ok {
					continue
				}

				// Read cardinality. Skip if we're below the threshold.
				n := idx.TagValueN(name, tag.Key)
				if n < maxValuesPerTag {
					continue
				}

				if reason == "" {
					reason = fmt.Sprintf("max-values-per-tag limit exceeded (%d/%d): measurement=%q tag=%q value=%q",
						n, maxValuesPerTag, name, string(tag.Key), string(tag.Value))
				}

				droppedKeys = append(droppedKeys, keys[i])
				continue outer
			}

			// Increment success count if all checks complete.
			if n != i {
				keys[n], names[n], tagsSlice[n] = keys[i], names[i], tagsSlice[i]
			}
			n++
		}

		// Slice to only include successful points.
		keys, names, tagsSlice = keys[:n], names[:n], tagsSlice[:n]
	}

	if err := idx.Index.CreateSeriesListIfNotExists(idx.seriesIDSet, idx.measurements, keys, names, tagsSlice, &idx.opt, idx.opt.Config.MaxSeriesPerDatabase == 0); err != nil {
		reason = err.Error()
		droppedKeys = append(droppedKeys, keys...)
	}

	// Report partial writes back to shard.
	if len(droppedKeys) > 0 {
		dropped := len(droppedKeys) // number dropped before deduping
		bytesutil.SortDedup(droppedKeys)
		return tsdb.PartialWriteError{
			Reason:      reason,
			Dropped:     dropped,
			DroppedKeys: droppedKeys,
		}
	}

	return nil
}

// SeriesN returns the number of unique non-tombstoned series local to this shard.
func (idx *ShardIndex) SeriesN() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return int64(idx.seriesIDSet.Cardinality())
}

// InitializeSeries is called during start-up.
// This works the same as CreateSeriesListIfNotExists except it ignore limit errors.
func (idx *ShardIndex) InitializeSeries(keys, names [][]byte, tags []models.Tags) error {
	return idx.Index.CreateSeriesListIfNotExists(idx.seriesIDSet, idx.measurements, keys, names, tags, &idx.opt, true)
}

// CreateSeriesIfNotExists creates the provided series on the index if it is not
// already present.
func (idx *ShardIndex) CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error {
	return idx.Index.CreateSeriesListIfNotExists(idx.seriesIDSet, idx.measurements, [][]byte{key}, [][]byte{name}, []models.Tags{tags}, &idx.opt, false)
}

// TagSets returns a list of tag sets based on series filtering.
func (idx *ShardIndex) TagSets(name []byte, opt query.IteratorOptions) ([]*query.TagSet, error) {
	return idx.Index.TagSets(idx.seriesIDSet, name, opt)
}

// SeriesIDSet returns the bitset associated with the series ids.
func (idx *ShardIndex) SeriesIDSet() *tsdb.SeriesIDSet {
	return idx.seriesIDSet
}

// NewShardIndex returns a new index for a shard.
func NewShardIndex(id uint64, seriesIDSet *tsdb.SeriesIDSet, opt tsdb.EngineOptions) tsdb.Index {
	return &ShardIndex{
		Index:        opt.InmemIndex.(*Index),
		id:           id,
		seriesIDSet:  seriesIDSet,
		measurements: make(map[string]int),
		opt:          opt,
	}
}

// seriesIDIterator emits series ids.
type seriesIDIterator struct {
	database string
	mms      measurements
	keys     struct {
		buf []*series
		i   int
	}
	opt query.IteratorOptions
}

// Stats returns stats about the points processed.
func (itr *seriesIDIterator) Stats() query.IteratorStats { return query.IteratorStats{} }

// Close closes the iterator.
func (itr *seriesIDIterator) Close() error { return nil }

// Next emits the next point in the iterator.
func (itr *seriesIDIterator) Next() (tsdb.SeriesIDElem, error) {
	for {
		// Load next measurement's keys if there are no more remaining.
		if itr.keys.i >= len(itr.keys.buf) {
			if err := itr.nextKeys(); err != nil {
				return tsdb.SeriesIDElem{}, err
			}
			if len(itr.keys.buf) == 0 {
				return tsdb.SeriesIDElem{}, nil
			}
		}

		// Read the next key.
		series := itr.keys.buf[itr.keys.i]
		itr.keys.i++

		if !itr.opt.Authorizer.AuthorizeSeriesRead(itr.database, series.Measurement.NameBytes, series.Tags) {
			continue
		}

		return tsdb.SeriesIDElem{SeriesID: series.ID}, nil
	}
}

// nextKeys reads all keys for the next measurement.
func (itr *seriesIDIterator) nextKeys() error {
	for {
		// Ensure previous keys are cleared out.
		itr.keys.i, itr.keys.buf = 0, itr.keys.buf[:0]

		// Read next measurement.
		if len(itr.mms) == 0 {
			return nil
		}
		mm := itr.mms[0]
		itr.mms = itr.mms[1:]

		// Read all series keys.
		ids, err := mm.SeriesIDsAllOrByExpr(itr.opt.Condition)
		if err != nil {
			return err
		} else if len(ids) == 0 {
			continue
		}
		itr.keys.buf = mm.SeriesByIDSlice(ids)

		// Sort series by key
		sort.Slice(itr.keys.buf, func(i, j int) bool {
			return itr.keys.buf[i].Key < itr.keys.buf[j].Key
		})

		return nil
	}
}

// errMaxSeriesPerDatabaseExceeded is a marker error returned during series creation
// to indicate that a new series would exceed the limits of the database.
type errMaxSeriesPerDatabaseExceeded struct {
	limit int
}

func (e errMaxSeriesPerDatabaseExceeded) Error() string {
	return fmt.Sprintf("max-series-per-database limit exceeded: (%d)", e.limit)
}
