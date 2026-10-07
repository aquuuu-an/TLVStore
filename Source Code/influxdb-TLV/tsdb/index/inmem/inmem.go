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
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/influxdata/influxdb/nxl"
	"github.com/influxdata/influxdb/services/meta"

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
const secondWindow = 3600 // 秒级窗口固定为最近1小时（3600秒）

func init() {
	tsdb.NewInmemIndex = func(name string, sfile *tsdb.SeriesFile) (interface{}, error) { return NewIndex(name, sfile), nil }

	tsdb.RegisterIndex(IndexName, func(id uint64, database, path string, seriesIDSet *tsdb.SeriesIDSet, sfile *tsdb.SeriesFile, opt tsdb.EngineOptions) tsdb.Index {
		return NewShardIndex(id, seriesIDSet, opt)
	})

	tsdb.RegisterIndexForGroup(IndexName, func(id uint64, database, path string, seriesIDSet, groupIDSet, sequenceIDSet *tsdb.SeriesIDSet,
		sfile *tsdb.SeriesFile, opt tsdb.EngineOptions) tsdb.Index {
		return NewShardIndexForGroup(id, seriesIDSet, groupIDSet, sequenceIDSet, opt)
	})
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

func (eti *EdgeTimeIndex) EstimatedSize() uint64 {
	if eti == nil {
		return 0
	}

	var size uint64

	size += uint64(unsafe.Sizeof(*eti))

	size += uint64(unsafe.Sizeof(eti.secondBitmap))

	size += uint64(len(eti.secondBitmap)) * uint64(unsafe.Sizeof(uint64(0)))

	if eti.minuteIndex != nil {
		size += estimateRoaringSize(eti.minuteIndex)
	}
	if eti.hourIndex != nil {
		size += estimateRoaringSize(eti.hourIndex)
	}

	return size
}

func estimateRoaringSize(bm *roaring.Bitmap) uint64 {
	if bm == nil {
		return 0
	}

	// GetSerializedSizeInBytes 返回 bitmap 数据的真实大小
	size := bm.GetSerializedSizeInBytes()

	// 再加上 roaring.Bitmap 结构体本身
	size += uint64(unsafe.Sizeof(*bm))

	return size
}

// Index is the in memory index of a collection of measurements, time
// series, and their tags. Exported functions are goroutine safe while
// un-exported functions assume the caller will use the appropriate locks.
type Index struct {
	Mu sync.RWMutex

	database string
	sfile    *tsdb.SeriesFile          // 索引文件
	fieldset *tsdb.MeasurementFieldSet // 数据库所属的测量值的集合

	// In-memory metadata index, built on load and updated when new series come in
	// key是测量名称 比如cpu
	/**
	 value是// in-memory index fields
		seriesByID          map[uint64]*series      // lookup table for series by their id
		seriesByTagKeyValue map[string]*tagKeyValue // map from tag key to value to sorted set of series ids
	*/
	measurements map[string]*measurement // measurement name to object and index

	// 组元数据
	groupMetaData *meta.GroupMetaData // 传进组元数据

	// 把所有的series单独拎出来
	series map[string]*series // map series key to the Series object

	// ADD: 组map和序列map
	groups    map[string]*tagGroup    // 一个组只有一个组key
	sequences map[string]*tagSequence // 一个序列是可以属于多个组的

	AdjacencyMatrix map[int]map[int]map[int]*EdgeTimeIndex

	indexInitStartTimestamp time.Time
	indexInitEndTimestamp   time.Time

	// ADD
	groupSketch, groupTSSketch       estimator.Sketch
	sequenceSketch, sequenceTSSketch estimator.Sketch

	seriesSketch, seriesTSSketch             estimator.Sketch
	measurementsSketch, measurementsTSSketch estimator.Sketch

	// Mutex to control rebuilds of the index
	rebuildQueue sync.Mutex
}

func (i *Index) SetGroupMetaData(groupMetaData *meta.GroupMetaData) {
	i.groupMetaData = groupMetaData
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
	i.Mu.Lock()
	defer i.Mu.Unlock()

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
	i.Mu.RLock()
	defer i.Mu.RUnlock()

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
	i.Mu.RLock()
	defer i.Mu.RUnlock()

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
	i.Mu.RLock()
	// 1. 获取该源节点对应的所有目标节点层
	l1, ok := i.AdjacencyMatrix[srcHash]
	if !ok {
		i.Mu.RUnlock()
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
	i.Mu.RUnlock()

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
	if dstHash == -1 && bucket == -1 {
		return true
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

//func (i *Index) EdgeExistsAtRange(srcHash, dstHash, bucket int, start, end int64) bool {
//	i.mu.RLock()
//	defer i.mu.RUnlock()
//
//	if srcHash == -1 && dstHash == -1 {
//		return false
//	}
//
//	// 自动单位转换为秒
//	toSec := func(ts int64) int64 {
//		switch {
//		case ts > 1e18:
//			return ts / 1e9
//		case ts > 1e12:
//			return ts / 1e6
//		default:
//			return ts
//		}
//	}
//	if start < 0 {
//		start = 0
//	}
//	startSec := toSec(start)
//	endSec := toSec(end)
//
//	dstMap, ok := i.AdjacencyMatrix[srcHash]
//	if !ok {
//		// src 不存在
//		return false
//	}
//
//	fpMap, ok := dstMap[dstHash]
//	if !ok {
//		// dst 不存在
//		return false
//	}
//
//	e, ok := fpMap[bucket]
//	if !ok {
//		// fp 不存在
//		return false
//	}
//
//	histStart := startSec
//	histEnd := endSec
//	//if histEnd >= windowStart {
//	//	histEnd = windowStart - 1
//	//}
//	if histStart <= histEnd {
//		startHour := uint32(histStart / 3600)
//		endHour := uint32(histEnd / 3600)
//
//		startMinute := uint32(histStart / 60)
//		endMinute := uint32(histEnd / 60)
//
//		// 对每个小时：如果该小时存在，则只检查该小时内的分钟区间
//		for h := startHour; h <= endHour; h++ {
//			if !e.hourIndex.Contains(h) {
//				continue
//			}
//
//			// 计算当前小时对应的分钟范围（以 epoch minute 为单位）
//			hourMinuteStart := h * 60
//			hourMinuteEnd := (h+1)*60 - 1
//
//			// 取交集：mStart = max(startMinute, hourMinuteStart)
//			mStart := startMinute
//			if mStart < hourMinuteStart {
//				mStart = hourMinuteStart
//			}
//
//			// mEnd = min(endMinute, hourMinuteEnd)
//			mEnd := endMinute
//			if mEnd > hourMinuteEnd {
//				mEnd = hourMinuteEnd
//			}
//
//			// 在交集分钟范围内查找任意存在的分钟
//			for m := mStart; m <= mEnd; m++ {
//				if e.minuteIndex.Contains(m) {
//					return true
//				}
//			}
//		}
//	}
//	return false
//}

//	Inmem内存索引会在启动的时候全部构建完成，可以统计
//
// （1）内存倒排索引所占的内存空间(2)倒排列表长度和所占内存空间（3）倒排索引构建时间
func (i *Index) IndexAnalyseForOld() {
	i.Mu.RLock()
	defer i.Mu.RUnlock()

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
		fmt.Sprintf("倒排索引大小（lwq统计）：%d\n", invertIndexSize) +
		fmt.Sprintf("倒排索引大小（自带）：%d\n", invertIndexSize2) +
		fmt.Sprintf("倒排索引基数：%d\n", invertIndexCardinality) +
		fmt.Sprintf("倒排列表总长度：%d\n", invertIndexLength)

	writeToFile(logFile, logContent)
}

func (i *Index) IndexAnalyseForGroup() {
	i.Mu.RLock()
	defer i.Mu.RUnlock()

	// 计算构建时间
	buildTime := i.indexInitEndTimestamp.Nanosecond() - i.indexInitStartTimestamp.Nanosecond()

	// 计算series所占的内存空间大小
	groupKeyCount := len(i.groups)
	sequenceKeyCount := len(i.sequences)
	groupKeySize := uint64(0)
	sequenceKeySize := uint64(0)
	for key, value := range i.groups {
		groupKeySize += uint64(len(key))
		groupKeySize += uint64(value.bytes())
	}

	for key, value := range i.sequences {
		sequenceKeySize += uint64(len(key))
		sequenceKeySize += uint64(value.bytes())
	}

	// 计算内存倒排索引的大小
	gInvertIndexSize := uint64(0)
	//gInvertIndexSize2 := uint64(0)       // 自己带的统计大小的
	gInvertIndexCardinality := uint64(0) // 倒排列表基数
	gInvertIndexLength := uint64(0)      // 倒排列表长度

	sInvertIndexSize := uint64(0)
	//sInvertIndexSize2 := uint64(0)       // 自己带的统计大小的
	sInvertIndexCardinality := uint64(0) // 倒排列表基数
	sInvertIndexLength := uint64(0)      // 倒排列表长度

	amSize := uint64(0)
	amSize += uint64(unsafe.Sizeof(i.AdjacencyMatrix))

	for u, m1 := range i.AdjacencyMatrix {
		amSize += uint64(unsafe.Sizeof(u))
		amSize += uint64(unsafe.Sizeof(m1))

		for v, m2 := range m1 {
			amSize += uint64(unsafe.Sizeof(v))
			amSize += uint64(unsafe.Sizeof(m2))

			for t, eti := range m2 {
				amSize += uint64(unsafe.Sizeof(t))
				amSize += uint64(unsafe.Sizeof(eti))
				amSize += eti.EstimatedSize() // 自己实现
			}
		}
	}

	for _, invert := range i.measurements {
		groupByID := invert.groupByID
		for _, value := range groupByID {
			gInvertIndexSize += 8
			gInvertIndexSize += uint64(unsafe.Sizeof(value))
		}

		groupByTagKeyValue := invert.groupByTagKeyValue

		for tagKey, tagValue := range groupByTagKeyValue {
			gInvertIndexSize += uint64(len(tagKey))
			gInvertIndexSize += uint64(uint(tagValue.bytes()))
			gInvertIndexCardinality += uint64(tagValue.Cardinality())
			gInvertIndexLength += uint64(tagValue.invertListLength())
		}
		//gInvertIndexSize2 += uint64(invert.bytes()) // 自己带的统计大小的
	}

	groupMapSize := 0

	for _, invert := range i.measurements {
		sequenceByID := invert.sequenceByID
		for _, value := range sequenceByID {
			sInvertIndexSize += 8
			sInvertIndexSize += uint64(unsafe.Sizeof(value))
		}

		sequenceByTagKeyValue := invert.sequenceByTagKeyValue

		for tagKey, tagValue := range sequenceByTagKeyValue {
			sInvertIndexSize += uint64(len(tagKey))
			sInvertIndexSize += uint64(uint(tagValue.bytes()))
			sInvertIndexCardinality += uint64(tagValue.Cardinality())
			sInvertIndexLength += uint64(tagValue.invertListLength())
		}

		for _, seqMap := range invert.groupMap {
			groupMapSize += 8                  // gID
			groupMapSize += 16 * (len(seqMap)) // sID+gID
		}
		//gInvertIndexSize2 += uint64(invert.bytes()) // 自己带的统计大小的
	}

	logFile := nxl.IndexFilePath

	logContent := fmt.Sprintf("[ Database Name : %s ]\n", i.database) +
		fmt.Sprintf("(组)个数：%d\n", groupKeyCount) +
		fmt.Sprintf("(组)内存占用：%d\n", groupKeySize) +
		fmt.Sprintf("(组)倒排索引大小（lwq统计）：%d\n", gInvertIndexSize) +
		fmt.Sprintf("(组)倒排索引基数：%d\n", gInvertIndexCardinality) +
		fmt.Sprintf("(组)倒排列表总长度：%d\n", gInvertIndexLength) +
		fmt.Sprintf("(序列)个数：%d\n", sequenceKeyCount) +
		fmt.Sprintf("(序列)内存占用：%d\n", sequenceKeySize) +
		fmt.Sprintf("(序列)倒排索引大小（lwq统计）：%d\n", sInvertIndexSize) +
		fmt.Sprintf("(序列)倒排索引基数：%d\n", sInvertIndexCardinality) +
		fmt.Sprintf("(序列)倒排列表总长度：%d\n", sInvertIndexLength) +
		fmt.Sprintf("邻接矩阵大小（lwq统计）：%d\n", amSize) +
		fmt.Sprintf("连接map大小：%d\n", groupMapSize) +
		fmt.Sprintf("Index构建时间：%d\n", buildTime)

	writeToFile(logFile, logContent)
}

func calculateStructSize(data series) (totalSize int64) {
	// 计算结构体本身的大小
	structSize := int64(unsafe.Sizeof(data))
	totalSize += structSize

	// 获取结构体类型
	structType := reflect.TypeOf(data)

	// 遍历结构体的字段
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		// 如果字段是切片类型
		if field.Type.Kind() == reflect.Slice {
			// 计算切片头部的大小
			sliceHeaderSize := int64(unsafe.Sizeof(data.Tags))

			// 获取底层数组的大小
			sliceElemType := field.Type.Elem()
			arraySize := int64(reflect.New(sliceElemType).Type().Size())

			// 计算实际数据的大小
			dataSize := int64(cap(data.Tags)) * arraySize

			// 计算整个切片的大小
			sliceSize := sliceHeaderSize + dataSize

			// 累加到总大小
			totalSize += sliceSize
		}
	}

	return totalSize
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

func (i *Index) SetIndexInitStartTimestamp(t time.Time) {
	i.indexInitStartTimestamp = t
}

func (i *Index) SetIndexInitEndTimestamp(t time.Time) {
	i.indexInitEndTimestamp = t
}

// NewIndex returns a new initialized Index.
func NewIndex(database string, sfile *tsdb.SeriesFile) *Index {
	index := &Index{
		database:        database,
		sfile:           sfile,
		measurements:    make(map[string]*measurement),
		series:          make(map[string]*series),
		groups:          make(map[string]*tagGroup),                   // 一个组只有一个组key
		sequences:       make(map[string]*tagSequence),                // 一个序列是可以属于多个组的
		AdjacencyMatrix: make(map[int]map[int]map[int]*EdgeTimeIndex), // 初始化三维邻接矩阵
		//sequenceIDByGroupID: make(map[uint64]map[uint64]byte), // key为组ID，value为序列的ID集合（用map实现）
	}

	index.seriesSketch = hll.NewDefaultPlus()
	index.seriesTSSketch = hll.NewDefaultPlus()
	index.measurementsSketch = hll.NewDefaultPlus()
	index.measurementsTSSketch = hll.NewDefaultPlus()
	index.groupSketch = hll.NewDefaultPlus()
	index.groupTSSketch = hll.NewDefaultPlus()
	index.sequenceSketch = hll.NewDefaultPlus()
	index.sequenceTSSketch = hll.NewDefaultPlus()

	return index
}

func (i *Index) UniqueReferenceID() uintptr {
	return uintptr(unsafe.Pointer(i))
}

// Bytes estimates the memory footprint of this Index, in bytes.
func (i *Index) Bytes() int {
	var b int
	i.Mu.RLock()
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
	i.Mu.RUnlock()
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
	i.Mu.RLock()
	s := i.series[string(key)]
	i.Mu.RUnlock()
	return s, nil
}

// SeriesSketches returns the sketches for the series.
func (i *Index) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	i.Mu.RLock()
	defer i.Mu.RUnlock()
	return i.seriesSketch.Clone(), i.seriesTSSketch.Clone(), nil
}

// Measurement returns the measurement object from the index by the name
func (i *Index) Measurement(name []byte) (*measurement, error) {
	i.Mu.RLock()
	defer i.Mu.RUnlock()
	return i.measurements[string(name)], nil
}

// MeasurementExists returns true if the measurement exists.
func (i *Index) MeasurementExists(name []byte) (bool, error) {
	i.Mu.RLock()
	defer i.Mu.RUnlock()
	return i.measurements[string(name)] != nil, nil
}

// MeasurementsSketches returns the sketches for the measurements.
func (i *Index) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	i.Mu.RLock()
	defer i.Mu.RUnlock()
	return i.measurementsSketch.Clone(), i.measurementsTSSketch.Clone(), nil
}

// MeasurementsByName returns a list of measurements.
func (i *Index) MeasurementsByName(names [][]byte) ([]*measurement, error) {
	i.Mu.RLock()
	defer i.Mu.RUnlock()

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
		i.Mu.RLock()
		if max := opt.Config.MaxSeriesPerDatabase; max > 0 && len(i.series)+len(keys) > max {
			i.Mu.RUnlock()
			return errMaxSeriesPerDatabaseExceeded{limit: opt.Config.MaxSeriesPerDatabase}
		}
		i.Mu.RUnlock()
	}

	seriesIDs, err := i.sfile.CreateSeriesListIfNotExists(names, tagsSlice) // 会调用SeriesFile的创建
	if err != nil {
		return err
	}

	i.Mu.RLock()
	// If there is a series for this ID, it's already been added.
	seriesList := make([]*series, len(seriesIDs))
	for j, key := range keys {
		seriesList[j] = i.series[string(key)]
	}
	i.Mu.RUnlock()

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

	i.Mu.Lock()
	defer i.Mu.Unlock()

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

// ADD:Index的组标签添加函数
func (i *Index) CreateSeriesListIfNotExistsForGroup(measurements map[string]int, names [][]byte, opt *tsdb.EngineOptions, ignoreLimits bool,
	groupKeys, sequenceKeys [][]byte, groupTags, sequenceTags []models.Tags, groupsIDSet, sequencesIDSet *tsdb.SeriesIDSet) error {

	// Verify that the series will not exceed limit.
	if !ignoreLimits {
		i.Mu.RLock()
		if max := opt.Config.MaxSeriesPerDatabase; max > 0 && len(i.series)+len(sequenceKeys) > max {
			i.Mu.RUnlock()
			return errMaxSeriesPerDatabaseExceeded{limit: opt.Config.MaxSeriesPerDatabase}
		}
		i.Mu.RUnlock()
	}
	// 这里涉及到sfile了 连接 (这里存在的话返回ID，不存在的话返回新创建的ID)
	groupIDs, sequenceIDs, err := i.sfile.CreateSeriesListIfNotExistsForGroup(names, groupTags, sequenceTags)

	if err != nil {
		return err
	}

	i.Mu.RLock()
	// If there is a series for this ID, it's already been added.
	groupList := make([]*tagGroup, len(groupIDs))
	sequenceList := make([]*tagSequence, len(sequenceIDs))
	for j, key := range groupKeys {
		groupList[j] = i.groups[string(key)]
	}
	for j, key := range sequenceKeys {
		sequenceList[j] = i.sequences[string(key)]
	}
	i.Mu.RUnlock()

	var hasNewSeries bool
	for _, ss := range groupList {
		if ss == nil {
			hasNewSeries = true
			continue
		}

		// This series might need to be added to the local bitset, if the series
		// was created on another shard.
		groupsIDSet.Lock()
		if !groupsIDSet.ContainsNoLock(ss.ID) {
			groupsIDSet.AddNoLock(ss.ID)
			//measurements[ss.Measurement.Name]++
		}
		groupsIDSet.Unlock()
	}
	for _, ss := range sequenceList {
		if ss == nil {
			hasNewSeries = true
			continue
		}

		// This series might need to be added to the local bitset, if the series
		// was created on another shard.
		sequencesIDSet.Lock()
		for sequenceIDKey, _ := range ss.ID {
			if !sequencesIDSet.ContainsNoLock(sequenceIDKey) {
				sequencesIDSet.AddNoLock(sequenceIDKey)
				//measurements[ss.Measurement.Name]++
			}
		}
		sequencesIDSet.Unlock()
	}
	if !hasNewSeries {
		return nil
	}

	// get or create the measurement index
	// 以下创建倒排索引，暂时先用双层倒排索引
	mms := make([]*measurement, len(names))
	for j, name := range names {
		mms[j] = i.CreateMeasurementIndexIfNotExists([]byte(name))
	}

	i.Mu.Lock()
	defer i.Mu.Unlock()

	// Check for the series again under a write lock
	var newGroupN int
	for j, key := range groupKeys {
		if groupList[j] != nil {
			continue
		}

		ss := i.groups[string(key)]
		if ss == nil {
			newGroupN++
			continue
		}
		groupList[j] = ss

		// This series might need to be added to the local bitset, if the series
		// was created on another shard.
		groupsIDSet.Lock()
		if !groupsIDSet.ContainsNoLock(ss.ID) {
			groupsIDSet.AddNoLock(ss.ID)
			measurements[ss.Measurement.Name]++
		}
		groupsIDSet.Unlock()
	}

	var newSequenceN int
	for j, _ := range sequenceIDs {
		if sequenceList[j] != nil {
			continue
		}

		ss := i.sequences[string(sequenceKeys[j])]
		if ss == nil {
			newSequenceN++
			continue
		}
		sequenceList[j] = ss

		// This series might need to be added to the local bitset, if the series
		// was created on another shard.
		sequencesIDSet.Lock()
		for sequenceIDKey, _ := range ss.ID {
			if !sequencesIDSet.ContainsNoLock(sequenceIDKey) {
				sequencesIDSet.AddNoLock(sequenceIDKey)
			}
		}
		sequencesIDSet.Unlock()
	}

	if newGroupN == 0 && newSequenceN == 0 {
		return nil
	}

	for j, key := range groupKeys {
		if groupList[j] != nil || i.groups[string(key)] != nil {
			continue
		}
		gkey := string(key)
		ng := newGroup(groupIDs[j], mms[j], gkey, groupTags[j].Clone())
		i.groups[gkey] = ng

		mms[j].AddGroup(ng)

		i.groupSketch.Add(key)

		groupsIDSet.Lock()
		groupsIDSet.AddNoLock(groupIDs[j])
		measurements[mms[j].Name]++
		groupsIDSet.Unlock()
	}

	for j, _ := range sequenceIDs {
		if sequenceList[j] != nil || i.sequences[string(sequenceKeys[j])] != nil {
			mms[j].FlushGroupMap(groupIDs[j], sequenceIDs[j])
			// 还要维护sequence数据结构的map
			if sequenceItem := i.sequences[string(sequenceKeys[j])]; sequenceItem != nil {
				if _, ok := sequenceItem.ID[sequenceIDs[j]]; !ok {
					sequenceItem.ID[sequenceIDs[j]] = struct{}{}
				}
				mms[j].AddID2Sequence(groupIDs[j], sequenceIDs[j], sequenceItem)
				sequencesIDSet.Lock()
				sequencesIDSet.AddNoLock(sequenceIDs[j])
				measurements[mms[j].Name]++
				sequencesIDSet.Unlock()
			}

			continue
		}

		// set the in memory ID for query processing on this shard
		// The series key and tags are clone to prevent a memory leak
		skey := string(sequenceKeys[j])
		nseq := newSequence(sequenceIDs[j], mms[j], skey, sequenceTags[j].Clone())
		i.sequences[skey] = nseq

		mms[j].AddSequence(groupIDs[j], nseq) // 这里要维护关系
		mms[j].FlushGroupMap(groupIDs[j], sequenceIDs[j])

		//if _, ok := testMap[groupIDs[j]]; ok {
		//	fmt.Println(groupIDs[j], len(mms[j].groupMap[groupIDs[j]]))
		//}

		// Add the series to the series sketch.
		i.seriesSketch.Add(sequenceKeys[j])

		// This series needs to be added to the bitset tracking undeleted series IDs.
		sequencesIDSet.Lock()
		sequencesIDSet.AddNoLock(sequenceIDs[j])
		measurements[mms[j].Name]++
		sequencesIDSet.Unlock()
	}

	return nil
}

// CreateMeasurementIndexIfNotExists creates or retrieves an in memory index
// object for the measurement
func (i *Index) CreateMeasurementIndexIfNotExists(name []byte) *measurement {
	name = escape.Unescape(name)

	// See if the measurement exists using a read-lock
	i.Mu.RLock()
	m := i.measurements[string(name)]
	if m != nil {
		i.Mu.RUnlock()
		return m
	}
	i.Mu.RUnlock()

	// Doesn't exist, so lock the index to create it
	i.Mu.Lock()
	defer i.Mu.Unlock()

	// Make sure it was created in between the time we released our read-lock
	// and acquire the write lock
	m = i.measurements[string(name)]
	if m == nil {
		m = newMeasurement(i.database, string(name))
		m.SetGroupMetaData(i.groupMetaData.Group[i.database]) // 给measurement传入元数据
		i.measurements[string(name)] = m

		// Add the measurement to the measurements sketch.
		i.measurementsSketch.Add([]byte(name))
	}
	return m
}

// HasTagKey returns true if tag key exists.
func (i *Index) HasTagKey(name, key []byte) (bool, error) {
	i.Mu.RLock()
	mm := i.measurements[string(name)]
	i.Mu.RUnlock()

	if mm == nil {
		return false, nil
	}
	return mm.HasTagKey(string(key)), nil
}

// HasTagValue returns true if tag value exists.
func (i *Index) HasTagValue(name, key, value []byte) (bool, error) {
	i.Mu.RLock()
	mm := i.measurements[string(name)]
	i.Mu.RUnlock()

	if mm == nil {
		return false, nil
	}
	return mm.HasTagKeyValue(key, value), nil
}

// TagValueN returns the cardinality of a tag value.
func (i *Index) TagValueN(name, key []byte) int {
	i.Mu.RLock()
	mm := i.measurements[string(name)]
	i.Mu.RUnlock()

	if mm == nil {
		return 0
	}
	return mm.CardinalityBytes(key)
}

// MeasurementTagKeysByExpr returns an ordered set of tag keys filtered by an expression.
func (i *Index) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	i.Mu.RLock()
	mm := i.measurements[string(name)]
	i.Mu.RUnlock()

	if mm == nil {
		return nil, nil
	}
	return mm.TagKeysByExpr(expr)
}

// TagKeyHasAuthorizedSeries determines if there exists an authorized series for
// the provided measurement name and tag key.
func (i *Index) TagKeyHasAuthorizedSeries(auth query.Authorizer, name []byte, key string) bool {
	i.Mu.RLock()
	mm := i.measurements[string(name)]
	i.Mu.RUnlock()

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
	i.Mu.RLock()
	mm := i.measurements[string(name)]
	i.Mu.RUnlock()

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
	i.Mu.RLock()
	mm := i.measurements[string(name)]
	i.Mu.RUnlock()

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
	i.Mu.RLock()
	mm := i.measurements[string(name)]
	i.Mu.RUnlock()

	if mm == nil {
		return 0
	}
	return mm.CardinalityBytes(key)
}

// TagsForSeries returns the tag map for the passed in series
func (i *Index) TagsForSeries(key string) (models.Tags, error) {
	i.Mu.RLock()
	ss := i.series[key]
	i.Mu.RUnlock()

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
	i.Mu.RLock()
	defer i.Mu.RUnlock()

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
	i.Mu.RLock()
	defer i.Mu.RUnlock()

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
	i.Mu.Lock()
	defer i.Mu.Unlock()
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
	i.Mu.Lock()
	defer i.Mu.Unlock()

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

	i.Mu.Lock()
	defer i.Mu.Unlock()

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
	i.Mu.RLock()
	defer i.Mu.RUnlock()

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

// TODO: measurement查找tagSet
func (i *Index) TagSetsForGroup(groupIDSets, sequenceIDSets *tsdb.SeriesIDSet, name []byte, opt query.IteratorOptions) ([]*query.TagSet, error) {
	i.Mu.RLock()
	defer i.Mu.RUnlock()

	isExist := i.EdgeExistsAtRange(opt.SrcHash, opt.DstHash, opt.Fp, opt.StartTime, opt.EndTime)
	if isExist == false {
		return nil, nil
	}
	mm := i.measurements[string(name)]
	if mm == nil {
		return nil, nil
	}

	//访问倒排索引
	tagSets, err := mm.TagSetsForGroup2(groupIDSets, sequenceIDSets, opt)
	if err != nil {
		return nil, err
	}

	return tagSets, nil
}

func (i *Index) SeriesKeys() []string {
	i.Mu.RLock()
	s := make([]string, 0, len(i.series))
	for k := range i.series {
		s = append(s, k)
	}
	i.Mu.RUnlock()
	return s

}

// SetFieldSet sets a shared field set from the engine.
func (i *Index) SetFieldSet(fieldset *tsdb.MeasurementFieldSet) {
	i.Mu.Lock()
	defer i.Mu.Unlock()
	i.fieldset = fieldset
}

// FieldSet returns the assigned fieldset.
func (i *Index) FieldSet() *tsdb.MeasurementFieldSet {
	i.Mu.RLock()
	defer i.Mu.RUnlock()
	return i.fieldset
}

// SetFieldName adds a field name to a measurement.
func (i *Index) SetFieldName(measurement []byte, name string) {
	m := i.CreateMeasurementIndexIfNotExists(measurement)
	m.SetFieldName(name)
}

// ForEachMeasurementName iterates over each measurement name.
func (i *Index) ForEachMeasurementName(fn func(name []byte) error) error {
	i.Mu.RLock()
	mms := make(measurements, 0, len(i.measurements))
	for _, m := range i.measurements {
		mms = append(mms, m)
	}
	sort.Sort(mms)
	i.Mu.RUnlock()

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
	i.Mu.RLock()
	defer i.Mu.RUnlock()

	m := i.measurements[string(name)]
	if m == nil {
		return nil, nil
	}
	return tsdb.NewSeriesIDSliceIterator([]uint64(m.SeriesIDsByTagKey(key))), nil
}

func (i *Index) TagValueSeriesIDIterator(name, key, value []byte) (tsdb.SeriesIDIterator, error) {
	i.Mu.RLock()
	defer i.Mu.RUnlock()

	m := i.measurements[string(name)]
	if m == nil {
		return nil, nil
	}
	return tsdb.NewSeriesIDSliceIterator([]uint64(m.SeriesIDsByTagValue(key, value))), nil
}

func (i *Index) TagKeyIterator(name []byte) (tsdb.TagKeyIterator, error) {
	i.Mu.RLock()
	defer i.Mu.RUnlock()

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

func (i *Index) TagKeyIteratorForGroup(name []byte) (tsdb.TagKeyIterator, error) {
	i.Mu.RLock()
	defer i.Mu.RUnlock()

	m := i.measurements[string(name)]
	if m == nil {
		return nil, nil
	}
	keys := m.TagKeysForGroup()
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
	i.Mu.RLock()
	defer i.Mu.RUnlock()

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
	i.Mu.RLock()
	defer i.Mu.RUnlock()

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
	i.Mu.RLock()
	defer i.Mu.RUnlock()

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
	i.Mu.RLock()
	defer i.Mu.RUnlock()

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

		i.Mu.Lock()
		nm := m.Rebuild()

		i.measurements[string(name)] = nm
		i.Mu.Unlock()
		return nil
	})
}

// assignExistingSeries assigns the existing series to shardID and returns the series, names and tags that
// do not exists yet.
func (i *Index) assignExistingSeries(shardID uint64, seriesIDSet *tsdb.SeriesIDSet, measurements map[string]int,
	keys, names [][]byte, tagsSlice []models.Tags) ([][]byte, [][]byte, []models.Tags) {

	i.Mu.RLock()
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
	i.Mu.RUnlock()
	return keys[:n], names[:n], tagsSlice[:n]
}

func (i *Index) assignExistingSeriesForGroup(shardID uint64, measurements map[string]int, names [][]byte,
	groupKey, sequenceKey [][]byte, groupTags, sequenceTags []models.Tags, groupIDSet, sequenceIDSet *tsdb.SeriesIDSet) ([][]byte, []models.Tags, []models.Tags) {

	i.Mu.RLock()
	var n int

	for j, key := range groupKey {
		if ss := i.groups[string(key)]; ss == nil { //组key不存在
			names[n] = names[j]
			groupKey[n] = groupKey[j]
			sequenceKey[n] = sequenceKey[j]
			groupTags[n] = groupTags[j]
			sequenceTags[n] = sequenceTags[j]
			n++
		} else {
			// 加入组的bitmap
			if !groupIDSet.Contains(ss.ID) {
				groupIDSet.Lock()
				if !groupIDSet.ContainsNoLock(ss.ID) {
					groupIDSet.AddNoLock(ss.ID)
					//measurements[string(names[j])]++ // 不知道干嘛的
				}
				groupIDSet.Unlock()
			}
			if se := i.sequences[string(sequenceKey[j])]; se == nil { //序列key不存在
				names[n] = names[j]
				groupKey[n] = groupKey[j]
				sequenceKey[n] = sequenceKey[j]
				groupTags[n] = groupTags[j]
				sequenceTags[n] = sequenceTags[j]
				n++
			} else { // 组和序列都存在
				for seid, _ := range se.ID {
					if !sequenceIDSet.Contains(seid) {
						sequenceIDSet.Lock()
						if !sequenceIDSet.ContainsNoLock(seid) {
							sequenceIDSet.AddNoLock(seid)
							//measurements[string(names[j])]++
						}
						sequenceIDSet.Unlock()
					}
				}
			}
		}
	}

	i.Mu.RUnlock()
	return names[:n], groupTags[:n], sequenceTags[:n]
}

// ADD (1)把不存在的series数据挑出来返回（2）对已经存在的时间线，加入bitmap
func (i *Index) assignExistingSeriesForGroup2(shardID uint64, measurements map[string]int, names [][]byte,
	groupKey, sequenceKey [][]byte, groupTags, sequenceTags []models.Tags, groupIDSet, sequenceIDSet *tsdb.SeriesIDSet, ts []int64) ([][]byte, []models.Tags, []models.Tags) {

	i.Mu.RLock()
	var n int

	for j, key := range groupKey {
		if ss := i.groups[string(key)]; ss == nil { //组key不存在
			names[n] = names[j]
			groupKey[n] = groupKey[j]
			sequenceKey[n] = sequenceKey[j]
			groupTags[n] = groupTags[j]
			sequenceTags[n] = sequenceTags[j]
			n++
		} else {
			// 加入组的bitmap
			if !groupIDSet.Contains(ss.ID) {
				groupIDSet.Lock()
				if !groupIDSet.ContainsNoLock(ss.ID) {
					groupIDSet.AddNoLock(ss.ID)
					//measurements[string(names[j])]++ // 不知道干嘛的
				}
				groupIDSet.Unlock()
			}
			if se := i.sequences[string(sequenceKey[j])]; se == nil { //序列key不存在
				names[n] = names[j]
				groupKey[n] = groupKey[j]
				sequenceKey[n] = sequenceKey[j]
				groupTags[n] = groupTags[j]
				sequenceTags[n] = sequenceTags[j]
				n++
			} else { // 组和序列都存在
				for seid, _ := range se.ID {
					if !sequenceIDSet.Contains(seid) {
						sequenceIDSet.Lock()
						if !sequenceIDSet.ContainsNoLock(seid) {
							sequenceIDSet.AddNoLock(seid)
							//measurements[string(names[j])]++
						}
						sequenceIDSet.Unlock()
					}
				}
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
	events := make([]edgeEvent, 0, len(sequenceTags))

	// for j, tags := range sequenceTags {
	// 	var srcHash, dstHash, bucket int
	// 	for _, tag := range tags {
	// 		if string(tag.Key) == "source" {
	// 			srcHash, _ = strconv.Atoi(string(tag.Value))
	// 		} else if string(tag.Key) == "destination" {
	// 			dstHash, _ = strconv.Atoi(string(tag.Value))
	// 		} else if string(tag.Key) == "fp" {
	// 			bucket, _ = strconv.Atoi(string(tag.Value))
	// 		}
	// 	}
	// 	events = append(events, edgeEvent{
	// 		src:    srcHash,
	// 		dst:    dstHash,
	// 		bucket: bucket,
	// 		ts:     toSec(ts[j]),
	// 	})

	// }
	for j, tags := range sequenceTags {
		var srcHash, dstHash, bucket int
		for _, tag := range tags {
			if string(tag.Key) == "destination" {
				dstHash, _ = strconv.Atoi(string(tag.Value))
			} else if string(tag.Key) == "fp" {
				bucket, _ = strconv.Atoi(string(tag.Value))
			}
		}
		for _, tag := range groupTags[j] {
			if string(tag.Key) == "source" {
				srcHash, _ = strconv.Atoi(string(tag.Value))
				break
			}

		}
		events = append(events, edgeEvent{
			src:    srcHash,
			dst:    dstHash,
			bucket: bucket,
			ts:     toSec(ts[j]),
		})

	}

	i.Mu.RUnlock()

	for _, e := range events {
		i.AddEdgeEvent(e.src, e.dst, e.bucket, e.ts)
	}

	return names[:n], groupTags[:n], sequenceTags[:n]
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

	// 组ID和序列ID的bitmap
	groupIDSet    *tsdb.SeriesIDSet
	sequenceIDSet *tsdb.SeriesIDSet

	// mapping of measurements to the count of series ids in the set. protected
	// by the seriesIDSet lock.
	measurements map[string]int

	//database string

	opt tsdb.EngineOptions
}

//func (idx *ShardIndex) SetDatabase(database string) {
//	idx.database = database
//}

func (idx *ShardIndex) IndexAnalyse(shardId uint64) {
	//TODO implement me
	panic("implement me")
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

// CreateSeriesListIfNotExists creates a list of series if they doesn't exist in bulk.  nxl插入数据的时候会维护时间线索引
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

func (idx *ShardIndex) CreateSeriesListIfNotExistsForGroup(names [][]byte,
	groupKey, sequenceKey [][]byte, groupTags, sequenceTags []models.Tags) error {
	//返回的是索引中不存在的series
	names, groupTags, sequenceTags = idx.assignExistingSeriesForGroup(idx.id, idx.measurements, names, groupKey, sequenceKey, groupTags, sequenceTags, idx.groupIDSet, idx.sequenceIDSet)
	if len(names) == 0 {
		return nil
	}
	var (
		reason              string
		droppedGroupKeys    [][]byte
		droppedSequenceKeys [][]byte
	)

	// Ensure that no tags go over the maximum cardinality.
	if maxValuesPerTag := idx.opt.Config.MaxValuesPerTag; maxValuesPerTag > 0 {
		var n int

	outer:
		for i, name := range names { //判断标签基数是否超限
			tags := groupTags[i]
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
				droppedGroupKeys = append(droppedGroupKeys, groupKey[i])
				droppedSequenceKeys = append(droppedSequenceKeys, sequenceKey[i])
				continue outer
			}

			tags = sequenceTags[i]
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
				droppedGroupKeys = append(droppedGroupKeys, groupKey[i])
				droppedSequenceKeys = append(droppedSequenceKeys, sequenceKey[i])
				continue outer
			}

			// Increment success count if all checks complete.
			if n != i {
				names[n], groupKey[n], sequenceKey[n], groupTags[n], sequenceTags[n] = names[i], groupKey[i], sequenceKey[i], groupTags[i], sequenceTags[i]
			}
			n++
		}

		// Slice to only include successful points.
		names, groupKey, sequenceKey, groupTags, sequenceTags = names[:n], groupKey[:n], sequenceKey[:n], groupTags[:n], sequenceTags[:n]
	}
	// 上面都是挑选出合法的数据
	if err := idx.Index.CreateSeriesListIfNotExistsForGroup(idx.measurements, names, &idx.opt, idx.opt.Config.MaxSeriesPerDatabase == 0,
		groupKey, sequenceKey, groupTags, sequenceTags, idx.groupIDSet, idx.sequenceIDSet); err != nil {
		reason = err.Error()
		droppedGroupKeys = append(droppedGroupKeys, groupKey...)
		droppedSequenceKeys = append(droppedSequenceKeys, sequenceKey...)
	}

	// Report partial writes back to shard.
	if len(droppedGroupKeys) > 0 {
		dropped := make([][]byte, len(droppedGroupKeys)) // number dropped before deduping

		for i := 0; i < len(droppedGroupKeys); i++ {
			// 使用append函数将两个一维字节数组合并
			dropped[i] = append(droppedGroupKeys[i], droppedSequenceKeys[i]...)
		}
		bytesutil.SortDedup(droppedGroupKeys)
		return tsdb.PartialWriteError{
			Reason:      reason,
			Dropped:     len(dropped),
			DroppedKeys: dropped,
		}
	}

	return nil
}

// ADD:添加inmem的添加时间线函数
func (idx *ShardIndex) CreateSeriesListIfNotExistsForGroup2(names [][]byte,
	groupKey, sequenceKey [][]byte, groupTags, sequenceTags []models.Tags, ts []int64) error {
	//返回的是索引中不存在的series
	names, groupTags, sequenceTags = idx.assignExistingSeriesForGroup2(idx.id, idx.measurements, names, groupKey, sequenceKey, groupTags, sequenceTags, idx.groupIDSet, idx.sequenceIDSet, ts)
	if len(names) == 0 {
		return nil
	}
	var (
		reason              string
		droppedGroupKeys    [][]byte
		droppedSequenceKeys [][]byte
	)

	// Ensure that no tags go over the maximum cardinality.
	if maxValuesPerTag := idx.opt.Config.MaxValuesPerTag; maxValuesPerTag > 0 {
		var n int

	outer:
		for i, name := range names { //判断标签基数是否超限
			tags := groupTags[i]
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
				droppedGroupKeys = append(droppedGroupKeys, groupKey[i])
				droppedSequenceKeys = append(droppedSequenceKeys, sequenceKey[i])
				continue outer
			}

			tags = sequenceTags[i]
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
				droppedGroupKeys = append(droppedGroupKeys, groupKey[i])
				droppedSequenceKeys = append(droppedSequenceKeys, sequenceKey[i])
				continue outer
			}

			// Increment success count if all checks complete.
			if n != i {
				names[n], groupKey[n], sequenceKey[n], groupTags[n], sequenceTags[n] = names[i], groupKey[i], sequenceKey[i], groupTags[i], sequenceTags[i]
			}
			n++
		}

		// Slice to only include successful points.
		names, groupKey, sequenceKey, groupTags, sequenceTags = names[:n], groupKey[:n], sequenceKey[:n], groupTags[:n], sequenceTags[:n]
	}
	// 上面都是挑选出合法的数据
	if err := idx.Index.CreateSeriesListIfNotExistsForGroup(idx.measurements, names, &idx.opt, idx.opt.Config.MaxSeriesPerDatabase == 0,
		groupKey, sequenceKey, groupTags, sequenceTags, idx.groupIDSet, idx.sequenceIDSet); err != nil {
		reason = err.Error()
		droppedGroupKeys = append(droppedGroupKeys, groupKey...)
		droppedSequenceKeys = append(droppedSequenceKeys, sequenceKey...)
	}

	// Report partial writes back to shard.
	if len(droppedGroupKeys) > 0 {
		dropped := make([][]byte, len(droppedGroupKeys)) // number dropped before deduping

		for i := 0; i < len(droppedGroupKeys); i++ {
			// 使用append函数将两个一维字节数组合并
			dropped[i] = append(droppedGroupKeys[i], droppedSequenceKeys[i]...)
		}
		bytesutil.SortDedup(droppedGroupKeys)
		return tsdb.PartialWriteError{
			Reason:      reason,
			Dropped:     len(dropped),
			DroppedKeys: dropped,
		}
	}

	return nil
}

// SeriesN returns the number of unique non-tombstoned series local to this shard.
func (idx *ShardIndex) SeriesN() int64 {
	idx.Mu.RLock()
	defer idx.Mu.RUnlock()
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

// ADD：添加组标签的时间线添加函数
func (idx *ShardIndex) CreateSeriesIfNotExistsForGroup(name []byte, groupKey,
	sequenceKey []byte, groupTags, sequenceTags models.Tags) error {
	return idx.Index.CreateSeriesListIfNotExistsForGroup(idx.measurements, [][]byte{name},
		&idx.opt, false, [][]byte{groupKey}, [][]byte{sequenceKey}, []models.Tags{groupTags}, []models.Tags{sequenceTags}, idx.groupIDSet, idx.sequenceIDSet)
}

// TagSets returns a list of tag sets based on series filtering.
// TODO: 处理TagSet
func (idx *ShardIndex) TagSets(name []byte, opt query.IteratorOptions) ([]*query.TagSet, error) {
	if !nxl.ValidForGroup(idx.database) { // 这里要传入database
		return idx.Index.TagSets(idx.seriesIDSet, name, opt)
	} else {
		return idx.Index.TagSetsForGroup(idx.groupIDSet, idx.sequenceIDSet, name, opt)
	}
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

func NewShardIndexForGroup(id uint64, seriesIDSet, groupIDSet, sequenceIDSet *tsdb.SeriesIDSet, opt tsdb.EngineOptions) tsdb.Index {
	return &ShardIndex{
		Index:         opt.InmemIndex.(*Index),
		id:            id,
		seriesIDSet:   seriesIDSet,
		groupIDSet:    groupIDSet,
		sequenceIDSet: sequenceIDSet,
		measurements:  make(map[string]int),
		opt:           opt,
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
