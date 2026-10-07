package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/nxl"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/tsdb/index/inmem"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	// 获取文件路径和大小参数
	filePath := os.Args[1]
	forGroupTmp := os.Args[2]
	numMetricsTmp := os.Args[3]
	numMetrics, _ := strconv.Atoi(numMetricsTmp)
	forGroup := false
	if forGroupTmp == "true" {
		forGroup = true
	}
	wb := &writeBenchmark{
		outPath:     filepath.Join("benchout", "group"),
		samplesFile: filePath,
		forGroup:    forGroup,
		numMetrics:  numMetrics,
		//samplesFile: "D:\\desktop\\data\\cpu-only\\influx-cpu-only-10000",
		//forGroup:    true,
		//numMetrics:  12000000,
	}

	wb.run()
}

type writeBenchmark struct {
	outPath     string
	samplesFile string
	forGroup    bool
	numMetrics  int
	storage     *tsdb.Shard
}

func (b *writeBenchmark) run() {

	groupMetaData, _ := parseJSONFile("/home/luwenqi/influxdb-engine-test-dir/group.json")
	nxl.SelectGroup = b.forGroup

	// 创建Shard
	tmpDir := "/home/luwenqi/influxdb-engine-test-dir/datadir"
	tmpShard := filepath.Join(tmpDir, "shard")
	tmpWal := filepath.Join(tmpDir, "wal")
	database := "nxl"

	sfile := tsdb.NewSeriesFile(tmpDir)

	if !b.forGroup {
		if err := sfile.Open(); err != nil {
			exitWithError(err)
		}
	} else {
		if err := sfile.OpenForGroup(); err != nil {
			exitWithError(err)
		}
	}

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.Config.CacheSnapshotMemorySize = 67108864
	opts.InmemIndex = inmem.NewIndex(filepath.Base(tmpDir), sfile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile, opts)
	sh.SetGroupMetaData(groupMetaData)

	if err := sh.Open(); err != nil {
		exitWithError(err)
	}

	// #########################################################################此处写入数据
	useTime := int64(0)
	f, err := os.Open(b.samplesFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// 逐批次读取文件
	scanner := bufio.NewScanner(f)
	lines := make([]string, 0, 10000) // 切片用于存储一批数据

	for scanner.Scan() {
		// 解析InfluxDB Line Protocol格式的数据点
		point := scanner.Text()

		lines = append(lines, point)

		// 如果已经收集了 10,000 行数据，就执行批量写入
		if len(lines) == 10000 {
			joined := strings.Join(lines, "\n")
			byteData := []byte(joined)

			points, _ := models.ParsePointsWithPrecision(byteData, time.Now().UTC(), "ns")
			startTime := time.Now()
			if b.forGroup {
				MapPoint2GroupPoint(database, groupMetaData, points)
			}
			err = sh.WritePoints(points)
			if err != nil {
				exitWithError(err)
			}
			endTime := time.Now()
			useTime += (endTime.Sub(startTime)).Nanoseconds()
			lines = lines[:0] // 清空切片
		}
	}

	// 处理剩余未达到 10,000 行的数据
	if len(lines) > 0 {
		joined := strings.Join(lines, ",")
		points, _ := models.ParsePointsWithPrecision([]byte(joined), time.Now().UTC(), "ns")
		startTime := time.Now()
		if b.forGroup {
			MapPoint2GroupPoint(database, groupMetaData, points)
		}
		err = sh.WritePoints(points)
		if err != nil {
			exitWithError(err)
		}
		endTime := time.Now()
		useTime += (endTime.Sub(startTime)).Nanoseconds()
	}

	fmt.Println(" > total samples:", b.numMetrics)
	fmt.Println(" > use times:", useTime/1000/1000/1000)
	fmt.Println(" > samples/sec:", float64(b.numMetrics)/(float64(useTime)/1000/1000/1000))
}

func MapPoint2GroupPoint(database string, groupMetaData *meta.GroupMetaData, points []models.Point) error {

	const NULL_VALUE = "####"
	databaseGroupMeta := groupMetaData.Group[database]

	for _, p := range points {
		msName := p.Name()
		msGroupData := databaseGroupMeta.Group[string(msName)]
		if msGroupData == nil {
			//fmt.Println("不存在该组的组元数据")
			continue
		}
		curTags := p.Tags()

		var groupTag models.Tags
		var sequenceTag models.Tags

		index := 0
		curIndex := 0   // 当前数据点的标签
		groupIndex := 0 // 组标签遍历索引

		// 此处处理缺失数据, 对缺失标签采用空值填充
		// 扫描算法：首先模式的组标签和point的标签数据点是按字符串排好序的

		for ; index < len(msGroupData.TagKeys); index++ {
			shouldHandelTag := msGroupData.TagKeys[index]                                    // 当前应该处理的标签
			if curIndex < len(curTags) && string(curTags[curIndex].Key) == shouldHandelTag { // 标签一一对应了，没缺失
				if groupIndex < len(msGroupData.GroupTags) && shouldHandelTag == msGroupData.GroupTags[groupIndex] { // 是组标签
					groupTag = append(groupTag, curTags[curIndex])
					curIndex++
					groupIndex++
				} else { // 是序列标签
					sequenceTag = append(sequenceTag, curTags[curIndex])
					curIndex++
				}
			} else { // 如果当前处理的标签不在预定模式的标签里，说明当前应该访问的标签shouldHandelTag缺失了
				missingTag := models.Tag{Key: []byte(shouldHandelTag), Value: []byte(NULL_VALUE)}
				if groupIndex < len(msGroupData.GroupTags) && string(missingTag.Key) == msGroupData.GroupTags[groupIndex] { // 是组标签
					groupTag = append(groupTag, missingTag)
					groupIndex++
				} else { // 是序列标签
					sequenceTag = append(sequenceTag, missingTag)
				}
			}

		}

		p.SetGroupTags(groupTag)
		p.SetSequenceTags(sequenceTag)

	}
	return nil
}

func measureTime(stage string, f func()) time.Duration {
	fmt.Printf(">> start stage=%s\n", stage)
	start := time.Now()
	f()
	fmt.Printf(">> completed stage=%s duration=%s\n", stage, time.Since(start))
	return time.Since(start)
}

func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

type Shard struct {
	path            string // path 指的是tsm file所在的位置
	walPath         string // wal path指的是wal所在的位置
	id              uint64
	database        string //shard所属的database
	retentionPolicy string
	sfile           *tsdb.SeriesFile    // series 是database的概念的，也就是一个database下面所有的shard是共享这个数据结构的。这一点在存储的结构上也能看出来
	groupMetaData   *meta.GroupMetaData // 传进组元数据
	mu              sync.RWMutex
	_engine         Engine     //tsm engine，负责存储的主要功能。
	index           ShardIndex //index 索引。
}

type Index struct {
	mu sync.RWMutex

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

type measurement struct {
	Database  string
	Name      string `json:"name,omitempty"`
	NameBytes []byte // cached version as []byte

	mu         sync.RWMutex
	fieldNames map[string]struct{}

	// in-memory index fields
	seriesByID          map[uint64]*series      // lookup table for series by their id
	seriesByTagKeyValue map[string]*tagKeyValue // map from tag key to value to sorted set of series ids

	// ADD:新的倒排索引
	groupByID             map[uint64]*tagGroup               // 组
	sequenceByID          map[uint64]*tagSequence            // 序列
	groupByTagKeyValue    map[string]*tagKeyValue            // 组倒排  里面只需要保存组到序列的映射
	sequenceByTagKeyValue map[string]*tagKeyValueForSequence // 序列倒排  需要保存序列到组的映射
	groupMap              map[uint64]map[uint64]uint64       // 第一级为组key， 第二级为序列key
	// 组标签数据
	groupMetaData *meta.SingleDBGroupMetaData

	// lazyily created sorted series IDs
	sortedSeriesIDs   seriesIDs // sorted list of series IDs in this measurement
	sortedGroupIDs    seriesIDs // sorted list of Group IDs in this measurement
	sortedSequenceIDs seriesIDs // sorted list of Sequence IDs in this measurement
	groupIsSorted     bool
	sequenceIsSorted  bool

	// Indicates whether the seriesByTagKeyValueMap needs to be rebuilt as it contains deleted series
	// that waste memory.
	dirty bool
}

type series struct {
	mu      sync.RWMutex
	deleted bool

	// immutable
	ID          uint64
	Measurement *measurement
	Key         string
	Tags        models.Tags
}

type tagGroup struct {
	mu      sync.RWMutex
	deleted bool
	// immutable
	ID          uint64
	Measurement *measurement
	Key         string
	Tags        models.Tags
}

type tagSequence struct {
	mu      sync.RWMutex
	deleted bool
	// immutable
	// 因为存在因为序列内容一样而ID不一样的情况，所以需要单独处理，所以这个内容相同的序列会包含很多不同的ID
	ID          map[uint64]struct{}
	Measurement *measurement
	Key         string
	Tags        models.Tags
}

type tagKeyValue struct {
	mu      sync.RWMutex
	entries map[string]*tagKeyValueEntry
}

type tagKeyValueForSequence struct {
	mu      sync.RWMutex
	entries map[string]*tagKeyValueEntryForSequence
}

type tagKeyValueEntry struct {
	m map[uint64]struct{} // series id set
	a seriesIDs           // lazily sorted list of series.
}

type tagKeyValueEntryForSequence struct {
	m map[uint64]uint64 // series id set  // 加上关联的组ID
	a seriesIDs         // lazily sorted list of series.
}

type seriesIDs []uint64

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

type Engine struct {
	mu           sync.RWMutex
	index        tsdb.Index
	database     string
	wg           *sync.WaitGroup // waitgroup for active level compaction goroutines level合并用的
	done         chan struct{}   // channel to signal level compactions to stop
	levelWorkers int             // Number of "workers" that expect compactions to be in a disabled state
	// cache刷盘用到的变量
	snapDone chan struct{}   // channel to signal snapshot compactions to stop
	snapWG   *sync.WaitGroup // waitgroup for running snapshot compactions

	// 组元数据
	groupMetaData *meta.GroupMetaData // 传进组元数据

	id           uint64
	path         string
	sfile        *tsdb.SeriesFile
	logger       *zap.Logger // Logger to be used for important messages
	traceLogger  *zap.Logger // Logger to be used when trace-logging is on.
	traceLogging bool

	fieldset *tsdb.MeasurementFieldSet

	Cache     *tsm1.Cache
	FileStore *tsm1.FileStore

	// provides access to the total set of series IDs
	seriesIDSets tsdb.SeriesIDSets
}

func parseJSONFile(filePath string) (*meta.GroupMetaData, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var jsonData meta.JSONData
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, err
	}

	groupMetaData := meta.GroupMetaData{
		Group: make(map[string]*meta.SingleDBGroupMetaData),
	}

	for _, measurementGroup := range jsonData.MeasurementGroup {
		singleMetaData := &meta.SingleMsMetaData{
			TagKeys:   make([]string, 0),
			GroupTags: make([]string, 0),
			FieldKeys: make([]string, 0),
			Fields:    make(map[string]*meta.Field),
		}

		tagKeys := strings.Split(measurementGroup.TagKeys, ",")
		sort.Strings(tagKeys)
		for _, key := range tagKeys {
			key = strings.TrimSpace(key)
			if key != "" {
				singleMetaData.TagKeys = append(singleMetaData.TagKeys, key)
			}
		}

		groupTags := strings.Split(measurementGroup.GroupTags, ",")
		sort.Strings(groupTags)
		for _, key := range groupTags {
			key = strings.TrimSpace(key)
			if key != "" {
				singleMetaData.GroupTags = append(singleMetaData.GroupTags, key)
			}
		}

		fieldKeys := strings.Split(measurementGroup.FieldKeys, ",")
		sort.Strings(fieldKeys)
		for _, key := range fieldKeys {
			key = strings.TrimSpace(key)
			if key != "" {
				tmp := strings.Split(key, "(")
				realKey := tmp[0]
				datatype := influxql.DataTypeFromString(tmp[1][:len(tmp[1])-1])
				id := uint8(len(singleMetaData.Fields))
				field := &meta.Field{ID: id, Name: realKey, Type: datatype}
				singleMetaData.Fields[realKey] = field
				singleMetaData.FieldKeys = append(singleMetaData.FieldKeys, realKey)
			}
		}

		if _, exist := groupMetaData.Group[measurementGroup.DataBase]; !exist {
			groupMetaData.Group[measurementGroup.DataBase] = &meta.SingleDBGroupMetaData{
				Group: make(map[string]*meta.SingleMsMetaData),
			}
		}

		groupMetaData.Group[measurementGroup.DataBase].Group[measurementGroup.MsName] = singleMetaData
	}

	// 解析数据类型

	return &groupMetaData, nil
}
