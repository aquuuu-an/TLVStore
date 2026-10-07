package nxl

import (
	"fmt"
	"os"
	"time"
)

// 以下全局变量控制结果输出
var SelectGroup bool            // 为组进行实验
var CacheAnalyseOption bool     // 是否统计Cache内容
var CacheFlushTimeOption bool   // 是否统计Cache刷盘时间
var IndexAnalyseOption bool     // 是否统计倒排索引内容
var IndexTimeAnalyseOption bool // 统计倒排索引维护时间

// 变量
var LRUCacheSize int // 缓存大小

// 文件输出路径
var CacheFilePath string      // cache统计内容
var CacheFlushTimePath string // cache刷盘时间文件路径
var IndexFilePath string      // index统计输出路径
var IndexTimeFilePath string  // index插入时间统计输出路径

// 是组的数据库返回true
func ValidForGroup(database string) bool {
	if SelectGroup {
		return forGroup(database)
	} else {
		return forOld(database)
	}
}

func IsForGroup() bool {
	return SelectGroup
}

func forOld(database string) bool {
	if database == "_internal" || database == "test" {
		return false
	}
	return true
}

func forGroup(database string) bool {
	if database == "_internal" {
		return false
	}
	return true
}

func AnalyseCacheContent() bool { // 是否统计Cache内容
	return CacheAnalyseOption
}

func AnalyseCacheFlushTime() bool { // 是否统计Cache刷盘时间
	return CacheFlushTimeOption
}

func AnalyseIndexContent() bool { // 是否统计倒排索引内容
	return IndexAnalyseOption
}

func AnalyseIndexTime() bool { // 是否统计倒排索引维护时间
	return IndexTimeAnalyseOption
}

// 写入倒排索引维护时间到文件
func RecordIndexTime(start, end time.Time) { // 记录倒排索引维护时间
	s := start.UnixNano()
	e := end.UnixNano()
	logContent := fmt.Sprintf("start=%d, end=%d\n", s, e)
	err := RecordTimeWriteToFile(IndexTimeFilePath, logContent)
	if err != nil {
		return
	}
}

// 写入刷盘时间到文件
func RecordCacheFlushTime(start, end time.Time) { // 记录刷盘时间
	s := start.UnixNano()
	e := end.UnixNano()
	logContent := fmt.Sprintf("start=%d, end=%d\n", s, e)
	err := RecordTimeWriteToFile(CacheFlushTimePath, logContent)
	if err != nil {
		return
	}
}

func RecordTimeWriteToFile(fileName, content string) error { // 记录倒排索引维护时间
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	file.WriteString(content)
	return nil
}
