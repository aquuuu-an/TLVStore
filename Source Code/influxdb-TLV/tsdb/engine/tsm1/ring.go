package tsm1

import (
	"fmt"
	"github.com/cespare/xxhash"
	"github.com/influxdata/influxdb/pkg/bytesutil"
	"sync"
	"sync/atomic"
)

// partitions is the number of partitions we used in the ring's continuum. It
// basically defines the maximum number of partitions you can have in the ring.
// If a smaller number of partitions are chosen when creating a ring, then
// they're evenly spread across this many partitions in the ring.
const partitions = 16

// ring 是一种将系列键映射到条目的结构。
// ring 被实现为一个粗略的哈希环，你可以拥有环中可变数量的成员，以及 a 的适当成员
// 始终可以找到给定的系列键。不同于真正的哈希环
// 不过，这个环是不可调整大小的——该环中最多只能有 256 个成员
// 环，成员数必须始终为 2 的幂。
// ring 的工作原理如下：ring 的每个成员都包含一个 store，它
// 包含系列键到条目的映射。一个环总是有 256 个分区，
// 一个成员占用一个或多个这些分区（取决于有多少
// 成员被指定在环中）
// 要确定应该将系列键添加到的分区，系列
// 对密钥进行哈希处理，前 8 位用作环的索引。
// ring is a structure that maps series keys to entries.
//
// ring is implemented as a crude hash ring, in so much that you can have
// variable numbers of members in the ring, and the appropriate member for a
// given series key can always consistently be found. Unlike a true hash ring
// though, this ring is not resizeable—there must be at most 256 members in the
// ring, and the number of members must always be a power of 2.
//
// ring works as follows: Each member of the ring contains a single store, which
// contains a map of series keys to entries. A ring always has 256 partitions,
// and a member takes up one or more of these partitions (depending on how many
// members are specified to be in the ring)
//
// To determine the partition that a series key should be added to, the series
// key is hashed and the first 8 bits are used as an index to the ring.
type ring struct {
	// Number of keys within the ring. This is used to provide a hint for
	// allocating the return values in keys(). It will not be perfectly accurate
	// since it doesn't consider adding duplicate keys, or trying to remove non-
	// existent keys.
	keysHint int64

	// The unique set of partitions in the ring.
	// len(partitions) <= len(continuum)
	partitions []*partition
}

type ringForGroup struct {
	// Number of keys within the ring. This is used to provide a hint for
	// allocating the return values in keys(). It will not be perfectly accurate
	// since it doesn't consider adding duplicate keys, or trying to remove non-
	// existent keys.
	keysHint int64

	// The unique set of partitions in the ring.
	// len(partitions) <= len(continuum)
	partitions []*partitionForGroup
}

func (r *ring) memSizeAndCount() (keyMemSize uint64, valueMemSize uint64, keyCount uint64, valueCount uint64) {
	for _, partition := range r.partitions {
		kms, vms, kc, vc := partition.memSizeAndCount()
		keyMemSize += kms
		valueMemSize += vms
		keyCount += kc
		valueCount += vc
	}
	return
}

func (r *ringForGroup) memSizeAndCount() (keyMemSize uint64, valueMemSize uint64, gKeyMemSize uint64, sKeyMemSize uint64,
	groupKeyCount uint64, sequenceKeyCount uint64, valueCount uint64) {
	for _, partition := range r.partitions {
		kms, vms, gkms, skms, gkc, skc, vc := partition.memSizeAndCount()
		keyMemSize += kms
		valueMemSize += vms
		gKeyMemSize += gkms
		sKeyMemSize += skms
		groupKeyCount += gkc
		sequenceKeyCount += skc
		valueCount += vc
	}
	return
}

// newring returns a new ring initialised with n partitions. n must always be a
// power of 2, and for performance reasons should be larger than the number of
// cores on the host. The supported set of values for n is:
//
//	{1, 2, 4, 8, 16, 32, 64, 128, 256}.
func newring(n int) (*ring, error) {
	if n <= 0 || n > partitions {
		return nil, fmt.Errorf("invalid number of paritions: %d", n)
	}

	r := ring{
		partitions: make([]*partition, n), // maximum number of partitions.
	}

	// The trick here is to map N partitions to all points on the continuum,
	// such that the first eight bits of a given hash will map directly to one
	// of the N partitions.
	for i := 0; i < len(r.partitions); i++ {
		r.partitions[i] = &partition{
			store: make(map[string]*entry),
		}
	}
	return &r, nil
}

func newRingForGroup(n int) (*ringForGroup, error) {
	if n <= 0 || n > partitions {
		return nil, fmt.Errorf("invalid number of paritions: %d", n)
	}

	r := ringForGroup{
		partitions: make([]*partitionForGroup, n), // maximum number of partitions.
	}

	// The trick here is to map N partitions to all points on the continuum,
	// such that the first eight bits of a given hash will map directly to one
	// of the N partitions.
	for i := 0; i < len(r.partitions); i++ {
		r.partitions[i] = &partitionForGroup{
			store: make(map[string]map[string]*MultiEntry),
		}
	}
	return &r, nil
}

// reset resets the ring so it can be reused. Before removing references to entries
// within each partition it gathers sizing information to provide hints when
// reallocating entries in partition maps.
//
// reset is not safe for use by multiple goroutines.
func (r *ring) reset() {
	for _, partition := range r.partitions {
		partition.reset()
	}
	r.keysHint = 0
}

func (r *ringForGroup) resetForGroup() {
	for _, partition := range r.partitions {
		partition.resetForGroup()
	}
	r.keysHint = 0
}

// getPartition retrieves the hash ring partition associated with the provided
// key.
func (r *ring) getPartition(key []byte) *partition {
	return r.partitions[int(xxhash.Sum64(key)%uint64(len(r.partitions)))]
}

// 通过组key得到分区
func (r *ringForGroup) getPartition(groupKey []byte) *partitionForGroup {
	return r.partitions[int(xxhash.Sum64(groupKey)%uint64(len(r.partitions)))]
}

// entry returns the entry for the given key.
// entry is safe for use by multiple goroutines.
func (r *ring) entry(key []byte) *entry {
	return r.getPartition(key).entry(key)
}

func (r *ringForGroup) sequenceMap(groupKey []byte) map[string]*MultiEntry {
	return r.getPartition(groupKey).entry(groupKey)
}

func (r *ringForGroup) entryForGroup2(groupKey []byte) map[string]*MultiEntry {
	return r.getPartition(groupKey).entry(groupKey)
}

// 返回组的二级map
func (r *ringForGroup) entryForGroup(groupKey []byte, sequenceKey []byte) *MultiEntry {
	sequenceMap := r.getPartition(groupKey).entry(groupKey)
	return sequenceMap[string(sequenceKey)]
}

// write writes values to the entry in the ring's partition associated with key.
// If no entry exists for the key then one will be created.
// write is safe for use by multiple goroutines.
func (r *ring) write(key []byte, values Values) (bool, error) {
	return r.getPartition(key).write(key, values)
}

// key是组key
func (r *ringForGroup) writeForGroup(groupKey []byte, sequenceKey []byte, values MultiValuesList) (bool, bool, error) {
	return r.getPartition(groupKey).write(groupKey, sequenceKey, values)
}

// add adds an entry to the ring.
func (r *ring) add(key []byte, entry *entry) {
	r.getPartition(key).add(key, entry)
	atomic.AddInt64(&r.keysHint, 1)
}

func (r *ringForGroup) addForGroup(groupKey []byte, sequenceKey []byte, entry *MultiEntry) {
	r.getPartition(groupKey).add(groupKey, sequenceKey, entry)
	atomic.AddInt64(&r.keysHint, 1)
}

// remove deletes the entry for the given key.
// remove is safe for use by multiple goroutines.
func (r *ring) remove(key []byte) {
	r.getPartition(key).remove(key)
	if r.keysHint > 0 {
		atomic.AddInt64(&r.keysHint, -1)
	}
}

func (r *ringForGroup) removeForGroup(groupKey []byte, sequenceKey []byte) {
	r.getPartition(groupKey).remove(groupKey, sequenceKey)
}

// keys returns all the keys from all partitions in the hash ring. The returned
// keys will be in order if sorted is true.
func (r *ring) keys(sorted bool) [][]byte {
	keys := make([][]byte, 0, atomic.LoadInt64(&r.keysHint))
	for _, p := range r.partitions {
		keys = append(keys, p.keys()...)
	}

	if sorted {
		bytesutil.Sort(keys)
	}
	return keys
}

func (r *ringForGroup) groupKeys(sorted bool) [][]byte {
	keys := make([][]byte, 0, atomic.LoadInt64(&r.keysHint))
	for _, p := range r.partitions {
		keys = append(keys, p.groupKeys()...)
	}

	if sorted {
		bytesutil.Sort(keys)
	}
	return keys
}

func (r *ring) count() int {
	var n int
	for _, p := range r.partitions {
		n += p.count()
	}
	return n
}

func (r *ringForGroup) countForGroup() int {
	var n int
	for _, p := range r.partitions {
		n += p.countForGroup()
	}
	return n
}

// apply applies the provided function to every entry in the ring under a read
// lock using a separate goroutine for each partition. The provided function
// will be called with each key and the corresponding entry. The first error
// encountered will be returned, if any. apply is safe for use by multiple
// goroutines.
func (r *ring) apply(f func([]byte, *entry) error) error {

	var (
		wg  sync.WaitGroup
		res = make(chan error, len(r.partitions))
	)

	for _, p := range r.partitions {
		wg.Add(1)

		go func(p *partition) {
			defer wg.Done()

			p.mu.RLock()
			for k, e := range p.store {
				if err := f([]byte(k), e); err != nil {
					res <- err
					p.mu.RUnlock()
					return
				}
			}
			p.mu.RUnlock()
		}(p)
	}

	go func() {
		wg.Wait()
		close(res)
	}()

	// Collect results.
	for err := range res {
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ringForGroup) applyForGroup(f func([]byte, []byte, *MultiEntry) error) error {

	var (
		wg  sync.WaitGroup
		res = make(chan error, len(r.partitions))
	)

	for _, p := range r.partitions {
		wg.Add(1)

		go func(p *partitionForGroup) {
			defer wg.Done()

			p.mu.RLock()

			for groupKey, seqMap := range p.store {
				for sequenceKey, multiEntry := range seqMap {
					if err := f([]byte(groupKey), []byte(sequenceKey), multiEntry); err != nil {
						res <- err
						p.mu.RUnlock()
						return
					}
				}
			}

			p.mu.RUnlock()
		}(p)
	}
	go func() {
		wg.Wait()
		close(res)
	}()

	// Collect results.
	for err := range res {
		if err != nil {
			return err
		}
	}
	return nil
}

// applySerial is similar to apply, but invokes f on each partition in the same
// goroutine.
// apply is safe for use by multiple goroutines.
func (r *ring) applySerial(f func([]byte, *entry) error) error {
	for _, p := range r.partitions {
		p.mu.RLock()
		for k, e := range p.store {
			if e.count() == 0 {
				continue
			}
			if err := f([]byte(k), e); err != nil {
				p.mu.RUnlock()
				return err
			}
		}
		p.mu.RUnlock()
	}
	return nil
}

func (r *ring) split(n int) []storer {
	var keys int
	storers := make([]storer, n)
	for i := 0; i < n; i++ {
		storers[i], _ = newring(len(r.partitions)) // 和当前ring的结构是一样的
	}

	for i, p := range r.partitions {
		r := storers[i%n].(*ring) // hash 获取某个storer（也就是ring）
		r.partitions[i] = p       // 将原先Cache中的分区散列到多个ring中，最终结果就是新生成的每个ring持有若干个partition
		keys += len(p.store)
	}
	return storers
}

func (r *ringForGroup) splitForGroup(n int) []storerForGroup {
	var keys int
	storers := make([]storerForGroup, n)
	for i := 0; i < n; i++ {
		storers[i], _ = newRingForGroup(len(r.partitions)) // 和当前ring的结构是一样的
	}

	for i, p := range r.partitions {
		r := storers[i%n].(*ringForGroup) // hash 获取某个storer（也就是ring）
		r.partitions[i] = p               // 将原先Cache中的分区散列到多个ring中，最终结果就是新生成的每个ring持有若干个partition
		keys += len(p.store)
	}
	return storers
}

// partition provides safe access to a map of series keys to entries.
type partition struct {
	mu    sync.RWMutex
	store map[string]*entry
}

type partitionForGroup struct {
	mu    sync.RWMutex
	store map[string]map[string]*MultiEntry
}

// 计算partition的内存占用
func (p *partition) memSizeAndCount() (keyMemSize uint64, valueMemSize uint64, keyCount uint64, valueCount uint64) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	keyMemSize = 0
	valueMemSize = 0
	keyCount = uint64(len(p.store))
	valueCount = 0
	for key, value := range p.store {
		keyMemSize += uint64(len(key)) // 估算 entry 的内存占用
		valueMemSize += uint64(value.size())
		valueCount += uint64(value.values.Len())
	}
	return
}

func (p *partitionForGroup) memSizeAndCount() (keyMemSize uint64, valueMemSize uint64, gKeyMemSize uint64, sKeyMemSize uint64,
	groupKeyCount uint64, sequenceKeyCount uint64, valueCount uint64) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	keyMemSize = 0
	valueMemSize = 0
	groupKeyCount = uint64(len(p.store))
	sequenceKeyCount = 0
	valueCount = 0

	for groupKey, seqMap := range p.store {
		gKeyMemSize += uint64(len(groupKey))
		for sequenceKey, vs := range seqMap {
			sKeyMemSize += uint64(len(sequenceKey))
			sequenceKeyCount += 1
			valueMemSize += uint64(vs.values.Size())
			valueCount += uint64(len(vs.values) * vs.values[0].Width)
		}
	}
	keyMemSize += gKeyMemSize
	keyMemSize += sKeyMemSize
	return
}

// entry returns the partition's entry for the provided key.
// It's safe for use by multiple goroutines.
func (p *partition) entry(key []byte) *entry {
	p.mu.RLock()
	e := p.store[string(key)]
	p.mu.RUnlock()
	return e
}

// 通过组key返回组下的序列key
func (p *partitionForGroup) entry(key []byte) map[string]*MultiEntry {
	p.mu.RLock()
	e := p.store[string(key)]
	p.mu.RUnlock()
	return e
}

// write writes the values to the entry in the partition, creating the entry
// if it does not exist.
// write is safe for use by multiple goroutines.
func (p *partition) write(key []byte, values Values) (bool, error) {
	p.mu.RLock()
	e := p.store[string(key)]
	p.mu.RUnlock()
	if e != nil {
		// Hot path.
		return false, e.add(values)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check again.
	if e = p.store[string(key)]; e != nil {
		return false, e.add(values)
	}

	// Create a new entry using a preallocated size if we have a hint available.
	e, err := newEntryValues(values)
	if err != nil {
		return false, err
	}

	p.store[string(key)] = e
	return true, nil
}

// newGroupKey newSequenceKey error
func (p *partitionForGroup) write(groupKey []byte, sequenceKey []byte, values MultiValuesList) (bool, bool, error) {
	p.mu.RLock()
	seqMap, exists := p.store[string(groupKey)] // 获得序列map
	p.mu.RUnlock()

	newGroupKey := false
	if !exists {
		p.mu.Lock()
		if seqMap, exists = p.store[string(groupKey)]; !exists {
			p.store[string(groupKey)] = make(map[string]*MultiEntry)
			seqMap = p.store[string(groupKey)]
			newGroupKey = true
		}
		p.mu.Unlock()
	}

	p.mu.RLock()
	multiEntry, exists := seqMap[string(sequenceKey)]
	p.mu.RUnlock()
	if exists {
		return newGroupKey, false, multiEntry.add(values)
	}

	// 不存在则创建
	// Create a new entry using a preallocated size if we have a hint available.
	e, err := newMultiEntryValues(values)
	if err != nil {
		return false, false, err
	}

	p.mu.Lock()
	seqMap[string(sequenceKey)] = e
	p.mu.Unlock()
	return newGroupKey, true, nil
}

// add adds a new entry for key to the partition.
func (p *partition) add(key []byte, entry *entry) {
	p.mu.Lock()
	p.store[string(key)] = entry
	p.mu.Unlock()
}

func (p *partitionForGroup) add(groupKey []byte, sequenceKey []byte, entry *MultiEntry) {
	p.mu.Lock()
	if p.store[string(groupKey)] == nil {
		p.store[string(groupKey)] = make(map[string]*MultiEntry)
	}
	p.store[string(groupKey)][string(sequenceKey)] = entry
	p.mu.Unlock()
}

// remove deletes the entry associated with the provided key.
// remove is safe for use by multiple goroutines.
func (p *partition) remove(key []byte) {
	p.mu.Lock()
	delete(p.store, string(key))
	p.mu.Unlock()
}

func (p *partitionForGroup) remove(groupKey []byte, sequenceKey []byte) {
	p.mu.Lock()
	if p.store[string(groupKey)] != nil {
		delete(p.store[string(groupKey)], string(sequenceKey))
	}
	if len(p.store[string(groupKey)]) == 0 {
		delete(p.store, string(groupKey))
	}
	p.mu.Unlock()
}

// keys returns an unsorted slice of the keys in the partition.
func (p *partition) keys() [][]byte {
	p.mu.RLock()
	keys := make([][]byte, 0, len(p.store))
	for k, v := range p.store {
		if v.count() == 0 {
			continue
		}
		keys = append(keys, []byte(k))
	}
	p.mu.RUnlock()
	return keys
}

func (p *partitionForGroup) groupKeys() [][]byte {
	p.mu.RLock()
	keys := make([][]byte, 0, len(p.store))
	for k, v := range p.store {
		if len(v) == 0 { // 没有对应的序列map
			continue
		}
		keys = append(keys, []byte(k))
	}
	p.mu.RUnlock()
	return keys
}

// reset resets the partition by reinitialising the store. reset returns hints
// about sizes that the entries within the store could be reallocated with.
func (p *partition) reset() {
	p.mu.RLock()
	sz := len(p.store)
	p.mu.RUnlock()

	newStore := make(map[string]*entry, sz)
	p.mu.Lock()
	p.store = newStore
	p.mu.Unlock()
}

func (p *partitionForGroup) resetForGroup() {
	p.mu.RLock()
	sz := len(p.store)
	p.mu.RUnlock()

	newStore := make(map[string]map[string]*MultiEntry, sz)
	p.mu.Lock()
	p.store = newStore
	p.mu.Unlock()
}

func (p *partition) count() int {
	var n int
	p.mu.RLock()
	for _, v := range p.store {
		if v.count() > 0 {
			n++
		}
	}
	p.mu.RUnlock()
	return n
}

func (p *partitionForGroup) countForGroup() int {
	var n int
	p.mu.RLock()
	for _, seqMap := range p.store {
		n += len(seqMap)
	}
	p.mu.RUnlock()
	return n
}

func (p *partition) size() int {
	var n int
	p.mu.RLock()
	for _, v := range p.store {
		if v.count() > 0 {
			n++
		}
	}
	p.mu.RUnlock()
	return n
}
