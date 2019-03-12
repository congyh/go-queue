// esQueue
package queue

import (
	"fmt"
	"runtime"
	"sync/atomic"
)

type esCache struct {
	putNo uint32
	getNo uint32
	value interface{} // Note: 实际的value存储的地方
}

// lock free queue
type EsQueue struct {
	capaciity uint32
	capMod    uint32
	putPos    uint32
	getPos    uint32
	cache     []esCache // Note: 队列底层实际上还是使用的数组作为存储
}

func NewQueue(capaciity uint32) *EsQueue {
	// Note: 生成一个指针变量, struct内部每个field都会被初始成零值.
	q := new(EsQueue)
	// Note: 向大于等于的最接近的2的幂次靠近, 目的是使用位操作, 效率高
	// 例如: 2->2, 5->8
	q.capaciity = minQuantity(capaciity)
	q.capMod = q.capaciity - 1
	q.putPos = 0
	q.getPos = 0
	q.cache = make([]esCache, q.capaciity)
	for i := range q.cache {
		cache := &q.cache[i]
		cache.getNo = uint32(i)
		cache.putNo = uint32(i)
	}
	cache := &q.cache[0]
	// Note: 对于数组第一个元素, getNo和putNo设定为数组容量
	// TODO: 这里如何理解
	cache.getNo = q.capaciity
	cache.putNo = q.capaciity
	return q
}

func (q *EsQueue) String() string {
	getPos := atomic.LoadUint32(&q.getPos)
	putPos := atomic.LoadUint32(&q.putPos)
	return fmt.Sprintf("Queue{capaciity: %v, capMod: %v, putPos: %v, getPos: %v}",
		q.capaciity, q.capMod, putPos, getPos)
}

func (q *EsQueue) Capaciity() uint32 {
	return q.capaciity
}

// Note: 返回的是队列中目前存量
func (q *EsQueue) Quantity() uint32 {
	var putPos, getPos uint32
	var quantity uint32
	getPos = atomic.LoadUint32(&q.getPos)
	putPos = atomic.LoadUint32(&q.putPos)

	if putPos >= getPos { // Note: 如果put的坐标在前面, 直接减get的就是队列目前存量
		quantity = putPos - getPos
	} else { // Note: 如果get在前面, 说明put已经重新从头循环了.
		// 例如: capMod = 7, putPos = 3, getPos = 5, 那么67012都是还没有消费的
		// 所以3 + 7 - 5
		// 即quantity = putPos + (q.capMod - getPos)实际上更容易理解
		quantity = q.capMod + (putPos - getPos)
	}

	return quantity
}

// put queue functions
func (q *EsQueue) Put(val interface{}) (ok bool, quantity uint32) {
	// Note: 变量声明区
	var putPos, putPosNew, getPos, posCnt uint32
	var cache *esCache
	capMod := q.capMod

	// Note: 两者初始值都是0
	getPos = atomic.LoadUint32(&q.getPos)
	putPos = atomic.LoadUint32(&q.putPos)

	// posCnt是待消费的长度
	if putPos >= getPos {
		posCnt = putPos - getPos
	} else {
		posCnt = capMod + (putPos - getPos)
	}

	// Note: 如果待消费的长度已经是数组长度了
	// 也就是说putPos = getPos了, 那么就说明队列满了.
	// 返回前先主动交出控制权, 让其他goroutine运行, 避免又连续拿到时间片做无用功.
	// TODO: 个人感觉这里应该是posCnt >= capMod
	// TODO: 想清楚为啥. https://github.com/yireyun/go-queue/issues/6
	if posCnt >= capMod-1 {
		runtime.Gosched()
		return false, posCnt
	}
	// Note: 如果队列没有满, 则尝试进行put操作
	// 尝试拿到队列的下一个写入位
	putPosNew = putPos + 1
	// Note: 如果没有成功拿到, 那么让出时间片的理由与前者相同
	if !atomic.CompareAndSwapUint32(&q.putPos, putPos, putPosNew) {
		runtime.Gosched()
		return false, posCnt
	}

	// Note: 如果拿到了写入位, 那么就可以拿到实际的存储结构, 尝试进行写入了
	cache = &q.cache[putPosNew&capMod]

	for {
		getNo := atomic.LoadUint32(&cache.getNo)
		putNo := atomic.LoadUint32(&cache.putNo)
		// Note: 这里以第一遍写入为例, putPosNew = 1, 所以是满足的
		if putPosNew == putNo && getNo == putNo {
			cache.value = val
			// Note: 1 + 2 = 3, putNo变为了3
			atomic.AddUint32(&cache.putNo, q.capaciity)
			// Note: 队列存量+1
			return true, posCnt + 1
		} else {
			// Note: 当要写的还没有被上一轮的读完的时候, 会走到这里.
			runtime.Gosched()
		}
	}
}

// puts queue functions
func (q *EsQueue) Puts(values []interface{}) (puts, quantity uint32) {
	var putPos, putPosNew, getPos, posCnt, putCnt uint32
	capMod := q.capMod

	getPos = atomic.LoadUint32(&q.getPos)
	putPos = atomic.LoadUint32(&q.putPos)

	if putPos >= getPos {
		posCnt = putPos - getPos
	} else {
		posCnt = capMod + (putPos - getPos)
	}

	if posCnt >= capMod-1 {
		runtime.Gosched()
		return 0, posCnt
	}

	if capPuts, size := q.capaciity-posCnt, uint32(len(values)); capPuts >= size {
		putCnt = size
	} else {
		putCnt = capPuts
	}
	putPosNew = putPos + putCnt

	if !atomic.CompareAndSwapUint32(&q.putPos, putPos, putPosNew) {
		runtime.Gosched()
		return 0, posCnt
	}

	for posNew, v := putPos+1, uint32(0); v < putCnt; posNew, v = posNew+1, v+1 {
		var cache *esCache = &q.cache[posNew&capMod]
		for {
			getNo := atomic.LoadUint32(&cache.getNo)
			putNo := atomic.LoadUint32(&cache.putNo)
			if posNew == putNo && getNo == putNo {
				cache.value = values[v]
				atomic.AddUint32(&cache.putNo, q.capaciity)
				break
			} else {
				runtime.Gosched()
			}
		}
	}
	return putCnt, posCnt + putCnt
}

// get queue functions
func (q *EsQueue) Get() (val interface{}, ok bool, quantity uint32) {
	var putPos, getPos, getPosNew, posCnt uint32
	var cache *esCache
	capMod := q.capMod

	putPos = atomic.LoadUint32(&q.putPos)
	getPos = atomic.LoadUint32(&q.getPos)

	if putPos >= getPos {
		posCnt = putPos - getPos
	} else {
		posCnt = capMod + (putPos - getPos)
	}

	// Note: 如果队列存量为0的话, 说明没有需要消费的
	if posCnt < 1 {
		// Note: 主动交出控制权, 最好的结果是让Producer开始放东西.
		runtime.Gosched()
		return nil, false, posCnt
	}

	getPosNew = getPos + 1
	// Note: 如果没有拿到读取权限的话
	if !atomic.CompareAndSwapUint32(&q.getPos, getPos, getPosNew) {
		runtime.Gosched()
		return nil, false, posCnt
	}
	// Note: 拿到了读取权限1&capMod最后一位总归是1, 所以是1
	cache = &q.cache[getPosNew&capMod]

	for {
		getNo := atomic.LoadUint32(&cache.getNo)
		putNo := atomic.LoadUint32(&cache.putNo)
		// Note: 以第一次读取为例, 1 == 1, 1 == (3 - 2)
		// 第二个式子成立说明这个位置已经被写过了, 可以读取了.
		if getPosNew == getNo && getNo == putNo-q.capaciity {
			val = cache.value
			// Note: 读取完成后手动置为nil, 是为了保险起见
			// 防止把指针留下了, 引起祸患
			cache.value = nil
			// Note: 前移读指针, 与写指针保持一致
			atomic.AddUint32(&cache.getNo, q.capaciity)
			// Note: 队列存量-1, 标识消费成功.
			return val, true, posCnt - 1
		} else {
			// Note: 当要读的时候, 上一轮还没有写完的时候, 会到这里.
			runtime.Gosched()
		}
	}
}

// gets queue functions
func (q *EsQueue) Gets(values []interface{}) (gets, quantity uint32) {
	var putPos, getPos, getPosNew, posCnt, getCnt uint32
	capMod := q.capMod

	putPos = atomic.LoadUint32(&q.putPos)
	getPos = atomic.LoadUint32(&q.getPos)

	if putPos >= getPos {
		posCnt = putPos - getPos
	} else {
		posCnt = capMod + (putPos - getPos)
	}

	if posCnt < 1 {
		runtime.Gosched()
		return 0, posCnt
	}

	if size := uint32(len(values)); posCnt >= size {
		getCnt = size
	} else {
		getCnt = posCnt
	}
	getPosNew = getPos + getCnt

	if !atomic.CompareAndSwapUint32(&q.getPos, getPos, getPosNew) {
		runtime.Gosched()
		return 0, posCnt
	}

	for posNew, v := getPos+1, uint32(0); v < getCnt; posNew, v = posNew+1, v+1 {
		var cache *esCache = &q.cache[posNew&capMod]
		for {
			getNo := atomic.LoadUint32(&cache.getNo)
			putNo := atomic.LoadUint32(&cache.putNo)
			if posNew == getNo && getNo == putNo-q.capaciity {
				values[v] = cache.value
				cache.value = nil
				getNo = atomic.AddUint32(&cache.getNo, q.capaciity)
				break
			} else {
				runtime.Gosched()
			}
		}
	}

	return getCnt, posCnt - getCnt
}

// round 到最近的2的倍数
func minQuantity(v uint32) uint32 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}

func Delay(z int) {
	for x := z; x > 0; x-- {
	}
}
