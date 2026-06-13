package starGo

import (
	"fmt"
	"sync/atomic"
)

func Try(f func(), handler func(interface{})) {
	defer func() {
		if err := recover(); err != nil {
			if handler == nil {
				Stack()
				ErrorLog("错误信息:%v", err)
			} else {
				handler(err)
			}
		}
	}()
	f()
}

func Go(f func(Stop chan struct{})) {
	lifecycle.waitAllGroup.Add(1)
	id := atomic.AddUint64(&goId, 1)
	c := atomic.AddInt32(&goCount, 1)
	debugStr := SimpleTack()
	InfoLog("新开协程 id:%d 当前协程数量:%d 来自:%s", id, c, debugStr)

	go func() {
		Try(func() { f(lifecycle.stopChanForGo) }, nil)
		lifecycle.waitAllGroup.Done()
		c = atomic.AddInt32(&goCount, -1)
		InfoLog("协程运行结束 id:%d 当前协程数量:%d 来自:%s", id, c, debugStr)
	}()
}

func Go2(f func()) {
	lifecycle.waitAllGroup.Add(1)
	debugStr := SimpleTack()
	c := atomic.AddInt32(&goCount, 1)
	InfoLog("新开协程 当前协程数量:%d 来自:%s", c, debugStr)
	go func() {
		Try(func() { f() }, nil)
		lifecycle.waitAllGroup.Done()
		c = atomic.AddInt32(&goCount, -1)
		InfoLog("协程运行结束 当前协程数量:%d 来自:%s", c, debugStr)
	}()
}

func goForLog(f func(Stop chan struct{})) {
	defer func() {
		if err := recover(); err != nil {
			// 只打印异常，避免死循环
			fmt.Printf("捕获到日志抛出的异常:%v", err)
		}
	}()

	if lifecycle.logForStopSignal != 0 {
		return
	}

	lifecycle.waitLogGroup.Add(1)
	go func() {
		f(lifecycle.stopChanForLog)
		lifecycle.waitLogGroup.Done()
	}()
}
