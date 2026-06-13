package starGo

import (
	"fmt"
	"sync/atomic"
)

// Try 执行 f，并在发生 panic 时进行兜底。
// handler 为空时记录调用栈与错误日志，否则交由 handler 自行处理。
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

// goManaged 启动一个纳入统计的协程，集中处理协程计数与退出收尾。
// onStart 在计数自增后调用，onEnd 在协程结束、计数自减后调用，
// 两者的入参均为变更后的当前协程数量，仅负责对应时机的日志输出。
func goManaged(fn func(), onStart, onEnd func(count int32)) {
	waitAllGroup.Add(1)
	onStart(atomic.AddInt32(&goCount, 1))

	go func() {
		defer waitAllGroup.Done()
		Try(fn, nil)
		onEnd(atomic.AddInt32(&goCount, -1))
	}()
}

// Go 启动一个受统一停止信号控制的协程，并为其分配自增 id。
func Go(f func(Stop chan struct{})) {
	id := atomic.AddUint64(&goId, 1)
	source := SimpleTack()
	goManaged(
		func() { f(stopChanForGo) },
		func(c int32) { InfoLog("新开协程 id:%d 当前协程数量:%d 来自:%s", id, c, source) },
		func(c int32) { InfoLog("协程运行结束 id:%d 当前协程数量:%d 来自:%s", id, c, source) },
	)
}

// Go2 启动一个不接收停止信号的协程。
func Go2(f func()) {
	source := SimpleTack()
	goManaged(
		f,
		func(c int32) { InfoLog("新开协程 当前协程数量:%d 来自:%s", c, source) },
		func(c int32) { InfoLog("协程运行结束 当前协程数量:%d 来自:%s", c, source) },
	)
}

// goForLog 专用于日志写入协程：使用独立的等待组与停止信号，
// 且 panic 时只打印而不写日志，避免日志系统自身异常引发递归。
func goForLog(f func(Stop chan struct{})) {
	defer func() {
		if err := recover(); err != nil {
			// 只打印异常，避免死循环
			fmt.Printf("捕获到日志抛出的异常:%v", err)
		}
	}()

	if logForStopSignal != 0 {
		return
	}

	waitLogGroup.Add(1)
	go func() {
		f(stopChanForLog)
		waitLogGroup.Done()
	}()
}
