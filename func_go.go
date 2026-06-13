package starGo

import (
	"fmt"
	"sync/atomic"
)

// Try 执行函数f，如果发生panic则调用handler处理。
// 如果handler为nil，则打印调用栈和错误日志。
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

// launchGoroutine 统一的协程启动入口，集中处理计数、WaitGroup、panic兜底和启动/结束日志。
//   - callerSkip: 传给 callerInfo 的调用栈偏移量，用于记录真正的调用来源
//   - id: 协程编号，>0 时在日志中输出；为 0 时省略编号
//   - f: 协程实际执行的函数
func launchGoroutine(callerSkip int, id uint64, f func()) {
	waitAllGroup.Add(1)
	c := atomic.AddInt32(&goCount, 1)
	debugStr := callerInfo(callerSkip)

	if id > 0 {
		InfoLog("新开协程 id:%d 当前协程数量:%d 来自:%s", id, c, debugStr)
	} else {
		InfoLog("新开协程 当前协程数量:%d 来自:%s", c, debugStr)
	}

	go func() {
		Try(f, nil)
		waitAllGroup.Done()
		c = atomic.AddInt32(&goCount, -1)
		if id > 0 {
			InfoLog("协程运行结束 id:%d 当前协程数量:%d 来自:%s", id, c, debugStr)
		} else {
			InfoLog("协程运行结束 当前协程数量:%d 来自:%s", c, debugStr)
		}
	}()
}

// Go 启动一个带停止信号通道的协程，协程会获得一个全局唯一的编号并记录在日志中。
func Go(f func(Stop chan struct{})) {
	id := atomic.AddUint64(&goId, 1)
	// callerSkip=2: callerInfo -> launchGoroutine -> Go -> Go的调用者
	launchGoroutine(2, id, func() { f(stopChanForGo) })
}

// Go2 启动一个无参数的简单协程。
func Go2(f func()) {
	// callerSkip=2: callerInfo -> launchGoroutine -> Go2 -> Go2的调用者
	launchGoroutine(2, 0, f)
}

// goForLog 启动日志系统专用协程。
// 使用独立的 WaitGroup 和 panic 处理，避免与日志系统自身产生死循环。
func goForLog(f func(Stop chan struct{})) {
	defer func() {
		if err := recover(); err != nil {
			// 只打印异常，避免死循环（日志协程内不能再调用日志函数）
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
