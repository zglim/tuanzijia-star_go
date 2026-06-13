package starGo

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

// ============================================================
// systemLifecycle 集中管理系统级生命周期状态：
//   - 停止信号广播（allForStopSignal / logForStopSignal）
//   - 停止通道（stopChanForGo / stopChanForLog）
//   - 协程同步（waitAllGroup / waitLogGroup）
//   - 退出回调 / 重载回调 / 资源关闭回调
//
// 所有字段通过唯一实例 lifecycle 访问，避免全局变量散乱操作。
// ============================================================
type systemLifecycle struct {
	mu sync.Mutex

	// ---------- 停止信号 ----------
	// 使用 atomic 读写，避免在热循环中加锁
	allForStopSignal int32 // 通用停止信号：0=运行中，1=已停止
	logForStopSignal int32 // 日志停止信号：0=运行中，1=已停止

	// ---------- 停止通道 ----------
	stopChanForGo  chan struct{} // 通知业务协程退出
	stopChanForLog chan struct{} // 通知日志协程退出

	// ---------- 协程同步 ----------
	waitAllGroup sync.WaitGroup // 业务协程计数
	waitLogGroup sync.WaitGroup // 日志协程计数

	// ---------- 回调注册 ----------
	exitFuncs       []func() // 退出时回调（先于资源关闭执行）
	reloadFuncs     []func() // 重载时回调
	resourceClosers []func() // 资源关闭回调（在退出回调之后、停止信号之前执行）
}

// lifecycle 是进程唯一的生命周期管理实例。
var lifecycle = &systemLifecycle{
	stopChanForGo:  make(chan struct{}),
	stopChanForLog: make(chan struct{}),
}

// ============================================================
// 公开 API（保持原有签名稳定）
// ============================================================

// WaitForSystemExit 阻塞等待系统退出信号（SIGINT / SIGKILL / SIGTERM），
// 然后按顺序执行：退出回调 → 资源关闭 → 停止信号广播 → 等待协程退出。
func WaitForSystemExit() {
	sign := make(chan os.Signal, 1)
	signal.Notify(sign, os.Interrupt, os.Kill, syscall.SIGTERM)
	<-sign

	InfoLog("收到退出信号")
	lifecycle.shutdown()

	lifecycle.waitAllGroup.Wait()

	if !atomic.CompareAndSwapInt32(&lifecycle.logForStopSignal, 0, 1) {
		return
	}
	close(lifecycle.stopChanForLog)
	lifecycle.waitLogGroup.Wait()
	fmt.Println("服务器已关闭")
}

// RegisterSystemExitFunc 注册退出时回调，按注册顺序执行。
func RegisterSystemExitFunc(f func()) {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	lifecycle.exitFuncs = append(lifecycle.exitFuncs, f)
}

// RegisterSystemReloadFunc 注册重载时回调，按注册顺序执行。
func RegisterSystemReloadFunc(f func()) {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	lifecycle.reloadFuncs = append(lifecycle.reloadFuncs, f)
}

// RegisterResourceCloser 注册资源关闭回调。
// 所有需要随系统退出而释放的资源（数据库、缓存、连接池等）统一在此注册，
// 避免不同资源类型的清理逻辑散落在 systemExit 中无序增长。
func RegisterResourceCloser(f func()) {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	lifecycle.resourceClosers = append(lifecycle.resourceClosers, f)
}

// Daemon 将当前进程以后台方式重新启动（fork 自身）。
// skip 参数中指定的命令行参数（或其前缀）会在重启时被跳过。
func Daemon(skip ...string) {
	if os.Getppid() != 1 {
		filePath, _ := filepath.Abs(os.Args[0])
		newCmd := []string{os.Args[0]}
		add := 0
		for _, v := range os.Args[1:] {
			if add == 1 {
				add = 0
				continue
			} else {
				add = 0
			}
			for _, s := range skip {
				if strings.Contains(v, s) {
					if strings.Contains(v, "--") {
						add = 2
					} else {
						add = 1
					}
					break
				}
			}
			if add == 0 {
				newCmd = append(newCmd, v)
			}
		}
		InfoLog("后台运行参数:%v", newCmd)
		cmd := exec.Command(filePath)
		cmd.Args = newCmd
		_ = cmd.Start()
	}
}

// ============================================================
// 内部方法
// ============================================================

// shutdown 按顺序执行完整的系统退出流程：
//  1. 执行退出回调（业务自定义清理）
//  2. 执行资源关闭回调（数据库、缓存、连接等统一收口）
//  3. 广播停止信号（通知所有协程退出）
//  4. 关闭内置连接资源（tcp/udp/ws/mysql/redis，作为兜底）
func (lc *systemLifecycle) shutdown() {
	lc.runExitCallbacks()
	lc.closeResources()
	lc.broadcastStop()
	lc.closeBuiltinConnections()
}

// runExitCallbacks 执行业务注册的退出回调。
func (lc *systemLifecycle) runExitCallbacks() {
	InfoLog("调用退出时方法")
	lc.mu.Lock()
	fns := make([]func(), len(lc.exitFuncs))
	copy(fns, lc.exitFuncs)
	lc.mu.Unlock()

	for _, f := range fns {
		f()
	}
}

// closeResources 执行所有注册的资源关闭回调（统一收口）。
func (lc *systemLifecycle) closeResources() {
	InfoLog("关闭已注册资源")
	lc.mu.Lock()
	fns := make([]func(), len(lc.resourceClosers))
	copy(fns, lc.resourceClosers)
	lc.mu.Unlock()

	for _, f := range fns {
		f()
	}
}

// broadcastStop 广播停止信号，通知所有业务协程退出。
// 使用 atomic CAS 保证只执行一次。
func (lc *systemLifecycle) broadcastStop() {
	InfoLog("更新停止信号")
	if !atomic.CompareAndSwapInt32(&lc.allForStopSignal, 0, 1) {
		return
	}
	close(lc.stopChanForGo)
}

// closeBuiltinConnections 关闭框架内置的连接资源（兜底清理）。
// 如果已通过 RegisterResourceCloser 注册，则此处为幂等操作。
func (lc *systemLifecycle) closeBuiltinConnections() {
	InfoLog("关闭所有连接")

	// TCP 客户端
	tcpClientMap.Range(func(key, value interface{}) bool {
		client := value.(*Client)
		client.SetStop()
		_ = client.GetConn().Close()
		tcpClientMap.Delete(key)
		return true
	})

	// UDP 客户端
	udpClientMap.Range(func(key, value interface{}) bool {
		udpClientMap.Delete(key)
		return true
	})

	// WebSocket 客户端
	wsClientMap.Range(func(key, value interface{}) bool {
		client := value.(*WebSocketClient)
		client.SetStop()
		_ = client.GetConn().Close()
		wsClientMap.Delete(key)
		return true
	})

	// MySQL / Redis
	InfoLog("关闭mysql和redis连接")
	if mysqlCfg != nil {
		_ = mysqlCfg.GetDb().Close()
	}
	if redisCfg != nil {
		_ = redisCfg.GetConnection().Close()
	}

	InfoLog("系统退出方法调用完成")
}

// reload 执行所有注册的重载回调。
func (lc *systemLifecycle) reload() {
	lc.mu.Lock()
	fns := make([]func(), len(lc.reloadFuncs))
	copy(fns, lc.reloadFuncs)
	lc.mu.Unlock()

	for _, f := range fns {
		f()
	}
}

// ============================================================
// 包级便捷函数（供框架内部其他模块使用）
// ============================================================

// systemReload 触发系统重载（保持原有入口）。
func systemReload() {
	lifecycle.reload()
}
