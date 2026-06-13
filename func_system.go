package starGo

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
)

// 本文件集中管理系统生命周期：退出、重载、停止信号广播与资源回收。
// 设计上分为三层：
//  1. 对外注册入口（RegisterSystemExitFunc / RegisterSystemReloadFunc 等）。
//  2. 生命周期主流程（WaitForSystemExit / systemExit / systemReload）。
//  3. 统一收口的底层能力（broadcastStop 广播停止信号、closeResources 回收资源）。
// 业务回调与资源清理都通过登记表收口，新增系统级事件或资源类型时只需登记，
// 无需改动主流程，便于作为框架级基础设施持续扩展。

// systemExitFunc 与 systemReloadFunc 保存业务注册的退出 / 重载回调，
// 属于系统层私有状态，仅由本文件内的注册与触发逻辑访问。
var (
	systemExitFunc   []func()
	systemReloadFunc []func()
)

// resourceCloseFunc 是资源回收的统一收口点：所有需要在退出时释放的资源都以
// 回调形式登记于此，退出时按登记顺序依次执行。新增资源类型只需通过
// registerResourceCloseFunc 登记，而不必再修改退出主流程，避免清理逻辑无序膨胀。
var resourceCloseFunc []func()

func init() {
	// 框架内置资源的回收逻辑集中登记到统一收口点，登记顺序即为关闭顺序：
	// 先断开网络连接，再关闭底层存储连接。
	registerResourceCloseFunc(closeNetworkClients)
	registerResourceCloseFunc(closeStorageConnections)
}

// RegisterSystemExitFunc 注册一个在系统退出时调用的回调。
func RegisterSystemExitFunc(f func()) {
	systemExitFunc = append(systemExitFunc, f)
}

// RegisterSystemReloadFunc 注册一个在系统重载时调用的回调。
func RegisterSystemReloadFunc(f func()) {
	systemReloadFunc = append(systemReloadFunc, f)
}

// registerResourceCloseFunc 向资源回收收口点登记一个清理回调。
func registerResourceCloseFunc(f func()) {
	resourceCloseFunc = append(resourceCloseFunc, f)
}

// WaitForSystemExit 阻塞等待退出信号，收到后驱动整个退出流程：
// 触发业务退出回调与资源回收 -> 等待业务协程结束 -> 广播日志停止信号并等待日志协程收尾。
func WaitForSystemExit() {
	sign := make(chan os.Signal, 1)
	signal.Notify(sign, os.Interrupt, os.Kill, syscall.SIGTERM)
	<-sign
	InfoLog("收到退出信号")

	systemExit()

	// 业务协程全部退出后，再广播日志停止信号，确保退出过程中的日志不会丢失。
	waitAllGroup.Wait()
	if !broadcastStop(&logForStopSignal, stopChanForLog) {
		return
	}
	waitLogGroup.Wait()
	fmt.Println("服务器已关闭")
}

// systemExit 执行退出主流程：先调用业务退出回调，再广播业务协程停止信号并完成
// 资源回收。停止广播与资源回收只会执行一次，重复调用是安全的。
func systemExit() {
	InfoLog("调用退出时方法")
	for _, f := range systemExitFunc {
		f()
	}

	InfoLog("更新停止信号")
	if !broadcastStop(&allForStopSignal, stopChanForGo) {
		return
	}

	closeResources()
	InfoLog("系统退出方法调用完成")
}

// systemReload 调用所有已注册的重载回调。
func systemReload() {
	for _, f := range systemReloadFunc {
		f()
	}
}

// broadcastStop 以原子方式将停止标志由 0 翻转为 1，并在首次翻转时关闭对应的停止
// 通道，从而向所有监听者广播停止信号。返回值表示本次调用是否为首次触发，
// 调用方可据此保证关联的清理逻辑只执行一次。
func broadcastStop(flag *int32, ch chan struct{}) bool {
	if !atomic.CompareAndSwapInt32(flag, 0, 1) {
		return false
	}
	close(ch)
	return true
}

// closeResources 依次执行所有已登记的资源回收回调，是资源释放的唯一出口。
func closeResources() {
	InfoLog("关闭所有连接")
	for _, f := range resourceCloseFunc {
		f()
	}
}

// closeNetworkClients 关闭并清理全部网络连接（TCP / UDP / WebSocket）。
func closeNetworkClients() {
	// 关闭所有tcp连接
	tcpClientMap.Range(func(key, value interface{}) bool {
		client := value.(*Client)
		client.SetStop()
		client.GetConn().Close()
		tcpClientMap.Delete(key)
		return true
	})

	// 关闭所有udp连接
	udpClientMap.Range(func(key, value interface{}) bool {
		udpClientMap.Delete(key)
		return true
	})

	// 关闭所有webSocket连接
	wsClientMap.Range(func(key, value interface{}) bool {
		client := value.(*WebSocketClient)
		client.SetStop()
		_ = client.GetConn().Close()
		wsClientMap.Delete(key)
		return true
	})
}

// closeStorageConnections 关闭 mysql 与 redis 连接。
func closeStorageConnections() {
	InfoLog("关闭mysql和redis连接")
	if mysqlCfg != nil {
		_ = mysqlCfg.GetDb().Close()
	}
	if redisCfg != nil {
		_ = redisCfg.GetConnection().Close()
	}
}

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
