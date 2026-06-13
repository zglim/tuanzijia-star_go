package starGo

import (
	"sync"
	"time"
)

// connCloser 抽象底层连接的关闭能力，使 TCP 与 WebSocket 客户端可以共用同一套
// 连接关闭收口逻辑。net.Conn 与 *websocket.Conn 均满足该接口。
type connCloser interface {
	Close() error
}

// baseClient 收敛 TCP Client 与 WebSocketClient 之间重复的连接状态管理与发送队列
// 处理逻辑：停止状态、活跃时间、发送通道，以及连接关闭收口都集中在这里维护，两个
// 客户端通过内嵌该结构复用这些能力，避免两套实现继续分叉。
type baseClient struct {
	closer     connCloser
	remoteAddr string
	stop       bool
	activeTime int64
	sendCh     chan []byte
	mutex      sync.RWMutex
}

func newBaseClient(closer connCloser, remoteAddr string) baseClient {
	return baseClient{
		closer:     closer,
		remoteAddr: remoteAddr,
		stop:       false,
		activeTime: time.Now().Unix(),
		sendCh:     make(chan []byte, 1024),
	}
}

func (c *baseClient) GetStop() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.stop
}

func (c *baseClient) SetStop() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.stop = true
}

func (c *baseClient) GetActiveTime() int64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.activeTime
}

func (c *baseClient) SetActiveTime() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.activeTime = time.Now().Unix()
}

func (c *baseClient) AppendSendQueue(message []byte) {
	c.sendCh <- message
}

// shutdown 是连接关闭与停止状态更新的唯一收口：无论是读协程退出、发送失败，还是
// 过期清理与系统退出，都通过它来同时置位 stop 并关闭底层连接，避免在某处只改了其中
// 一步而漏掉另一步。重复调用是安全的。
func (c *baseClient) shutdown() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.stop {
		return
	}
	c.stop = true
	if c.closer != nil {
		_ = c.closer.Close()
	}
}

// runReceiveLoop 提供读协程的统一骨架：循环调用 readOne 读取一帧数据，再交给 dispatch
// 处理，直到 stop 被置位或 readOne 报告无法继续读取；协程退出时一定通过 shutdown 收口
// 连接。具体协议如何读取、如何解析交给调用方注入，从而让读协程的职责边界保持清晰。
func (c *baseClient) runReceiveLoop(readOne func() (payload []byte, ok bool), dispatch func(payload []byte)) {
	defer c.shutdown()
	for !c.GetStop() {
		payload, ok := readOne()
		if !ok {
			break
		}
		data := payload
		Go2(func() {
			dispatch(data)
		})
	}
}

// runSendLoop 提供写协程的统一骨架：从发送队列取出消息并交给 writeOne 实际写出，任意
// 一次写出失败都通过 shutdown 收口连接，使发送失败与连接关闭、停止状态保持一致。具体
// 协议如何封帧与写出交给调用方注入。
func (c *baseClient) runSendLoop(stop chan struct{}, writeOne func(message []byte) error) {
	for !c.GetStop() {
		select {
		case message := <-c.sendCh:
			msg := message
			Go2(func() {
				if err := writeOne(msg); err != nil {
					ErrorLog("向客户端:%v发送数据出错,错误信息:%v", c.remoteAddr, err)
					c.shutdown()
				}
			})
		case <-stop:
			return
		}
	}
}
