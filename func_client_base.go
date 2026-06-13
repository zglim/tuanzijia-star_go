package starGo

import (
	"sync"
	"time"
)

// clientConnection 定义客户端连接的统一接口，用于收口连接关闭和过期清理逻辑。
// 后续扩展新协议客户端时，只需实现此接口即可复用过期清理和发送队列逻辑。
type clientConnection interface {
	GetStop() bool
	SetStop()
	GetActiveTime() int64
	Close() error
	Addr() string
}

// BaseClient 封装 TCP/WebSocket 等客户端共用的连接状态管理、发送队列和停止控制逻辑。
// 各协议客户端通过嵌入此结构体来复用这些能力，避免重复实现。
type BaseClient struct {
	stop       bool
	activeTime int64
	sendCh     chan []byte
	mutex      sync.RWMutex
}

func newBaseClient() BaseClient {
	return BaseClient{
		stop:       false,
		activeTime: time.Now().Unix(),
		sendCh:     make(chan []byte, 1024),
	}
}

func (b *BaseClient) GetStop() bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.stop
}

func (b *BaseClient) SetStop() {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.stop = true
}

func (b *BaseClient) GetActiveTime() int64 {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.activeTime
}

func (b *BaseClient) SetActiveTime() {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.activeTime = time.Now().Unix()
}

func (b *BaseClient) AppendSendQueue(message []byte) {
	b.sendCh <- message
}

// shutdownConn 统一收口连接关闭操作：先关闭底层连接，再标记停止状态。
// 保证无论从哪里触发关闭（读协程异常、写协程失败、过期清理），操作顺序一致。
func shutdownConn(conn clientConnection) {
	_ = conn.Close()
	conn.SetStop()
}

// sendLoopWithChannel 通用的发送协程循环，由具体客户端在 start() 中调用，
// 传入自己的 sendCh 和协议写入函数。
func sendLoopWithChannel(client clientConnection, sendCh chan []byte, writeFn func([]byte) error) {
	Go(func(Stop chan struct{}) {
		for !client.GetStop() {
			select {
			case message := <-sendCh:
				Go2(func() {
					if err := writeFn(message); err != nil {
						ErrorLog("向客户端:%v发送数据出错,错误信息:%v", client.Addr(), err)
						shutdownConn(client)
					}
				})
			case <-Stop:
				return
			}
		}
	})
}

// cleanExpiredClients 统一处理过期客户端的清理逻辑。
// 遍历 clientMap，找出超时的连接，统一调用 shutdownConn 关闭并收集地址列表，
// 最后触发回调。这样各协议客户端无需各自维护一套过期清理代码。
func cleanExpiredClients(clientMap *sync.Map, expireCallback ClientExpireCallBack) {
	now := time.Now().Unix()
	removeClient := make([]string, 0)
	clientMap.Range(func(key, value interface{}) bool {
		client := value.(clientConnection)
		if client.GetActiveTime()+clientExpireTime <= now {
			removeClient = append(removeClient, key.(string))
		}
		return true
	})

	callBackList := make([]string, 0)
	for _, key := range removeClient {
		value, exists := clientMap.Load(key)
		if !exists {
			continue
		}

		// 再次判断是否过期，防止将要移除时有发生通信的事件
		client := value.(clientConnection)
		if client.GetActiveTime()+clientExpireTime > time.Now().Unix() {
			continue
		}

		// 统一通过 shutdownConn 关闭连接并标记停止
		shutdownConn(client)
		clientMap.Delete(key)
		callBackList = append(callBackList, key)
	}

	if len(callBackList) > 0 {
		InfoLog("移除过期客户端连接:%v", callBackList)
		if expireCallback != nil {
			expireCallback(callBackList)
		}
	}
}
