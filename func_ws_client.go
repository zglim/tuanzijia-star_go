package starGo

import (
	"time"

	"github.com/gorilla/websocket"
)

type WebSocketClient struct {
	BaseClient
	conn *websocket.Conn
}

func newWebSocketClient(conn *websocket.Conn) *WebSocketClient {
	return &WebSocketClient{
		BaseClient: newBaseClient(),
		conn:       conn,
	}
}

func (c *WebSocketClient) GetConn() *websocket.Conn {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.conn
}

// Close 关闭底层 WebSocket 连接，实现 clientConnection 接口。
func (c *WebSocketClient) Close() error {
	return c.GetConn().Close()
}

// Addr 返回客户端地址字符串，实现 clientConnection 接口。
func (c *WebSocketClient) Addr() string {
	return c.GetConn().RemoteAddr().String()
}

func (c *WebSocketClient) GetReceiveData(headerLen int32, data []byte) (message []byte, exists bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(data) < int(headerLen) {
		return
	}

	// 获取头部信息
	header := data[:headerLen]

	// 将头部数据转换为内容的长度
	contentLength := BytesToInt32(header, true)

	// 判断长度是否满足
	if len(data) < int(headerLen+contentLength) {
		return
	}

	// 提取消息内容
	message = data[headerLen : headerLen+contentLength]

	// 存在合理的数据
	exists = true

	return
}

func (c *WebSocketClient) start() {
	// 读协程：负责从连接读取消息并分发到业务回调
	Go(func(Stop chan struct{}) {
		defer func() {
			shutdownConn(c)
		}()
		for !c.GetStop() {
			_, data, err := c.GetConn().ReadMessage()
			if err != nil {
				ErrorLog("读取消息错误:%v", err)
				break
			}
			c.SetActiveTime()

			Go2(func() {
				message, exists := c.GetReceiveData(wsReceiveDataHeaderLen, data)
				if exists && wsHandlerReceiveFunc != nil {
					wsHandlerReceiveFunc(message, c.Addr())
				}
			})
		}
	})
	// 写协程：负责从发送队列取数据写入连接，复用通用发送循环
	sendLoopWithChannel(c, c.sendCh, func(message []byte) error {
		return c.GetConn().WriteMessage(websocket.BinaryMessage, message)
	})
}

func registerWebSocketClient(c *WebSocketClient) {
	wsClientMap.Store(c.Addr(), c)
}

func clearExpireWebSocketClient() {
	Go(func(Stop chan struct{}) {
		t := time.NewTicker(5 * time.Second)
		for allForStopSignal == 0 {
			<-t.C
			cleanExpiredClients(&wsClientMap, wsClientExpireHandleFunc)
		}
	})
}
