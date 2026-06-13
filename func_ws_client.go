package starGo

import (
	"time"

	"github.com/gorilla/websocket"
)

type WebSocketClient struct {
	baseClient
	conn *websocket.Conn
}

func newWebSocketClient(conn *websocket.Conn) *WebSocketClient {
	return &WebSocketClient{
		baseClient: newBaseClient(conn, conn.RemoteAddr().String()),
		conn:       conn,
	}
}

func (c *WebSocketClient) GetConn() *websocket.Conn {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.conn
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

// start 启动 WebSocket 客户端的读、写两个协程。读写协程的循环骨架与连接关闭收口都
// 复用 baseClient，这里只注入 WebSocket 特有的读取与写出逻辑。
func (c *WebSocketClient) start() {
	Go(func(Stop chan struct{}) {
		c.runReceiveLoop(c.readOnce, c.dispatch)
	})
	Go(func(Stop chan struct{}) {
		c.runSendLoop(Stop, c.writeOnce)
	})
}

// readOnce 读取一条完整的 WebSocket 消息帧；出错时返回 ok=false 以结束读协程。
func (c *WebSocketClient) readOnce() (payload []byte, ok bool) {
	_, data, err := c.GetConn().ReadMessage()
	if err != nil {
		ErrorLog("读取消息错误:%v", err)
		return nil, false
	}
	return data, true
}

// dispatch 按头部长度从消息帧中拆出内容并回调业务处理函数。
func (c *WebSocketClient) dispatch(data []byte) {
	message, exists := c.GetReceiveData(wsReceiveDataHeaderLen, data)
	if exists && wsHandlerReceiveFunc != nil {
		wsHandlerReceiveFunc(message, c.GetConn().RemoteAddr().String())
	}
}

// writeOnce 以二进制帧的形式将一条消息写出到连接。
func (c *WebSocketClient) writeOnce(message []byte) error {
	return c.GetConn().WriteMessage(websocket.BinaryMessage, message)
}

func registerWebSocketClient(c *WebSocketClient) {
	wsClientMap.Store(c.GetConn().RemoteAddr().String(), c)
}

func clearExpireWebSocketClient() {
	Go(func(Stop chan struct{}) {
		for allForStopSignal == 0 {
			t := time.NewTicker(5 * time.Second)
			<-t.C
			removeClient := make([]string, 0)
			wsClientMap.Range(func(key, value interface{}) bool {
				client := value.(*WebSocketClient)
				if client.GetActiveTime()+clientExpireTime <= time.Now().Unix() {
					removeClient = append(removeClient, key.(string))
				}

				return true
			})

			// 移除过期的客户端
			callBackList := make([]string, 0)
			for _, key := range removeClient {
				value, exists := wsClientMap.Load(key)
				if !exists {
					continue
				}

				// 再次判断是否过期，防止将要移除时有发生通信的事件
				client := value.(*WebSocketClient)
				if client.GetActiveTime()+clientExpireTime > time.Now().Unix() {
					continue
				}

				// 移除过期客户端，统一通过 shutdown 收口连接关闭与停止状态
				client.shutdown()
				wsClientMap.Delete(key)
				callBackList = append(callBackList, key)
			}

			if len(callBackList) > 0 && wsClientExpireHandleFunc != nil {
				wsClientExpireHandleFunc(callBackList)
			}
		}
	})
}
