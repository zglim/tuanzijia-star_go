package starGo

import (
	"io"
	"net"
	"time"
)

type ClientCallBack func(message []byte, addr string)

type ClientExpireCallBack func(addr []string)

type Client struct {
	baseClient
	conn         net.Conn
	receiveQueue []byte
}

func newTcpClient(conn net.Conn) *Client {
	return &Client{
		baseClient:   newBaseClient(conn, conn.RemoteAddr().String()),
		conn:         conn,
		receiveQueue: make([]byte, 0),
	}
}

func (c *Client) GetConn() net.Conn {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.conn
}

func (c *Client) GetReceiveData(headerLen int32) (message []byte, exists bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.receiveQueue) < int(headerLen) {
		return
	}

	// 获取头部信息
	header := c.receiveQueue[:headerLen]

	// 将头部数据转换为内容的长度
	contentLength := BytesToInt32(header, true)

	// 判断长度是否满足
	if len(c.receiveQueue) < int(headerLen+contentLength) {
		return
	}

	// 提取消息内容
	message = c.receiveQueue[headerLen : headerLen+contentLength]

	// 将对应的数据截断，以得到新的内容
	c.receiveQueue = c.receiveQueue[headerLen+contentLength:]

	// 存在合理的数据
	exists = true

	return
}

func (c *Client) AppendReceiveQueue(message []byte) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.receiveQueue = append(c.receiveQueue, message...)
}

// start 启动 TCP 客户端的读、写两个协程。读写协程的循环骨架与连接关闭收口都复用
// baseClient，这里只负责注入 TCP 特有的读取、解析与封帧写出逻辑。
func (c *Client) start() {
	Go(func(Stop chan struct{}) {
		c.runReceiveLoop(c.readOnce, c.dispatch)
	})
	Go(func(Stop chan struct{}) {
		c.runSendLoop(Stop, c.writeOnce)
	})
}

// readOnce 读取一段原始字节并追加到接收缓冲区；返回的 payload 不携带数据，实际的
// 粘包拆分在 dispatch 中从缓冲区完成。出错时返回 ok=false 以结束读协程。
func (c *Client) readOnce() (payload []byte, ok bool) {
	readBytes := make([]byte, 1024)
	n, err := c.GetConn().Read(readBytes)
	if err != nil {
		if err != io.EOF {
			ErrorLog("读取消息错误：%s，本次读取的字节数为：%d", err, n)
		}
		return nil, false
	}
	c.AppendReceiveQueue(readBytes[:n])
	return nil, true
}

// dispatch 从接收缓冲区按头部长度拆出一条完整消息并回调业务处理函数。
func (c *Client) dispatch(_ []byte) {
	message, exists := c.GetReceiveData(tcpReceiveDataHeaderLen)
	if exists && tcpHandlerReceiveFunc != nil {
		tcpHandlerReceiveFunc(message, c.GetConn().RemoteAddr().String())
	}
}

// writeOnce 按“长度头 + 内容”的格式将一条消息写出到连接。
func (c *Client) writeOnce(message []byte) error {
	header := Int32ToBytes(int32(len(message)), true)
	header = append(header, message...)
	_, err := c.GetConn().Write(header)
	return err
}

func registerTcpClient(c *Client) {
	tcpClientMap.Store(c.GetConn().RemoteAddr().String(), c)
}

func clearExpireTcpClient() {
	Go(func(Stop chan struct{}) {
		t := time.NewTicker(5 * time.Second)
		for allForStopSignal == 0 {
			<-t.C
			removeClient := make([]string, 0)
			tcpClientMap.Range(func(key, value interface{}) bool {
				client := value.(*Client)
				if client.GetActiveTime()+clientExpireTime <= time.Now().Unix() {
					removeClient = append(removeClient, key.(string))
				}

				return true
			})

			// 移除过期的客户端
			callBackList := make([]string, 0)
			for _, key := range removeClient {
				value, exists := tcpClientMap.Load(key)
				if !exists {
					continue
				}

				// 再次判断是否过期，防止将要移除时有发生通信的事件
				client := value.(*Client)
				if client.GetActiveTime()+clientExpireTime > time.Now().Unix() {
					continue
				}

				// 移除过期客户端，统一通过 shutdown 收口连接关闭与停止状态
				client.shutdown()
				tcpClientMap.Delete(key)
				callBackList = append(callBackList, key)
			}

			if len(callBackList) > 0 {
				InfoLog("移除过期客户端连接:%v", callBackList)
				if tcpClientExpireHandleFunc != nil {
					tcpClientExpireHandleFunc(callBackList)
				}
			}
		}
	})
}
