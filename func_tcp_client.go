package starGo

import (
	"io"
	"net"
	"time"
)

type ClientCallBack func(message []byte, addr string)

type ClientExpireCallBack func(addr []string)

type Client struct {
	BaseClient
	conn         net.Conn
	receiveQueue []byte
}

func newTcpClient(conn net.Conn) *Client {
	return &Client{
		BaseClient:   newBaseClient(),
		conn:         conn,
		receiveQueue: make([]byte, 0),
	}
}

func (c *Client) GetConn() net.Conn {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.conn
}

// Close 关闭底层 TCP 连接，实现 clientConnection 接口。
func (c *Client) Close() error {
	return c.GetConn().Close()
}

// Addr 返回客户端地址字符串，实现 clientConnection 接口。
func (c *Client) Addr() string {
	return c.GetConn().RemoteAddr().String()
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

func (c *Client) start() {
	// 读协程：负责从连接读取数据并分发到业务回调
	Go(func(Stop chan struct{}) {
		defer func() {
			shutdownConn(c)
		}()
		for !c.GetStop() {
			readBytes := make([]byte, 1024)
			n, err := c.GetConn().Read(readBytes)
			if err != nil {
				if err != io.EOF {
					ErrorLog("读取消息错误：%s，本次读取的字节数为：%d", err, n)
				}
				break
			}
			c.AppendReceiveQueue(readBytes[:n])
			c.SetActiveTime()

			Go2(func() {
				message, exists := c.GetReceiveData(tcpReceiveDataHeaderLen)
				if exists && tcpHandlerReceiveFunc != nil {
					tcpHandlerReceiveFunc(message, c.Addr())
				}
			})
		}
	})
	// 写协程：负责从发送队列取数据写入连接，复用通用发送循环
	sendLoopWithChannel(c, c.sendCh, func(message []byte) error {
		header := Int32ToBytes(int32(len(message)), true)
		header = append(header, message...)
		_, err := c.GetConn().Write(header)
		return err
	})
}

func registerTcpClient(c *Client) {
	tcpClientMap.Store(c.Addr(), c)
}

func clearExpireTcpClient() {
	Go(func(Stop chan struct{}) {
		t := time.NewTicker(5 * time.Second)
		for allForStopSignal == 0 {
			<-t.C
			cleanExpiredClients(&tcpClientMap, tcpClientExpireHandleFunc)
		}
	})
}
