package starGo

import (
	"net"
	"sync"
)

// UdpClient 表示一个已发现的 UDP 远端客户端。
// 每个客户端拥有独立的接收/发送通道和读写协程。
type UdpClient struct {
	addr      *net.UDPAddr
	conn      *net.UDPConn
	headerLen int32
	handler   ClientCallBack

	stop    bool
	mutex   sync.RWMutex

	receiveCh chan []byte
	sendCh    chan []byte

	// stopOnce 保证 stop 只被关闭一次
	stopOnce sync.Once
}

// newUdpClient 创建一个新的 UdpClient。读写协程由调用方通过 start() 启动。
func newUdpClient(conn *net.UDPConn, addr *net.UDPAddr, headerLen int32, handler ClientCallBack) *UdpClient {
	return &UdpClient{
		addr:      addr,
		conn:      conn,
		headerLen: headerLen,
		handler:   handler,
		stop:      false,
		receiveCh: make(chan []byte, 1024),
		sendCh:    make(chan []byte, 1024),
	}
}

// ---------- 公开方法 ----------

// GetAddr 返回客户端的远端地址。
func (c *UdpClient) GetAddr() *net.UDPAddr {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.addr
}

// GetStop 返回客户端是否已停止。
func (c *UdpClient) GetStop() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.stop
}

// SetStop 标记客户端为已停止。
func (c *UdpClient) SetStop() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.stop = true
}

// GetReceiveData 从原始数据中按头部长度提取一条完整消息。
// 返回消息内容和是否成功提取。
func (c *UdpClient) GetReceiveData(headerLen int32, data []byte) (message []byte, exists bool) {
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

// SetSendQueue 将数据放入发送队列。
func (c *UdpClient) SetSendQueue(data []byte) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.sendCh <- data
}

// ---------- 内部方法 ----------

// enqueueReceive 将接收到的数据放入接收通道，供读协程处理。
func (c *UdpClient) enqueueReceive(data []byte) {
	if c.GetStop() {
		return
	}
	c.receiveCh <- data
}

// read 启动接收协程：从 receiveCh 读取数据，解析消息，触发回调。
func (c *UdpClient) read() {
	Go(func(Stop chan struct{}) {
		defer c.markStopped()

		for {
			if c.GetStop() {
				return
			}
			select {
			case <-Stop:
				return
			case receiveData := <-c.receiveCh:
				Go2(func() {
					message, exists := c.GetReceiveData(c.headerLen, receiveData)
					if exists && c.handler != nil {
						c.handler(message, c.GetAddr().String())
					}
				})
			}
		}
	})
}

// write 启动发送协程：从 sendCh 读取数据，写入 UDP conn。
func (c *UdpClient) write() {
	Go(func(Stop chan struct{}) {
		defer c.markStopped()

		for {
			if c.GetStop() {
				return
			}
			select {
			case <-Stop:
				return
			case sendData := <-c.sendCh:
				Go2(func() {
					_, _ = c.conn.WriteToUDP(sendData, c.GetAddr())
				})
			}
		}
	})
}

// markStopped 安全地将客户端标记为停止状态。
func (c *UdpClient) markStopped() {
	c.stopOnce.Do(func() {
		c.SetStop()
	})
}

// start 统一启动客户端的读、写协程。
// 由 UdpServer.getOrCreateClient 在客户端首次注册时调用。
func (c *UdpClient) start() {
	c.read()
	c.write()
}
