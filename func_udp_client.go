package starGo

import (
	"net"
	"sync"
)

// UdpClient 表示一个被服务端发现的 UDP 对端。每个客户端都持有自己的
// 接收队列、发送队列，以及解析与分发消息所需的全部配置（头部长度与
// 业务回调），因此其消息流转完全自包含，不再反向读取包级全局变量，
// 降低了客户端与服务端代码之间的隐式耦合。
type UdpClient struct {
	addr      *net.UDPAddr
	conn      *net.UDPConn
	handler   ClientCallBack
	headerLen int32

	receiveCh chan []byte
	sendCh    chan []byte

	stop  bool
	mutex sync.RWMutex
}

// newUdpClient 创建客户端，并把消息解析、分发所需的配置一并注入，
// 使客户端无需依赖服务端在别处设置的全局状态。
func newUdpClient(conn *net.UDPConn, addr *net.UDPAddr, handler ClientCallBack, headerLen int32) *UdpClient {
	return &UdpClient{
		addr:      addr,
		conn:      conn,
		handler:   handler,
		headerLen: headerLen,
		receiveCh: make(chan []byte, 1024),
		sendCh:    make(chan []byte, 1024),
		stop:      false,
	}
}

func (c *UdpClient) GetAddr() *net.UDPAddr {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.addr
}

func (c *UdpClient) GetStop() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.stop
}

func (c *UdpClient) SetStop() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.stop = true
}

// GetReceiveData 按“头部长度 + 内容”的协议从单个数据报中解析出业务消息。
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

// SetSendQueue 将待发送数据投递到发送队列，由发送协程统一写出。
// 直接复用并发安全的 channel 完成投递，不再额外持有互斥锁，
// 避免在队列写满时因持锁阻塞而牵连到其它读写操作。
func (c *UdpClient) SetSendQueue(data []byte) {
	if c.GetStop() {
		return
	}
	c.sendCh <- data
}

// pushReceive 供服务端读取协程调用，把收到的数据报投递到接收队列。
// 它是“服务端读取”与“客户端处理”之间唯一的数据交接点，边界清晰。
func (c *UdpClient) pushReceive(data []byte) {
	if c.GetStop() {
		return
	}
	c.receiveCh <- data
}

// start 统一启动客户端的接收处理与发送处理协程。把读写协程的启动
// 时序集中在这里，再配合两个协程退出时统一更新停止状态，
// 避免启动动作与状态维护分散到多个方法中。
func (c *UdpClient) start() {
	c.receiveLoop()
	c.sendLoop()
}

// receiveLoop 从接收队列取出数据报，解析后分发给业务回调。
func (c *UdpClient) receiveLoop() {
	Go(func(Stop chan struct{}) {
		defer c.SetStop()

		for !c.GetStop() {
			select {
			case <-Stop:
				return
			case receiveData := <-c.receiveCh:
				c.dispatch(receiveData)
			}
		}
	})
}

// dispatch 解析单个数据报，存在合法消息时再交给业务回调处理。
// 回调放到独立协程执行，避免业务处理耗时阻塞接收队列的消费。
func (c *UdpClient) dispatch(receiveData []byte) {
	message, exists := c.GetReceiveData(c.headerLen, receiveData)
	if !exists || c.handler == nil {
		return
	}

	addr := c.GetAddr().String()
	Go2(func() {
		c.handler(message, addr)
	})
}

// sendLoop 从发送队列取出数据并写回对端，单协程顺序写出，
// 使发送过程清晰且保持有序。
func (c *UdpClient) sendLoop() {
	Go(func(Stop chan struct{}) {
		defer c.SetStop()

		for !c.GetStop() {
			select {
			case <-Stop:
				return
			case sendData := <-c.sendCh:
				if _, err := c.conn.WriteToUDP(sendData, c.GetAddr()); err != nil {
					ErrorLog("向Udp客户端:%v发送数据出错,错误信息:%v", c.GetAddr().String(), err)
					return
				}
			}
		}
	})
}
