package starGo

import (
	"net"
	"sync"
)

// UdpServer 负责 UDP 服务端的生命周期：监听、客户端发现、消息分发。
type UdpServer struct {
	conn      *net.UDPConn
	headerLen int32
	handler   ClientCallBack

	mu      sync.RWMutex
	clients map[string]*UdpClient
}

// newUdpServer 创建一个 UdpServer 实例。
func newUdpServer(conn *net.UDPConn, handler ClientCallBack, headerLen int32) *UdpServer {
	return &UdpServer{
		conn:      conn,
		headerLen: headerLen,
		handler:   handler,
		clients:   make(map[string]*UdpClient),
	}
}

// getOrCreateClient 根据远端地址查找或创建客户端，并启动其读写协程。
// 返回客户端实例以及是否为新创建。
func (s *UdpServer) getOrCreateClient(addr *net.UDPAddr) (*UdpClient, bool) {
	key := addr.String()

	// 快速路径：读锁查找
	s.mu.RLock()
	c, ok := s.clients[key]
	s.mu.RUnlock()
	if ok {
		return c, false
	}

	// 慢路径：写锁创建
	s.mu.Lock()
	defer s.mu.Unlock()

	// 双重检查
	if c, ok = s.clients[key]; ok {
		return c, false
	}

	c = newUdpClient(s.conn, addr, s.headerLen, s.handler)
	s.clients[key] = c

	// 同步启动读写协程，保证客户端状态一致
	c.start()

	return c, true
}

// getClient 根据地址字符串查找已注册的客户端。
func (s *UdpServer) getClient(addr string) *UdpClient {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clients[addr]
}

// removeClient 移除已注册的客户端。
func (s *UdpServer) removeClient(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, addr)
}

// listen 启动 UDP 监听协程，负责从 conn 读取数据并分发给对应客户端。
func (s *UdpServer) listen() {
	_ = s.conn.SetReadBuffer(1024)
	_ = s.conn.SetWriteBuffer(1024)

	Go(func(Stop chan struct{}) {
		done := make(chan struct{})

		// 协程负责在停止信号到来或监听结束后关闭 conn
		Go(func(Stop1 chan struct{}) {
			select {
			case <-Stop1:
			case <-done:
			}
			_ = s.conn.Close()
		})

		s.readLoop()

		close(done)
	})
}

// readLoop 持续从 UDP conn 读取数据包，将其分发给对应客户端。
func (s *UdpServer) readLoop() {
	buf := make([]byte, 1024)
	for allForStopSignal == 0 {
		n, udpAddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if err.(net.Error).Timeout() {
				continue
			}
			break
		}
		if n <= 0 {
			continue
		}

		// 拷贝数据，避免后续读取覆盖
		data := make([]byte, n)
		copy(data, buf[:n])

		client, _ := s.getOrCreateClient(udpAddr)
		client.enqueueReceive(data)
	}
}

// shutdown 关闭所有客户端并清理资源。
func (s *UdpServer) shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for addr, c := range s.clients {
		c.SetStop()
		delete(s.clients, addr)
	}
}

// ---------- 全局兼容层 ----------

// defaultUdpServer 是全局默认 UDP 服务端实例，用于保持 StartUdpServer / GetUdpClient 接口不变。
var defaultUdpServer *UdpServer

// StartUdpServer 启动 UDP 服务端监听，注册消息回调和头部长度。
func StartUdpServer(addr string, handler ClientCallBack, headerLen int32) error {
	InfoLog("开始监听Udp地址:%v", addr)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		ErrorLog("监听Udp地址:%v失败,错误信息:%v", addr, err)
		return err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		ErrorLog("监听Udp地址:%v失败,错误信息:%v", addr, err)
		return err
	}

	srv := newUdpServer(conn, handler, headerLen)
	defaultUdpServer = srv

	// 同步到全局变量，保持其他模块（如 systemExit）的兼容性
	udpHandlerReceiveFunc = handler
	udpReceiveDataHeaderLen = headerLen

	srv.listen()
	return nil
}

// GetUdpClient 根据地址字符串获取已注册的 UDP 客户端。
func GetUdpClient(addr string) *UdpClient {
	if defaultUdpServer == nil {
		return nil
	}
	return defaultUdpServer.getClient(addr)
}
