package starGo

import (
	"net"
)

// udpServer 封装一次 UDP 服务端监听所需的运行时状态：底层连接、业务回调
// 以及头部协议长度。把这些信息集中在结构体里，既避免了用包级全局变量在
// 文件之间传递的隐式约定，也让“监听、读取、路由”这一层职责更加内聚。
// 已发现的客户端仍统一登记在包级 udpClientMap 中，以便系统退出等流程
// 沿用既有方式遍历处理。
type udpServer struct {
	conn      *net.UDPConn
	handler   ClientCallBack
	headerLen int32
}

// StartUdpServer 启动 UDP 服务端：解析并监听地址，登记回调与头部协议长度，
// 随后开启读取协程持续接收数据报。对外签名与行为保持不变。
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

	// 设置收发缓冲区
	_ = conn.SetReadBuffer(1024)
	_ = conn.SetWriteBuffer(1024)

	server := &udpServer{
		conn:      conn,
		handler:   handler,
		headerLen: headerLen,
	}

	// 启动监听协程
	server.listen()

	return nil
}

// GetUdpClient 根据地址获取已注册的 UDP 客户端，不存在时返回 nil。
func GetUdpClient(addr string) *UdpClient {
	client, exists := udpClientMap.Load(addr)
	if !exists {
		return nil
	}

	return client.(*UdpClient)
}

// listen 启动监听协程，并把底层连接的关闭与读取协程绑定在一起：收到停止
// 信号或读取协程自然退出时，统一关闭连接，避免连接生命周期管理散落在
// 多个位置。
func (s *udpServer) listen() {
	Go(func(Stop chan struct{}) {
		done := make(chan struct{})
		Go(func(Stop1 chan struct{}) {
			select {
			case <-Stop1:
			case <-done:
			}
			_ = s.conn.Close()
		})

		s.receive()

		close(done)
	})
}

// receive 持续从连接读取数据报，并按来源地址把数据交给对应的客户端。
// 该方法只承担“读取 + 路由”这一层职责，至于消息如何解析、如何分发给
// 业务回调，则交由 UdpClient 处理，读写两侧的职责边界因此更清晰。
func (s *udpServer) receive() {
	buf := make([]byte, 1024)
	for allForStopSignal == 0 {
		n, udpAddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			break
		}

		if n <= 0 {
			continue
		}

		// 复制本次读取到的数据报：下一轮读取会复用同一个 buf，若直接把
		// buf 投递给客户端，未处理完的数据会被后续读取覆盖。复制一份独立
		// 数据可保证投递给客户端的内容稳定可靠。
		datagram := make([]byte, n)
		copy(datagram, buf[:n])

		client := s.obtainClient(udpAddr)
		client.pushReceive(datagram)
	}
}

// obtainClient 返回来源地址对应的客户端：首次出现的地址会被创建、登记并
// 启动。把“发现客户端 + 注册到 map + 启动读写协程”收敛到同一处，使客户端
// 的注册与启动过程一目了然，也避免启动时序分散在多个位置。
func (s *udpServer) obtainClient(udpAddr *net.UDPAddr) *UdpClient {
	addr := udpAddr.String()
	if existing, exists := udpClientMap.Load(addr); exists {
		return existing.(*UdpClient)
	}

	client := newUdpClient(s.conn, udpAddr, s.handler, s.headerLen)
	if actual, loaded := udpClientMap.LoadOrStore(addr, client); loaded {
		// 已有其它路径完成注册，沿用既有客户端，避免重复启动协程。
		return actual.(*UdpClient)
	}

	// 仅在首次注册成功后启动一次，确保读写协程不会被重复拉起。
	client.start()
	return client
}
