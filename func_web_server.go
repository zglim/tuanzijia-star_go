package starGo

import "github.com/gin-gonic/gin"

// webRequestType 表示 HTTP 请求方法类型
type webRequestType string

const (
	GET     webRequestType = "GET"
	POST    webRequestType = "POST"
	DELETE  webRequestType = "DELETE"
	PATCH   webRequestType = "PATCH"
	PUT     webRequestType = "PUT"
	OPTIONS webRequestType = "OPTIONS"
	HEAD    webRequestType = "HEAD"
)

// WebServer 封装基于 gin 的 HTTP 服务，负责路由注册、去重校验和服务启动。
type WebServer struct {
	router          *gin.Engine                  // 路由实例
	serverIpAddress string                       // 监听地址，为空时使用 gin 默认地址
	isUseMiddleware bool                         // 是否启用默认中间件（Recovery + Logger）
	registeredURLs  map[webRequestType]map[string]bool // 已注册路由的去重记录
}

// NewWebServer 创建一个新的 WebServer 实例。
//   - ip: 监听地址，传空字符串时使用 gin 默认端口
//   - isUseMiddleware: 是否在启动时自动挂载 Recovery 和 Logger 中间件
func NewWebServer(ip string, isUseMiddleware bool) *WebServer {
	return &WebServer{
		router:          gin.New(),
		serverIpAddress: ip,
		isUseMiddleware: isUseMiddleware,
		registeredURLs:  make(map[webRequestType]map[string]bool),
	}
}

// StartWebServer 启动 HTTP 服务。
// 流程：挂载中间件 → 解析监听地址 → 启动路由
func (w *WebServer) StartWebServer() {
	w.applyMiddleware()

	addr := w.resolveListenAddr()
	var err error
	if addr == "" {
		err = w.router.Run()
	} else {
		err = w.router.Run(addr)
	}
	if err != nil {
		ErrorLog("启动webServer时出错,错误信息:%v", err)
	}
}

// RegisterRequestHandleFunc 注册指定请求方法的路由处理器。
// 若同一 (requestType, url) 组合已注册，则打印错误日志并跳过，保证路由不会重复注册。
func (w *WebServer) RegisterRequestHandleFunc(requestType webRequestType, url string, handleFunc gin.HandlerFunc) {
	if w.isRouteRegistered(requestType, url) {
		ErrorLog("已注册相同URL路径,requestType:%v, url:%v", requestType, url)
		return
	}
	w.markRouteRegistered(requestType, url)

	// 利用 gin.Engine.Handle 统一分发，无需按方法类型逐个 switch
	w.router.Handle(string(requestType), url, handleFunc)
}

// ---------- 内部辅助方法 ----------

// applyMiddleware 根据配置决定是否挂载默认中间件。
func (w *WebServer) applyMiddleware() {
	if !w.isUseMiddleware {
		return
	}
	w.router.Use(gin.Recovery())
	w.router.Use(gin.Logger())
}

// resolveListenAddr 返回服务监听地址；空字符串表示由 gin 自行决定默认端口。
func (w *WebServer) resolveListenAddr() string {
	return w.serverIpAddress
}

// isRouteRegistered 判断指定请求方法和 URL 是否已注册。
func (w *WebServer) isRouteRegistered(requestType webRequestType, url string) bool {
	urls, methodExists := w.registeredURLs[requestType]
	if !methodExists {
		return false
	}
	return urls[url]
}

// markRouteRegistered 记录已注册的路由，用于后续去重校验。
func (w *WebServer) markRouteRegistered(requestType webRequestType, url string) {
	if _, methodExists := w.registeredURLs[requestType]; !methodExists {
		w.registeredURLs[requestType] = make(map[string]bool)
	}
	w.registeredURLs[requestType][url] = true
}
