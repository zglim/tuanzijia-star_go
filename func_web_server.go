package starGo

import "github.com/gin-gonic/gin"

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

// supportedRequestTypes 收敛所有受支持的请求方法。
// 新增方法时只需在此登记，无需再维护重复的分发分支。
var supportedRequestTypes = map[webRequestType]struct{}{
	GET:     {},
	POST:    {},
	DELETE:  {},
	PATCH:   {},
	PUT:     {},
	OPTIONS: {},
	HEAD:    {},
}

// isSupported 判断该请求方法是否被支持，用于注册前的前置校验。
func (t webRequestType) isSupported() bool {
	_, ok := supportedRequestTypes[t]
	return ok
}

// routeRegistry 专门负责已注册路由的去重校验，
// 与真正的路由注册解耦，使重复 URL 判断的职责更清晰。
type routeRegistry struct {
	registered map[webRequestType]map[string]bool
}

func newRouteRegistry() *routeRegistry {
	return &routeRegistry{
		registered: make(map[webRequestType]map[string]bool),
	}
}

// reserve 尝试占用一个 requestType+url 路由。
// 若该路由此前已被注册则返回 false，否则记录并返回 true。
func (r *routeRegistry) reserve(requestType webRequestType, url string) bool {
	urls, exists := r.registered[requestType]
	if !exists {
		urls = make(map[string]bool)
		r.registered[requestType] = urls
	}
	if urls[url] {
		return false
	}
	urls[url] = true
	return true
}

type WebServer struct {
	router          *gin.Engine // 路由实例
	serverIpAddress string
	isUseMiddleware bool
	routes          *routeRegistry // 已注册路由记录，用于重复 URL 校验
}

func NewWebServer(ip string, isUseMiddleware bool) *WebServer {
	return &WebServer{
		router:          gin.New(),
		serverIpAddress: ip,
		isUseMiddleware: isUseMiddleware,
		routes:          newRouteRegistry(),
	}
}

func (w *WebServer) StartWebServer() {
	w.applyMiddleware()

	if err := w.runRouter(); err != nil {
		ErrorLog("启动webServer时出错,错误信息:%v", err)
	}
}

// applyMiddleware 根据配置集中启用所需中间件，统一管理中间件策略。
func (w *WebServer) applyMiddleware() {
	if !w.isUseMiddleware {
		return
	}
	w.router.Use(gin.Recovery(), gin.Logger())
}

// runRouter 负责服务启动参数处理：按是否配置监听地址选择启动方式。
func (w *WebServer) runRouter() error {
	if w.serverIpAddress == "" {
		return w.router.Run()
	}
	return w.router.Run(w.serverIpAddress)
}

func (w *WebServer) RegisterRequestHandleFunc(requestType webRequestType, url string, handleFunc gin.HandlerFunc) {
	if !requestType.isSupported() {
		ErrorLog("不支持的请求方法,requestType:%v, url:%v", requestType, url)
		return
	}

	// 先做去重校验：占用失败说明该路由已注册，直接返回。
	if !w.routes.reserve(requestType, url) {
		ErrorLog("已注册相同URL路径,requestType:%v, url:%v", requestType, url)
		return
	}

	// 校验通过后再交由 gin 做真实路由注册，二者职责清晰分离。
	w.router.Handle(string(requestType), url, handleFunc)
}
