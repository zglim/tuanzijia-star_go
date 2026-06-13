package starGo

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// newTestWebServer 创建一个用于单元测试的 WebServer，gin 设为测试模式。
func newTestWebServer() *WebServer {
	gin.SetMode(gin.TestMode)
	return NewWebServer("", false)
}

// performRequest 通过 httptest 向 router 发送请求并返回 ResponseRecorder。
func performRequest(w *WebServer, method, path string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	w.router.ServeHTTP(recorder, req)
	return recorder
}

func TestRegisterRequestHandleFunc_GET(t *testing.T) {
	w := newTestWebServer()
	w.RegisterRequestHandleFunc(GET, "/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	res := performRequest(w, "GET", "/ping")
	if res.Code != http.StatusOK {
		t.Fatalf("期望状态码 %d，实际 %d", http.StatusOK, res.Code)
	}
	if res.Body.String() != "pong" {
		t.Fatalf("期望响应体 %q，实际 %q", "pong", res.Body.String())
	}
}

func TestRegisterRequestHandleFunc_AllMethods(t *testing.T) {
	methods := []webRequestType{GET, POST, PUT, DELETE, PATCH, OPTIONS, HEAD}

	for _, method := range methods {
		t.Run(string(method), func(t *testing.T) {
			w := newTestWebServer()
			w.RegisterRequestHandleFunc(method, "/test", func(c *gin.Context) {
				c.String(http.StatusOK, string(method))
			})

			res := performRequest(w, string(method), "/test")
			if res.Code != http.StatusOK {
				t.Fatalf("方法 %s: 期望状态码 %d，实际 %d", method, http.StatusOK, res.Code)
			}
			// HEAD 请求不返回 body
			if method != HEAD && res.Body.String() != string(method) {
				t.Fatalf("方法 %s: 期望响应体 %q，实际 %q", method, method, res.Body.String())
			}
		})
	}
}

func TestRegisterRequestHandleFunc_DuplicateURL(t *testing.T) {
	w := newTestWebServer()

	w.RegisterRequestHandleFunc(GET, "/dup", func(c *gin.Context) {
		c.String(http.StatusOK, "first")
	})
	// 重复注册相同方法和路径，应该被拒绝
	w.RegisterRequestHandleFunc(GET, "/dup", func(c *gin.Context) {
		c.String(http.StatusOK, "second")
	})

	res := performRequest(w, "GET", "/dup")
	if res.Code != http.StatusOK {
		t.Fatalf("期望状态码 %d，实际 %d", http.StatusOK, res.Code)
	}
	// 应该返回第一次注册的处理器的响应
	if res.Body.String() != "first" {
		t.Fatalf("期望响应体 %q（首次注册），实际 %q", "first", res.Body.String())
	}
}

func TestRegisterRequestHandleFunc_SameURLDifferentMethods(t *testing.T) {
	w := newTestWebServer()

	w.RegisterRequestHandleFunc(GET, "/resource", func(c *gin.Context) {
		c.String(http.StatusOK, "get")
	})
	w.RegisterRequestHandleFunc(POST, "/resource", func(c *gin.Context) {
		c.String(http.StatusOK, "post")
	})

	resGet := performRequest(w, "GET", "/resource")
	if resGet.Body.String() != "get" {
		t.Fatalf("GET 期望 %q，实际 %q", "get", resGet.Body.String())
	}

	resPost := performRequest(w, "POST", "/resource")
	if resPost.Body.String() != "post" {
		t.Fatalf("POST 期望 %q，实际 %q", "post", resPost.Body.String())
	}
}

func TestRegisterRequestHandleFunc_UnregisteredRoute(t *testing.T) {
	w := newTestWebServer()

	res := performRequest(w, "GET", "/not-exist")
	if res.Code != http.StatusNotFound {
		t.Fatalf("未注册路由期望状态码 %d，实际 %d", http.StatusNotFound, res.Code)
	}
}

func TestIsRouteRegistered(t *testing.T) {
	w := newTestWebServer()

	if w.isRouteRegistered(GET, "/foo") {
		t.Fatal("注册前应返回 false")
	}

	w.markRouteRegistered(GET, "/foo")
	if !w.isRouteRegistered(GET, "/foo") {
		t.Fatal("注册后应返回 true")
	}

	// 同路径不同方法不应互相影响
	if w.isRouteRegistered(POST, "/foo") {
		t.Fatal("不同方法应返回 false")
	}
}

func TestNewWebServer_Fields(t *testing.T) {
	w := NewWebServer("0.0.0.0:8080", true)
	if w.router == nil {
		t.Fatal("router 不应为 nil")
	}
	if w.serverIpAddress != "0.0.0.0:8080" {
		t.Fatalf("期望地址 %q，实际 %q", "0.0.0.0:8080", w.serverIpAddress)
	}
	if !w.isUseMiddleware {
		t.Fatal("期望 isUseMiddleware 为 true")
	}
	if w.registeredURLs == nil {
		t.Fatal("registeredURLs 不应为 nil")
	}
}

func TestResolveListenAddr(t *testing.T) {
	w1 := NewWebServer("127.0.0.1:9090", false)
	if addr := w1.resolveListenAddr(); addr != "127.0.0.1:9090" {
		t.Fatalf("期望 %q，实际 %q", "127.0.0.1:9090", addr)
	}

	w2 := NewWebServer("", false)
	if addr := w2.resolveListenAddr(); addr != "" {
		t.Fatalf("期望空字符串，实际 %q", addr)
	}
}
