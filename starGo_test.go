package starGo

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestChannel(t *testing.T) {
	var wg sync.WaitGroup
	c := make(chan struct{})
	fmt.Println(1)
	wg.Add(1)
	go func() {
		<-c
		wg.Done()
		fmt.Println(2)
	}()

	go func() {
		time.Sleep(3 * time.Second)
		close(c)
	}()
	fmt.Println(3)
	wg.Wait()
}

func TestGo(t *testing.T) {
	gin.SetMode(gin.TestMode)
	web := NewWebServer("0.0.0.0:9020", false)
	web.RegisterRequestHandleFunc(GET, "/hello", func(context *gin.Context) {
		context.String(http.StatusOK, "hello,你好呀")
	})

	// 使用 httptest 验证路由注册是否正确，避免启动真实监听导致阻塞
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/hello", nil)
	web.router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("期望状态码 %d，实际 %d", http.StatusOK, recorder.Code)
	}
	expected := "hello,你好呀"
	if recorder.Body.String() != expected {
		t.Fatalf("期望响应体 %q，实际 %q", expected, recorder.Body.String())
	}
}
