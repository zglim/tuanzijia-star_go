//go:build integration
// +build integration

// 本文件包含需要外部服务（NATS、TCP 端口、文件系统等）或会阻塞等待信号的集成测试。
// 运行方式: go test -tags integration -run TestInfoLog|TestCsv_UnMarshalFile|TestNatPublish|TestWebSocketClient

package starGo

import (
	"fmt"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestInfoLog(t *testing.T) {
	StartLog("log", Debug)

	InfoLog("qwe")
	go func() {
		time.Sleep(61 * time.Second)
		stopChanForLog <- struct{}{}

		for i := int32(0); i < goCount; i++ {
			stopChanForGo <- struct{}{}
		}
	}()
	go func() {
		for {
			InfoLog("输出当前时间:%v", time.Now())
			WarnLog("输出当前时间:%v", time.Now())
			ErrorLog("输出当前时间:%v", time.Now())
			FatalLog("输出当前时间:%v", time.Now())
			time.Sleep(1 * time.Second)
		}
	}()
	WaitForSystemExit()
}

func TestCsv_UnMarshalFile(t *testing.T) {
	StartLog("log", Debug)
	type abc struct {
		Id   int32
		Name string
		Year string
	}
	inf := make([]abc, 0)
	cs := NewCsvReader()
	cs.UnMarshalFile("csv/test.csv", &inf)
	InfoLog(inf)
	WaitForSystemExit()
}

func TestNatPublish(t *testing.T) {
	StartNatConn("127.0.0.1:4222", "testNat")
	StartLog("log", Debug)

	SubscribeAsync("help", func(messag *NatResult) {
		Publish(messag.Reply, []byte("I can help!"))
	})

	Publish("help", []byte("你好呀"))

	go func() {
		time.Sleep(10 * time.Second)
		stopChanForLog <- struct{}{}

		for i := int32(0); i < goCount; i++ {
			fmt.Println(i)
			stopChanForGo <- struct{}{}
		}
	}()
	WaitForSystemExit()
}

func TestWebSocketClient(t *testing.T) {
	StartLog("log", Debug)

	err := StartTcpServer("127.0.0.1:9999", nil, nil, 4)
	if err != nil {
		ErrorLog(err)
		return
	}

	WaitForSystemExit()
}

// TestGo_Manual 是原始的手动 Web 服务启动测试，需要浏览器访问验证。
// 常规的 WebServer 单元测试请使用 func_web_server_test.go 中的自动化测试。
func TestGo_Manual(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	web := NewWebServer("0.0.0.0:9020", true)
	web.RegisterRequestHandleFunc(GET, "/hello", func(context *gin.Context) {
		context.String(200, "hello,你好呀")
	})
	web.StartWebServer()
	WaitForSystemExit()
}
