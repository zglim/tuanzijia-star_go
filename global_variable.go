package starGo

import (
	"math"
	"math/big"
	"os"
	"sync"

	"github.com/nats-io/nats.go"
)

var (
	maxBigInt64Edge = big.NewInt(0).Add(big.NewInt(math.MaxInt64), big.NewInt(1))
	baseString      = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	logDirPath   string
	logFileMap   map[logLv]*os.File
	logMutex     sync.Mutex
	logCh        chan *logObj
	logOnce      sync.Once
	logNowLv     logLv
	logLvNameMap = map[logLv]string{
		Debug: "debug",
		Info:  "info",
		Warn:  "warn",
		Error: "error",
		Fatal: "fatal",
	}

	// 系统生命周期协调原语，由 func_system.go 的退出流程统一驱动：
	// xxxForStopSignal 为只会翻转一次的停止标志，stopChanXxx 是对应的广播通道，
	// 业务 / 日志协程监听它们决定何时收尾，waitXxxGroup 用于等待这些协程全部退出。
	allForStopSignal int32
	logForStopSignal int32
	waitAllGroup     sync.WaitGroup
	waitLogGroup     sync.WaitGroup
	goCount          int32
	goId             uint64
	stopChanForGo    = make(chan struct{})
	stopChanForLog   = make(chan struct{})

	timerMutex       sync.RWMutex
	oneMinuteFunc    map[string]timerFunc
	fiveMinuteFunc   map[string]timerFunc
	thirtyMinuteFunc map[string]timerFunc

	tcpClientMap              sync.Map
	udpClientMap              sync.Map
	wsClientMap               sync.Map
	tcpReceiveDataHeaderLen   int32
	udpReceiveDataHeaderLen   int32
	wsReceiveDataHeaderLen    int32
	tcpHandlerReceiveFunc     ClientCallBack
	udpHandlerReceiveFunc     ClientCallBack
	wsHandlerReceiveFunc      ClientCallBack
	tcpClientExpireHandleFunc ClientExpireCallBack
	wsClientExpireHandleFunc  ClientExpireCallBack

	natChMap sync.Map
	natConn  *nats.Conn

	mysqlCfg *Mysql
	redisCfg *Redis
)
