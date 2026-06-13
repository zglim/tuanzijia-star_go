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

	goCount int32
	goId    uint64

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
