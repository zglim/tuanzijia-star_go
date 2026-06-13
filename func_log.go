package starGo

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type logObj struct {
	lv      logLv
	logInfo string
}

// ---------------------------------------------------------------------------
// 日志系统生命周期
// ---------------------------------------------------------------------------

func logStart() {
	goForLog(func(Stop chan struct{}) {
		for logForStopSignal == 0 {
			select {
			case <-Stop:
				return
			case logItem := <-logCh:
				writeLog(logItem)
			}
		}
	})
}

// StartLog 初始化日志系统：创建目录、打开各级别日志文件。只会执行一次。
func StartLog(dirPatch string, lv logLv) {
	logOnce.Do(func() {
		logNowLv = lv
		logDirPath = dirPatch
		// 文件夹路径不存在就创建
		if !IsDirExists(dirPatch) {
			if err := os.MkdirAll(dirPatch, os.ModePerm|os.ModeTemporary); err != nil {
				fmt.Printf("创建日志文件夹错误，错误信息:%v", err)
			}
		}

		for logLv, logName := range logLvNameMap {
			// 低于当前等级的日志不打开文件
			if logLv < lv {
				continue
			}

			// 得到最终的文件绝对路径
			fileName := fmt.Sprintf("%v.log", logName)
			fileAbsolutePath := filepath.Join(dirPatch, fileName)

			// 打开文件(如果文件存在就以写模式打开，并追加写入；如果文件不存在就创建，然后以写模式打开。)
			f, err := os.OpenFile(fileAbsolutePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm|os.ModeTemporary)
			if err != nil {
				fmt.Printf("打开%v日志文件错误，错误信息:%v", fileName, err)
			}

			// 将文件流保存
			logFileMap[logLv] = f
		}
	})
}

// ---------------------------------------------------------------------------
// 日志写入与轮转
// ---------------------------------------------------------------------------

func writeLog(log *logObj) {
	logMutex.Lock()
	defer logMutex.Unlock()

	// 日志级别不存在
	_, logLvExists := logLvNameMap[log.lv]
	if !logLvExists {
		return
	}

	// 日志文件未开启
	file, fileExists := logFileMap[log.lv]
	if !fileExists || file == nil {
		return
	}

	// 记录日志
	_, _ = file.WriteString(log.logInfo)
	fmt.Printf("%v", log.logInfo)
}

func reorganizeLog(nowTime time.Time) {
	logMutex.Lock()
	defer logMutex.Unlock()

	for _, file := range logFileMap {
		// 优先获取文件名
		fileName := file.Name()

		// 将文件流关闭
		_ = file.Close()
		file = nil

		// 重命名文件
		nameList := strings.Split(fileName, ".")
		newFileName := fmt.Sprintf("%v_%v.log", nameList[0], ToDateTimeString(nowTime.Add(-1*time.Hour)))
		err := os.Rename(fileName, newFileName)
		if err != nil {
			ErrorLog("重命名文件:%v失败，错误信息:%v", fileName, err)
		}
	}

	// 重新开启日志文件流
	for logLv, logName := range logLvNameMap {
		if logLv < logNowLv {
			continue
		}
		// 得到最终的文件绝对路径
		fileName := fmt.Sprintf("%v.log", logName)
		fileAbsolutePath := filepath.Join(logDirPath, fileName)

		// 打开文件(如果文件存在就以写模式打开，并追加写入；如果文件不存在就创建，然后以写模式打开。)
		f, err := os.OpenFile(fileAbsolutePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm|os.ModeTemporary)
		if err != nil {
			fmt.Printf("打开%v日志文件错误，错误信息:%v", fileName, err)
		}

		// 将文件流保存
		logFileMap[logLv] = f
	}
}

// ---------------------------------------------------------------------------
// 调用栈信息
// ---------------------------------------------------------------------------

// callerInfo 返回指定调用栈层级的 "文件路径:行号" 信息。
// skip 的含义与 runtime.Caller 的 skip 参数一致：
//
//	0 = callerInfo 自身
//	1 = 调用 callerInfo 的函数
//	2 = 再上一层，以此类推
func callerInfo(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "unknown:0"
	}
	// 截取最后两层目录，保持与旧版 SimpleTack 一致的输出格式
	i := strings.LastIndex(file, "/") + 1
	i = strings.LastIndex(file[:i-1], "/") + 1
	return fmt.Sprintf("%s:%d", file[i:], line)
}

// SimpleTack 返回调用 SimpleTack 的上层函数的 "文件:行号" 信息（向后兼容）。
func SimpleTack() string {
	return callerInfo(2)
}

// Stack 将当前协程的完整调用栈以 Error 级别写入日志。
func Stack() {
	buf := make([]byte, 1<<12)
	log(Error, string(buf[:runtime.Stack(buf, false)]))
}

// ---------------------------------------------------------------------------
// 日志核心投递
// ---------------------------------------------------------------------------

// log 是所有日志级别共用的内部投递函数。
// 调用链：外部调用者 -> DebugLog/InfoLog/... -> log
// 因此 runtime.Caller(2) 取到的是外部调用者的位置。
func log(lv logLv, v ...interface{}) {
	if lv < logNowLv {
		return
	}

	// 判断日志文件是否存在
	lvName, exists := logLvNameMap[lv]
	if !exists {
		return
	}

	// 记录调用来源：log(0) -> DebugLog等(1) -> 实际调用者(2)
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return
	}

	i := strings.LastIndex(file, "/") + 1
	logContent := fmt.Sprintf("[%s][%s][%s:%d]:", lvName, time.Now().Format("2006-01-02 15:04:05"), file[i:], line)
	if len(v) > 1 {
		logContent += fmt.Sprintf(v[0].(string), v[1:]...)
	} else {
		logContent += fmt.Sprint(v[0])
	}
	logContent += GetNewLineString()

	logCh <- &logObj{
		lv:      lv,
		logInfo: logContent,
	}
}

// ---------------------------------------------------------------------------
// 对外日志入口（保持原有 API 不变）
// ---------------------------------------------------------------------------

// DebugLog 输出 Debug 级别日志
func DebugLog(v ...interface{}) { log(Debug, v...) }

// InfoLog 输出 Info 级别日志
func InfoLog(v ...interface{}) { log(Info, v...) }

// WarnLog 输出 Warn 级别日志
func WarnLog(v ...interface{}) { log(Warn, v...) }

// ErrorLog 输出 Error 级别日志
func ErrorLog(v ...interface{}) { log(Error, v...) }

// FatalLog 输出 Fatal 级别日志
func FatalLog(v ...interface{}) { log(Fatal, v...) }
