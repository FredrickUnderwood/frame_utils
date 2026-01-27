package frame_utils

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/FredrickUnderwood/frame_utils/constants"
	"github.com/bytedance/sonic"
)

const (
	LogBufferSize = 4096
	TraceKey      = "trace_id"
)

var (
	logDir      string
	logBaseName string

	currentFile *os.File
	currentHour string

	wg      sync.WaitGroup
	logChan chan *BaseLog
)

type Source struct {
	Function string `json:"function"`
	File     string `json:"file"`
	Line     int    `json:"line"`
}

type BaseLog struct {
	Time    time.Time `json:"time"`
	Source  *Source   `json:"source"`
	Level   string    `json:"level"`
	Msg     string    `json:"msg"`
	TraceId string    `json:"traceId"`
}

func InitLogger(dir, baseName string) {
	logDir = dir
	logBaseName = baseName

	if err := os.MkdirAll(dir, 0777); err != nil {
		log.Fatalf("[InitLogger] mkdir fail, err: %v", err)
	}

	logChan = make(chan *BaseLog, LogBufferSize)
	wg.Add(1)
	go logWorker()
}

func CloseLogger() {
	if logChan != nil {
		close(logChan)
		wg.Wait()
	}
}

func logWorker() {
	defer wg.Done()

	for baseLog := range logChan {
		file, err := getFile(baseLog.Time)
		if err != nil {
			log.Printf("[outputLog] get file fail, err: %v", err)
		}
		logBytes, _ := sonic.Marshal(baseLog)
		logBytes = append(logBytes, '\n')
		_, _ = file.Write(logBytes)
	}

	if currentFile != nil {
		_ = currentFile.Close()
	}
}

func getFile(now time.Time) (*os.File, error) {
	hourKey := now.Format(constants.SimpleDateHourLayout)

	if currentFile != nil && currentHour == hourKey {
		return currentFile, nil
	}

	if currentFile != nil {
		_ = currentFile.Close()
	}

	fileName := filepath.Join(logDir, fmt.Sprintf("%s-%s.log", logBaseName, hourKey))
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	currentFile = f
	currentHour = hourKey
	return f, nil
}

func getLogSource() *Source {
	pc, file, line, ok := runtime.Caller(2)
	if !ok {
		return &Source{
			Function: "unknown",
			File:     "unknown",
			Line:     0,
		}
	}
	funcName := "unknown"
	if fn := runtime.FuncForPC(pc); fn != nil {
		funcName = fn.Name()
	}
	return &Source{
		Function: funcName,
		File:     file,
		Line:     line,
	}
}

func outputLog(ctx context.Context, baseLog *BaseLog) {
	logStr, _ := sonic.MarshalString(baseLog)

	log.Printf("[%s]%s\n", baseLog.Level, logStr)
	select {
	case logChan <- baseLog:
	default:
	}
}

func CtxInfo(ctx context.Context, format string, args ...any) {
	traceId := ctx.Value(TraceKey).(string)
	baseLog := &BaseLog{
		Time:    time.Now(),
		Source:  getLogSource(),
		Level:   "info",
		Msg:     fmt.Sprintf(format, args...),
		TraceId: traceId,
	}
	outputLog(ctx, baseLog)
}

func CtxWarn(ctx context.Context, format string, args ...any) {
	traceId := ctx.Value(TraceKey).(string)
	baseLog := &BaseLog{
		Time:    time.Now(),
		Source:  getLogSource(),
		Level:   "warn",
		Msg:     fmt.Sprintf(format, args...),
		TraceId: traceId,
	}
	outputLog(ctx, baseLog)
}

func CtxError(ctx context.Context, format string, args ...any) {
	traceId := ctx.Value(TraceKey).(string)
	baseLog := &BaseLog{
		Time:    time.Now(),
		Source:  getLogSource(),
		Level:   "error",
		Msg:     fmt.Sprintf(format, args...),
		TraceId: traceId,
	}
	outputLog(ctx, baseLog)
}
