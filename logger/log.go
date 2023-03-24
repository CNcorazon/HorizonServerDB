package logger

import (
	"log"
	"os"
)

var BlockLogger = initBlockLogger()
var StateLogger = initStateLogger()
var ShardLogger = initShardLogger()
var AnalysisLogger = initAnalysisLogger()

func initBlockLogger() *log.Logger {
	logFile, _ := os.Create("block.log")
	loger := log.New(logFile, "[block]", log.LstdFlags|log.Lshortfile|log.LUTC) // 将文件设置为loger作为输出
	return loger
}

func initStateLogger() *log.Logger {
	logFile, _ := os.Create("state.log")
	loger := log.New(logFile, "[state]", log.LstdFlags|log.Lshortfile|log.LUTC) // 将文件设置为loger作为输出
	return loger
}

func initShardLogger() *log.Logger {
	logFile, _ := os.Create("shard.log")
	loger := log.New(logFile, "[shard]", log.LstdFlags|log.Lshortfile|log.LUTC) // 将文件设置为loger作为输出
	return loger
}

func initAnalysisLogger() *log.Logger {
	logFile, _ := os.Create("analysis.log")
	loger := log.New(logFile, "[shard]", log.LstdFlags|log.Lshortfile|log.LUTC) // 将文件设置为loger作为输出
	return loger
}
