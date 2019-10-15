package work

import (
	"context"
	"github.com/zhongzhiqiang/cron-job/common/protocol"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type LogSink struct {
	client        *mongo.Client
	logCollection *mongo.Collection
	logChan       chan *protocol.JobLog
	logBatch      chan *protocol.LogBatch
}

var (
	G_LogSink *LogSink
)

func (logSink *LogSink) Save(batch *protocol.LogBatch) {
	_, _ = logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

func (logSink *LogSink) writeLoop() {
	var (
		log          *protocol.JobLog
		logBatch     *protocol.LogBatch
		commitTimer  *time.Timer
		timeoutBatch *protocol.LogBatch
	)
	for {
		select {
		case log = <-logSink.logChan:
			if logBatch == nil {
				logBatch = &protocol.LogBatch{}
				commitTimer = time.AfterFunc(
					1*time.Second,
					func(logBatch *protocol.LogBatch) func() {
						return func() {
							logSink.logBatch <- logBatch
						}
					}(logBatch))
			}
			logBatch.Logs = append(logBatch.Logs, log)
			if len(logBatch.Logs) == 100 {
				logSink.Save(logBatch)
				logBatch = nil
				commitTimer.Stop()
			}
		case timeoutBatch = <-logSink.logBatch:
			if timeoutBatch != logBatch {
				continue
			}
			logSink.logBatch <- timeoutBatch
			commitTimer.Stop()
			logBatch = nil
		}
	}
}

func (logSink *LogSink) Append(log *protocol.JobLog) {
	logSink.logChan <- log
}

func InitLogSink() (err error) {
	var (
		urlOption *options.ClientOptions
		client    *mongo.Client
	)
	urlOption = options.Client().ApplyURI("")
	if client, err = mongo.NewClient(urlOption); err != nil {
		return
	}
	G_LogSink = &LogSink{
		client:        client,
		logCollection: client.Database("log").Collection("log"),
		logChan:       make(chan *protocol.JobLog, 100),
		logBatch:      make(chan *protocol.LogBatch, 100),
	}
	go G_LogSink.writeLoop()
	return
}
