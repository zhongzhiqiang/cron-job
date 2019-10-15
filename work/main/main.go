package main

import (
	"fmt"
	"github.com/zhongzhiqiang/cron-job/work"
	"time"
)

func main() {
	var (
		err error
	)
	if err = work.InitSchedule(); err != nil {
		fmt.Println("init schedule error:", err.Error())
		return
	}
	if err = work.InitEtcd(); err != nil {
		fmt.Println("init etcd error:", err.Error())
		return
	}
	if err = work.InitLogSink(); err != nil {
		fmt.Println("init log sink error:", err.Error())
		return
	}

	for {
		time.Sleep(1)
	}
}
