package main

import (
	"fmt"
	"github.com/zhongzhiqiang/cron/work"
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


	for {
		time.Sleep(1)
	}
}
