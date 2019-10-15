package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zhongzhiqiang/cron/common/etcd"
	"github.com/zhongzhiqiang/cron/web/handler"
)

func main() {

	err := etcd.InitEtcd()
	if err != nil {
		fmt.Print(err.Error())
	}

	app := gin.New()

	app.POST("/save/job", handler.SaveJob)
	app.GET("/list/job", handler.ListAllJob)
	app.POST("/kill/job", handler.KillJob)


	if err = app.Run(":8080"); err != nil {
		fmt.Print("start serve error:", err.Error())
	}

}
