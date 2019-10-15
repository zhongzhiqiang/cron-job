package handler

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zhongzhiqiang/cron/common/etcd"
	"github.com/zhongzhiqiang/cron/common/protocol"
)

type DelJob struct {
	Name string `json:name`
}

func SaveJob(ctx *gin.Context) {
	var (
		job    protocol.Job
		oldJob protocol.Job
		err    error
	)
	err = ctx.BindJSON(&job)
	if err != nil {
		ctx.JSON(400, gin.H{"error": err.Error()})
		return
	}

	fmt.Println(job)
	if oldJob, err = etcd.G_JobMgr.SaveJob(&job); err != nil {
		ctx.JSON(500, "")
		return
	}
	ctx.JSON(200, oldJob)
}

func DeleteJob(c *gin.Context) {
	// TODO 删除任务
	var (
		deleteJob *DelJob
		err       error
		oldJob    protocol.Job
	)
	deleteJob = &DelJob{}
	if err = c.BindJSON(&deleteJob); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if oldJob, err = etcd.G_JobMgr.DeleteJob(deleteJob.Name); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, oldJob)

}

func ListAllJob(c *gin.Context) {
	// 返回所有的任务
	var (
		listJob []protocol.Job
		err     error
	)
	if listJob, err = etcd.G_JobMgr.ListAllJob(); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, listJob)
}

func KillJob(c *gin.Context) {
	// 删除所有任务
	var (
		err     error
		killJob DelJob
	)

	if err = c.Bind(killJob); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err = etcd.G_JobMgr.KillJob(killJob.Name); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(204, gin.H{"msg": "success"})
}
