package protocol

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"time"
)

type Job struct {
	JobName  string `json:"job_name"`
	Command  string `json:"command"`
	CronExpr string `json:"cron_expr"`
}

type JobSchedulePlan struct {
	Job      *Job
	Expr     *cronexpr.Expression
	NextTime time.Time
}

type JobEvent struct {
	EventType int
	Job       *Job
}

type JobExecuteInfo struct {
	Job        *Job
	PlanTime   time.Time
	RealTime   time.Time
	CancelCtx  context.Context
	CancelFunc context.CancelFunc
}
type JobExecuteResultInfo struct {
	ExecuteInfo *JobExecuteInfo
	OutPut      []byte
	Err         error
	StartTime   time.Time
	EndTime     time.Time
}

// 任务执行日志
type JobLog struct {
	JobName string `json:"jobName" bson:"jobName"` // 任务名字
	Command string `json:"command" bson:"command"` // 脚本命令
	Err string `json:"err" bson:"err"` // 错误原因
	Output string `json:"output" bson:"output"`	// 脚本输出
	PlanTime int64 `json:"planTime" bson:"planTime"` // 计划开始时间
	ScheduleTime int64 `json:"scheduleTime" bson:"scheduleTime"` // 实际调度时间
	StartTime int64 `json:"startTime" bson:"startTime"` // 任务执行开始时间
	EndTime int64 `json:"endTime" bson:"endTime"` // 任务执行结束时间
}

type LogBatch struct {
	Logs []interface{}	// 多条日志
}

func NewJob(JobName, Command, CronExpr string) *Job {
	return &Job{JobName: JobName, Command: Command, CronExpr: CronExpr}
}

func UnpackJob(value []byte) (job *Job, err error) {
	job = &Job{}
	err = json.Unmarshal(value, job)
	return
}

func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{EventType: eventType, Job: job}
}

func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {
	var (
		expr *cronexpr.Expression
	)
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}

	jobSchedulePlan = &JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

func BuildJobExecuteInfo(plan *JobSchedulePlan) (jobExecuteInfo *JobExecuteInfo) {
	jobExecuteInfo = &JobExecuteInfo{
		Job:      plan.Job,
		PlanTime: plan.NextTime,
		RealTime: time.Now(),
	}
	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}
