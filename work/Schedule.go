package work

import (
	"fmt"
	"github.com/zhongzhiqiang/cron/common/constant"
	"github.com/zhongzhiqiang/cron/common/protocol"
	"time"
)

type Schedule struct {
	jobEventChan      chan *protocol.JobEvent
	jobPlanTable      map[string]*protocol.JobSchedulePlan
	jobExecutingTable map[string]*protocol.JobExecuteInfo
	jobResultChan     chan *protocol.JobExecuteResultInfo
}

var (
	G_Schedule *Schedule
)

func (schedule *Schedule) handleJobEvent(jobEvent *protocol.JobEvent) {
	var (
		err             error
		jobSchedulePlan *protocol.JobSchedulePlan
		jobExist        bool
	)
	switch jobEvent.EventType {
	case constant.JOB_EVENT_SAVE:
		if jobSchedulePlan, err = protocol.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		schedule.jobPlanTable[jobEvent.Job.JobName] = jobSchedulePlan
	case constant.JOB_EVENT_DEL:
		if jobSchedulePlan, jobExist = schedule.jobPlanTable[jobEvent.Job.JobName]; jobExist {
			delete(schedule.jobPlanTable, jobEvent.Job.JobName)
		}
	}
}

func (schedule *Schedule) TryStartJob(plan *protocol.JobSchedulePlan) {
	var (
		jobExecuteInfo *protocol.JobExecuteInfo
		jobExecuting   bool
	)

	if jobExecuteInfo, jobExecuting = schedule.jobExecutingTable[plan.Job.JobName]; jobExecuting {
		return
	}
	jobExecuteInfo = protocol.BuildJobExecuteInfo(plan)
	schedule.jobExecutingTable[plan.Job.JobName] = jobExecuteInfo
	fmt.Println("执行任务", jobExecuteInfo.Job.JobName, plan.NextTime)
	delete(schedule.jobExecutingTable, plan.Job.JobName)
	fmt.Println("任务结束", plan.Job.JobName)
}

func (schedule *Schedule) TrySchedule() (scheduleAfter time.Duration) {
	var (
		jobPlan  *protocol.JobSchedulePlan
		now      time.Time
		nearTime *time.Time
	)
	if len(schedule.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
	}
	now = time.Now()

	for _, jobPlan = range schedule.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			schedule.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now)
		}
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}
	if nearTime != nil {
		scheduleAfter = (*nearTime).Sub(now)
	}
	return
}

func (schedule *Schedule) PushJobEvent(jobEvent *protocol.JobEvent) {
	schedule.jobEventChan <- jobEvent
}

func (schedule *Schedule) scheduleLoop() {
	var (
		scheduleAfter time.Duration
		jobEvent      *protocol.JobEvent
		scheduleTimer *time.Timer
		jobResult     *protocol.JobExecuteResultInfo
	)
	scheduleAfter = schedule.TrySchedule()

	scheduleTimer = time.NewTimer(scheduleAfter)

	for {
		select {
		case jobEvent = <-schedule.jobEventChan:
			schedule.handleJobEvent(jobEvent)
		case <-scheduleTimer.C:
		case jobResult = <- schedule.jobResultChan:
			schedule.HandleResult(jobResult)
		}
		scheduleAfter = schedule.TrySchedule()
		scheduleTimer.Reset(scheduleAfter)
	}

}

func (schedule *Schedule) HandleResult(jobResult *protocol.JobExecuteResultInfo) {

}

func (schedule *Schedule) PushResult(jobResult *protocol.JobExecuteResultInfo) {
	schedule.jobResultChan <- jobResult
}
func InitSchedule() (err error) {
	G_Schedule = &Schedule{
		jobEventChan:      make(chan *protocol.JobEvent, 1000),
		jobPlanTable:      make(map[string]*protocol.JobSchedulePlan),
		jobExecutingTable: make(map[string]*protocol.JobExecuteInfo),
	}
	go G_Schedule.scheduleLoop()
	return
}
