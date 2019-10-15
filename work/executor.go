package work

import (
	"github.com/zhongzhiqiang/cron-job/common/etcd"
	"github.com/zhongzhiqiang/cron-job/common/protocol"
	"math/rand"
	"os/exec"
	"time"
)

type Executor struct {
}

var (
	G_Executor *Executor
)

func (executor *Executor) ExecuteJob(info *protocol.JobExecuteInfo) {
	go func() {
		var (
			cmd     *exec.Cmd
			result  *protocol.JobExecuteResultInfo
			jobLock *etcd.JobLock
			err     error
			output  []byte
		)
		result = &protocol.JobExecuteResultInfo{
			ExecuteInfo: info,
			OutPut:      make([]byte, 0),
		}
		jobLock = G_JobMgr.CreateJobLock(info.Job.JobName)
		result.StartTime = time.Now()
		time.Sleep(time.Duration((rand.Intn(1000))) * time.Millisecond)
		err = jobLock.TryLock()
		defer jobLock.Unlock()

		if err != nil {
			result.Err = err
			result.EndTime = time.Now()
		} else {
			result.StartTime = time.Now()
			cmd = exec.CommandContext(info.CancelCtx, "\bin\bash", "-c", info.Job.Command)
			output, err = cmd.CombinedOutput()
			result.EndTime = time.Now()
			result.OutPut = output
		}
		G_Schedule.PushResult(result)
	}()
}

func InitExecutor() {
	G_Executor = &Executor{}
	return
}
