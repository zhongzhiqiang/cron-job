package work

import (
	"context"
	"fmt"
	"github.com/zhongzhiqiang/cron-job/common/constant"
	"github.com/zhongzhiqiang/cron-job/common/etcd"
	"github.com/zhongzhiqiang/cron-job/common/protocol"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	G_JobMgr *JobMgr
)

func InitEtcd() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)
	config = clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	}
	if client, err = clientv3.New(config); err != nil {
		return
	}
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)
	G_JobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}
	G_JobMgr.WatchJobs()
	G_JobMgr.WatchKillJobs()
	return
}

func (jobMgr *JobMgr) WatchJobs() (err error) {
	var (
		getResp      *clientv3.GetResponse
		kvpair       *mvccpb.KeyValue
		job          *protocol.Job
		jobEvent     *protocol.JobEvent
		watchVersion int64
		watchChan    clientv3.WatchChan
		watchResp    clientv3.WatchResponse
		watchEvent   *clientv3.Event
	)
	if getResp, err = jobMgr.kv.Get(context.TODO(), constant.SAVE_JOB_DIR, clientv3.WithPrefix()); err != nil {
		return
	}
	for _, kvpair = range getResp.Kvs {
		if job, err = protocol.UnpackJob(kvpair.Value); err != nil {
			return
		}
		jobEvent = protocol.BuildJobEvent(constant.JOB_EVENT_SAVE, job)
		fmt.Println("find job Event", jobEvent)
		G_Schedule.PushJobEvent(jobEvent)
	}

	go func() {
		watchVersion = getResp.Header.Revision + 1

		watchChan = jobMgr.watcher.Watch(context.TODO(), constant.SAVE_JOB_DIR,
			clientv3.WithRev(watchVersion), clientv3.WithPrefix())

		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					if job, err = protocol.UnpackJob(watchEvent.Kv.Value); err != nil {
						continue
					}
					jobEvent = protocol.BuildJobEvent(constant.JOB_EVENT_SAVE, job)
				case mvccpb.DELETE:
					if job, err = protocol.UnpackJob(watchEvent.Kv.Value); err != nil {
						continue
					}
					jobEvent = protocol.BuildJobEvent(constant.JOB_EVENT_DEL, job)
				}
				// TODO 将事件推送给调度器
				G_Schedule.PushJobEvent(jobEvent)
			}
		}
	}()

	return
}

func (jobMgr *JobMgr) WatchKillJobs() (err error) {
	var (
		watchChan  clientv3.WatchChan
		watchResp  clientv3.WatchResponse
		watchEvent *clientv3.Event
		job        *protocol.Job
		jobEvent   *protocol.JobEvent
	)
	go func() {
		watchChan = jobMgr.watcher.Watch(context.TODO(), constant.KILL_JOB_DIR, clientv3.WithPrefix())
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					if job, err = protocol.UnpackJob(watchEvent.Kv.Value); err != nil {
						continue
					}
					jobEvent = protocol.BuildJobEvent(constant.JOB_EVENT_KILL, job)
					// TODO 推送给调度器
					G_Schedule.PushJobEvent(jobEvent)
				}
			}
		}
	}()
	return
}

func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *etcd.JobLock) {
	jobLock = etcd.InitJObLock(jobName, jobMgr.kv, jobMgr.lease)
	return
}
