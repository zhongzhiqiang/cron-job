package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/zhongzhiqiang/cron-job/common/constant"
	"github.com/zhongzhiqiang/cron-job/common/protocol"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_JobMgr *JobMgr
)

func InitEtcd() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
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
	G_JobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}

func (jobMgr *JobMgr) SaveJob(job *protocol.Job) (oldJob protocol.Job, err error) {
	var (
		jobKey      string
		jobData     []byte
		putResponse *clientv3.PutResponse
	)

	// 1.获取存储的路径
	jobKey = constant.SAVE_JOB_DIR + job.JobName

	// 2.序列化值
	if jobData, err = json.Marshal(job); err != nil {
		return
	}
	fmt.Println(jobKey, string(jobData))
	// 3.保存数据
	if putResponse, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobData), clientv3.WithPrevKV()); err != nil {
		return
	}

	// 4.如果是更新，返回老的数据
	fmt.Println(putResponse.PrevKv)
	if putResponse.PrevKv != nil {
		if err = json.Unmarshal(putResponse.PrevKv.Value, &oldJob); err != nil {
			return
		}
	}
	return
}

func (jobMgr *JobMgr) DeleteJob(jobName string) (oldJob protocol.Job, err error) {
	var (
		jobKey  string
		delResp *clientv3.DeleteResponse
	)
	jobKey = constant.SAVE_JOB_DIR + jobName

	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}
	if len(delResp.PrevKvs) != 0 {
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJob); err != nil {
			return
		}
	}
	return
}

func (jobMgr *JobMgr) KillJob(jobName string) (err error) {
	var (
		jobKey    string
		leaseResp *clientv3.LeaseGrantResponse
	)

	jobKey = constant.KILL_JOB_DIR + jobName

	if leaseResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	if _, err = jobMgr.kv.Put(context.TODO(), jobKey, "", clientv3.WithLease(leaseResp.ID)); err != nil {
		return
	}

	return
}

func (jobMgr *JobMgr) ListAllJob() (jobList []protocol.Job, err error) {
	var (
		getResp  *clientv3.GetResponse
		keyValue *mvccpb.KeyValue
		job      protocol.Job
	)
	if getResp, err = jobMgr.kv.Get(context.TODO(), constant.SAVE_JOB_DIR, clientv3.WithPrefix()); err != nil {
		return
	}
	for _, keyValue = range getResp.Kvs {
		job = protocol.Job{}
		if err = json.Unmarshal(keyValue.Value, &job); err != nil {
			return
		}
		jobList = append(jobList, job)
	}
	return
}
