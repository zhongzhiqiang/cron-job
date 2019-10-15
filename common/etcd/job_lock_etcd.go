package etcd

import (
	"context"
	"github.com/zhongzhiqiang/cron/common"
	"github.com/zhongzhiqiang/cron/common/constant"
	"go.etcd.io/etcd/clientv3"
)

type JobLock struct {
	kv         clientv3.KV
	lease      clientv3.Lease
	jobName    string
	cancelFunc context.CancelFunc
	leaseId    clientv3.LeaseID
	isLocked   bool
}

func InitJObLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		kv:      kv,
		jobName: jobName,
		lease:   lease,
	}
	return
}

func (jobLock *JobLock) TryLock() (err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		cancelCtx      context.Context
		cancelFunc     context.CancelFunc
		keepRespChan   <-chan *clientv3.LeaseKeepAliveResponse
		txn            clientv3.Txn
		lockKey        string
		txnResp        *clientv3.TxnResponse
	)
	if leaseGrantResp, err = jobLock.lease.Grant(context.TODO(), 5); err != nil {
		return
	}
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())

	if keepRespChan, err = jobLock.lease.KeepAlive(cancelCtx, leaseGrantResp.ID); err != nil {
		goto FAIL
	}
	go func() {
		var (
			keepResp *clientv3.LeaseKeepAliveResponse
		)
		for {
			select {
			case keepResp = <-keepRespChan:
				if keepResp == nil {
					goto END
				}
			}
		}
	END:
	}()

	txn = jobLock.kv.Txn(context.TODO())

	lockKey = constant.LOCK_JOB_DIR + jobLock.jobName

	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseGrantResp.ID))).
		Else(clientv3.OpGet(lockKey))
	if txnResp, err = txn.Commit(); err != nil {
		goto FAIL
	}
	if !txnResp.Succeeded {
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}

	jobLock.leaseId = leaseGrantResp.ID
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true
	return
FAIL:
	cancelFunc()
	jobLock.lease.Revoke(context.TODO(), leaseGrantResp.ID)
	return
}

func (jobLock *JobLock) Unlock() {
	if jobLock.isLocked {
		jobLock.cancelFunc()
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseId)
	}
}
