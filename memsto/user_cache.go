package memsto

import (
	"fmt"
	"sync"
	"time"

	"github.com/ccfos/nightingale/v6/models"
	"github.com/ccfos/nightingale/v6/pkg/ctx"

	"github.com/pkg/errors"
	"github.com/toolkits/pkg/logger"
)

type UserCacheType struct {
	statTotal       int64
	statLastUpdated int64
	ctx             *ctx.Context
	stats           *Stats

	sync.RWMutex
	users map[int64]*models.User // key: id
}

func NewUserCache(ctx *ctx.Context, stats *Stats) *UserCacheType {
	uc := &UserCacheType{
		statTotal:       -1,
		statLastUpdated: -1,
		ctx:             ctx,
		stats:           stats,
		users:           make(map[int64]*models.User),
	}
	// 同步用户信息到缓存里
	uc.SyncUsers()
	return uc
}

func (uc *UserCacheType) StatChanged(total, lastUpdated int64) bool {
	if uc.statTotal == total && uc.statLastUpdated == lastUpdated {
		return false
	}

	return true
}

func (uc *UserCacheType) Set(m map[int64]*models.User, total, lastUpdated int64) {
	// 用户列表会被多个协程使用，加锁
	uc.Lock()
	uc.users = m
	uc.Unlock()

	// 这两个字段只有同步用户一个协程使用，无需加锁
	// only one goroutine used, so no need lock
	uc.statTotal = total
	uc.statLastUpdated = lastUpdated
}

func (uc *UserCacheType) GetByUserId(id int64) *models.User {
	uc.RLock()
	defer uc.RUnlock()
	return uc.users[id]
}

func (uc *UserCacheType) GetByUserIds(ids []int64) []*models.User {
	set := make(map[int64]struct{})

	uc.RLock()
	defer uc.RUnlock()

	var users []*models.User
	for _, id := range ids {
		if uc.users[id] == nil {
			continue
		}

		if _, has := set[id]; has {
			continue
		}

		users = append(users, uc.users[id])
		set[id] = struct{}{}
	}

	if users == nil {
		users = []*models.User{}
	}

	return users
}

func (uc *UserCacheType) GetMaintainerUsers() []*models.User {
	uc.RLock()
	defer uc.RUnlock()

	var users []*models.User
	for _, v := range uc.users {
		if v.Maintainer == 1 {
			users = append(users, v)
		}
	}

	if users == nil {
		users = []*models.User{}
	}

	return users
}

func (uc *UserCacheType) SyncUsers() {
	// 创建缓存时，同步所有的用户
	err := uc.syncUsers()
	if err != nil {
		fmt.Println("failed to sync users:", err)
		exit(1)
	}

	// 开启协程，定时同步用户
	go uc.loopSyncUsers()
}

func (uc *UserCacheType) loopSyncUsers() {
	duration := time.Duration(9000) * time.Millisecond
	for {
		time.Sleep(duration)
		if err := uc.syncUsers(); err != nil {
			logger.Warning("failed to sync users:", err)
		}
	}
}

func (uc *UserCacheType) syncUsers() error {
	start := time.Now()
	// 获取用户的total和last_updated信息
	stat, err := models.UserStatistics(uc.ctx)
	if err != nil {
		return errors.WithMessage(err, "failed to exec UserStatistics")
	}

	// 根据总行数和最后修改时间来判断是否修改过
	if !uc.StatChanged(stat.Total, stat.LastUpdated) {
		uc.stats.GaugeCronDuration.WithLabelValues("sync_users").Set(0)
		uc.stats.GaugeSyncNumber.WithLabelValues("sync_users").Set(0)

		logger.Debug("users not changed")
		return nil
	}

	// 从数据库里查询出所有的用户
	lst, err := models.UserGetAll(uc.ctx)
	if err != nil {
		return errors.WithMessage(err, "failed to exec UserGetAll")
	}

	m := make(map[int64]*models.User)
	for i := 0; i < len(lst); i++ {
		m[lst[i].Id] = lst[i]
	}
	// 保存到缓存里
	uc.Set(m, stat.Total, stat.LastUpdated)

	ms := time.Since(start).Milliseconds()
	uc.stats.GaugeCronDuration.WithLabelValues("sync_users").Set(float64(ms))
	uc.stats.GaugeSyncNumber.WithLabelValues("sync_users").Set(float64(len(m)))

	logger.Infof("timer: sync users done, cost: %dms, number: %d", ms, len(m))

	return nil
}
