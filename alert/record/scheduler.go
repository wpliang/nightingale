package record

import (
	"context"
	"fmt"
	"time"

	"github.com/ccfos/nightingale/v6/alert/aconf"
	"github.com/ccfos/nightingale/v6/alert/astats"
	"github.com/ccfos/nightingale/v6/alert/naming"
	"github.com/ccfos/nightingale/v6/memsto"
	"github.com/ccfos/nightingale/v6/prom"
	"github.com/ccfos/nightingale/v6/pushgw/writer"
)

type Scheduler struct {
	// key: hash
	recordRules map[string]*RecordRuleContext

	aconf aconf.Alert

	recordingRuleCache *memsto.RecordingRuleCacheType

	promClients *prom.PromClientMap
	writers     *writer.WritersType

	stats *astats.Stats
}

func NewScheduler(aconf aconf.Alert, rrc *memsto.RecordingRuleCacheType, promClients *prom.PromClientMap, writers *writer.WritersType, stats *astats.Stats) *Scheduler {
	scheduler := &Scheduler{
		aconf:       aconf,
		recordRules: make(map[string]*RecordRuleContext),

		recordingRuleCache: rrc,

		promClients: promClients,
		writers:     writers,

		stats: stats,
	}

	go scheduler.LoopSyncRules(context.Background())
	return scheduler
}

func (s *Scheduler) LoopSyncRules(ctx context.Context) {
	time.Sleep(time.Duration(s.aconf.EngineDelay) * time.Second)
	duration := 9000 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(duration):
			s.syncRecordRules()
		}
	}
}

func (s *Scheduler) syncRecordRules() {
	ids := s.recordingRuleCache.GetRuleIds()
	recordRules := make(map[string]*RecordRuleContext)
	for _, id := range ids {
		rule := s.recordingRuleCache.Get(id)
		if rule == nil {
			continue
		}
		// 获取有效的数据源，record规则里可以设置关联数据源，如果是all，则需把所有数据源找出来
		datasourceIds := s.promClients.Hit(rule.DatasourceIdsJson)
		for _, dsId := range datasourceIds {
			// 根据规则ID来判断当前N9E服务是否要处理当前规则
			// 会根据规则ID的HASH值来选择N9E服务，不同的规则会被不同的N9E服务处理，从而达到多个N9E服务来处理不同的规则，且不会重复处理
			// 注意：dsId=0没有实际N9E服务，这里会被跳过
			if !naming.DatasourceHashRing.IsHit(dsId, fmt.Sprintf("%d", rule.Id), s.aconf.Heartbeat.Endpoint) {
				continue
			}

			recordRule := NewRecordRuleContext(rule, dsId, s.promClients, s.writers)
			recordRules[recordRule.Hash()] = recordRule
		}
	}

	// 如果当前规则之前没开始过，则开始处理
	for hash, rule := range recordRules {
		if _, has := s.recordRules[hash]; !has {
			rule.Prepare()
			rule.Start()
			s.recordRules[hash] = rule
		}
	}

	// 如果当前规则之前有，现在没有了，则停止处理
	for hash, rule := range s.recordRules {
		if _, has := recordRules[hash]; !has {
			rule.Stop()
			delete(s.recordRules, hash)
		}
	}
}
