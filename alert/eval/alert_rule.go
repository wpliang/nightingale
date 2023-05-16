package eval

import (
	"context"
	"fmt"
	"time"

	"github.com/ccfos/nightingale/v6/alert/aconf"
	"github.com/ccfos/nightingale/v6/alert/astats"
	"github.com/ccfos/nightingale/v6/alert/naming"
	"github.com/ccfos/nightingale/v6/alert/process"
	"github.com/ccfos/nightingale/v6/memsto"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
	"github.com/ccfos/nightingale/v6/prom"
	"github.com/toolkits/pkg/logger"
)

type Scheduler struct {
	isCenter bool
	// key: hash
	alertRules map[string]*AlertRuleWorker

	ExternalProcessors *process.ExternalProcessorsType

	aconf aconf.Alert

	alertRuleCache  *memsto.AlertRuleCacheType
	targetCache     *memsto.TargetCacheType
	busiGroupCache  *memsto.BusiGroupCacheType
	alertMuteCache  *memsto.AlertMuteCacheType
	datasourceCache *memsto.DatasourceCacheType

	promClients *prom.PromClientMap

	naming *naming.Naming

	ctx   *ctx.Context
	stats *astats.Stats
}

func NewScheduler(isCenter bool, aconf aconf.Alert, externalProcessors *process.ExternalProcessorsType, arc *memsto.AlertRuleCacheType, targetCache *memsto.TargetCacheType,
	busiGroupCache *memsto.BusiGroupCacheType, alertMuteCache *memsto.AlertMuteCacheType, datasourceCache *memsto.DatasourceCacheType, promClients *prom.PromClientMap, naming *naming.Naming,
	ctx *ctx.Context, stats *astats.Stats) *Scheduler {
	scheduler := &Scheduler{
		isCenter:   isCenter,
		aconf:      aconf,
		alertRules: make(map[string]*AlertRuleWorker),

		ExternalProcessors: externalProcessors,

		alertRuleCache:  arc,
		targetCache:     targetCache,
		busiGroupCache:  busiGroupCache,
		alertMuteCache:  alertMuteCache,
		datasourceCache: datasourceCache,

		promClients: promClients,
		naming:      naming,

		ctx:   ctx,
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
			s.syncAlertRules()
		}
	}
}

func (s *Scheduler) syncAlertRules() {
	ids := s.alertRuleCache.GetRuleIds()
	alertRuleWorkers := make(map[string]*AlertRuleWorker)
	externalRuleWorkers := make(map[string]*process.Processor)
	for _, id := range ids {
		rule := s.alertRuleCache.Get(id)
		if rule == nil {
			continue
		}
		if rule.IsPrometheusRule() {
			// 获取有效的数据源，规则里可以设置关联数据源，如果是all，则需把所有数据源找出来
			datasourceIds := s.promClients.Hit(rule.DatasourceIdsJson)
			for _, dsId := range datasourceIds {
				// 根据规则ID来判断当前N9E服务是否要处理当前规则
				// 会根据规则ID的HASH值来选择N9E服务，不同的规则会被不同的N9E服务处理，从而达到多个N9E服务来处理不同的规则，且不会重复处理
				// 注意：dsId=0没有实际N9E服务，这里会被跳过
				if !naming.DatasourceHashRing.IsHit(dsId, fmt.Sprintf("%d", rule.Id), s.aconf.Heartbeat.Endpoint) {
					continue
				}
				ds := s.datasourceCache.GetById(dsId)
				if ds == nil {
					logger.Debugf("datasource %d not found", dsId)
					continue
				}

				if ds.Status != "enabled" {
					logger.Debugf("datasource %d status is %s", dsId, ds.Status)
					continue
				}
				// 初始化一个规则处理器，给下面的执行者调用的
				processor := process.NewProcessor(rule, dsId, s.alertRuleCache, s.targetCache, s.busiGroupCache, s.alertMuteCache, s.datasourceCache, s.promClients, s.ctx, s.stats)
				// 初始化规则的执行者
				alertRule := NewAlertRuleWorker(rule, dsId, processor, s.promClients, s.ctx)
				alertRuleWorkers[alertRule.Hash()] = alertRule
			}
		} else if rule.IsHostRule() && s.isCenter {
			// all host rule will be processed by center instance
			if !naming.DatasourceHashRing.IsHit(naming.HostDatasource, fmt.Sprintf("%d", rule.Id), s.aconf.Heartbeat.Endpoint) {
				continue
			}
			processor := process.NewProcessor(rule, 0, s.alertRuleCache, s.targetCache, s.busiGroupCache, s.alertMuteCache, s.datasourceCache, s.promClients, s.ctx, s.stats)
			alertRule := NewAlertRuleWorker(rule, 0, processor, s.promClients, s.ctx)
			alertRuleWorkers[alertRule.Hash()] = alertRule
		} else {
			// 如果 rule 不是通过 prometheus engine 来告警的，则创建为 externalRule
			// if rule is not processed by prometheus engine, create it as externalRule
			for _, dsId := range rule.DatasourceIdsJson {
				ds := s.datasourceCache.GetById(dsId)
				if ds == nil {
					logger.Debugf("datasource %d not found", dsId)
					continue
				}

				if ds.Status != "enabled" {
					logger.Debugf("datasource %d status is %s", dsId, ds.Status)
					continue
				}
				processor := process.NewProcessor(rule, dsId, s.alertRuleCache, s.targetCache, s.busiGroupCache, s.alertMuteCache, s.datasourceCache, s.promClients, s.ctx, s.stats)
				externalRuleWorkers[processor.Key()] = processor
			}
		}
	}

	// 如果当前规则之前没开始过，则开始处理
	for hash, rule := range alertRuleWorkers {
		if _, has := s.alertRules[hash]; !has {
			// 把规则的活跃告警事件载入
			rule.Prepare()
			// 开始执行规则
			rule.Start()
			s.alertRules[hash] = rule
		}
	}

	// 如果当前规则之前有，现在没有了，则停止处理
	for hash, rule := range s.alertRules {
		if _, has := alertRuleWorkers[hash]; !has {
			rule.Stop()
			delete(s.alertRules, hash)
		}
	}

	s.ExternalProcessors.ExternalLock.Lock()
	for key, processor := range externalRuleWorkers {
		if curProcessor, has := s.ExternalProcessors.Processors[key]; has {
			// rule存在,且hash一致,认为没有变更,这里可以根据需求单独实现一个关联数据更多的hash函数
			if processor.Hash() == curProcessor.Hash() {
				continue
			}
		}

		// 现有规则中没有rule以及有rule但hash不一致的场景，需要触发rule的update
		processor.RecoverAlertCurEventFromDb()
		s.ExternalProcessors.Processors[key] = processor
	}

	for key := range s.ExternalProcessors.Processors {
		if _, has := externalRuleWorkers[key]; !has {
			delete(s.ExternalProcessors.Processors, key)
		}
	}
	s.ExternalProcessors.ExternalLock.Unlock()
}
