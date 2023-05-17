package alert

import (
	"context"
	"fmt"

	"github.com/ccfos/nightingale/v6/alert/aconf"
	"github.com/ccfos/nightingale/v6/alert/astats"
	"github.com/ccfos/nightingale/v6/alert/dispatch"
	"github.com/ccfos/nightingale/v6/alert/eval"
	"github.com/ccfos/nightingale/v6/alert/naming"
	"github.com/ccfos/nightingale/v6/alert/process"
	"github.com/ccfos/nightingale/v6/alert/queue"
	"github.com/ccfos/nightingale/v6/alert/record"
	"github.com/ccfos/nightingale/v6/alert/router"
	"github.com/ccfos/nightingale/v6/alert/sender"
	"github.com/ccfos/nightingale/v6/conf"
	"github.com/ccfos/nightingale/v6/memsto"
	"github.com/ccfos/nightingale/v6/models"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
	"github.com/ccfos/nightingale/v6/pkg/httpx"
	"github.com/ccfos/nightingale/v6/pkg/logx"
	"github.com/ccfos/nightingale/v6/prom"
	"github.com/ccfos/nightingale/v6/pushgw/pconf"
	"github.com/ccfos/nightingale/v6/pushgw/writer"
	"github.com/ccfos/nightingale/v6/storage"
)

func Initialize(configDir string, cryptoKey string) (func(), error) {
	config, err := conf.InitConfig(configDir, cryptoKey)
	if err != nil {
		return nil, fmt.Errorf("failed to init config: %v", err)
	}

	logxClean, err := logx.Init(config.Log)
	if err != nil {
		return nil, err
	}

	db, err := storage.New(config.DB)
	if err != nil {
		return nil, err
	}
	ctx := ctx.NewContext(context.Background(), db)

	redis, err := storage.NewRedis(config.Redis)
	if err != nil {
		return nil, err
	}

	syncStats := memsto.NewSyncStats()
	alertStats := astats.NewSyncStats()

	targetCache := memsto.NewTargetCache(ctx, syncStats, redis)
	busiGroupCache := memsto.NewBusiGroupCache(ctx, syncStats)
	alertMuteCache := memsto.NewAlertMuteCache(ctx, syncStats)
	alertRuleCache := memsto.NewAlertRuleCache(ctx, syncStats)
	notifyConfigCache := memsto.NewNotifyConfigCache(ctx)
	dsCache := memsto.NewDatasourceCache(ctx, syncStats)

	promClients := prom.NewPromClient(ctx, config.Alert.Heartbeat)

	externalProcessors := process.NewExternalProcessors()

	Start(config.Alert, config.Pushgw, syncStats, alertStats, externalProcessors, targetCache, busiGroupCache, alertMuteCache, alertRuleCache, notifyConfigCache, dsCache, ctx, promClients, false)

	r := httpx.GinEngine(config.Global.RunMode, config.HTTP)
	rt := router.New(config.HTTP, config.Alert, alertMuteCache, targetCache, busiGroupCache, alertStats, ctx, externalProcessors)
	rt.Config(r)

	httpClean := httpx.Init(config.HTTP, r)

	return func() {
		logxClean()
		httpClean()
	}, nil
}

func Start(alertc aconf.Alert, pushgwc pconf.Pushgw, syncStats *memsto.Stats, alertStats *astats.Stats, externalProcessors *process.ExternalProcessorsType, targetCache *memsto.TargetCacheType, busiGroupCache *memsto.BusiGroupCacheType,
	alertMuteCache *memsto.AlertMuteCacheType, alertRuleCache *memsto.AlertRuleCacheType, notifyConfigCache *memsto.NotifyConfigCacheType, datasourceCache *memsto.DatasourceCacheType, ctx *ctx.Context, promClients *prom.PromClientMap, isCenter bool) {
	// 用户信息缓存
	userCache := memsto.NewUserCache(ctx, syncStats)
	// 用户组信息缓存， 逻辑跟NewUserCache一样
	userGroupCache := memsto.NewUserGroupCache(ctx, syncStats)
	// 告警订阅信息缓存， 逻辑跟NewUserCache一样
	alertSubscribeCache := memsto.NewAlertSubscribeCache(ctx, syncStats)
	// 生成新的指标的规则信息缓存， 逻辑跟NewUserCache一样
	recordingRuleCache := memsto.NewRecordingRuleCache(ctx, syncStats)

	// 初始化通知配置，规则里的通知媒介列表，联系人里的更多联系方式列表，通知模版
	go models.InitNotifyConfig(ctx, alertc.Alerting.TemplatesDir)

	// 心跳处理，n9e服务自动发现，注册和清理，为啥叫naming？
	// 所有活跃的N9E服务都被保存到每个数据源的一致性HASH里了
	naming := naming.NewNaming(ctx, alertc.Heartbeat, isCenter)

	// 初始化指标写入器， 把接收到的指标发送给时序数据库
	writers := writer.NewWriters(pushgwc)
	// 生成新的指标的规则处理调度器
	record.NewScheduler(alertc, recordingRuleCache, promClients, writers, alertStats)
	// 告警规则处理调度器
	eval.NewScheduler(isCenter, alertc, externalProcessors, alertRuleCache, targetCache, busiGroupCache, alertMuteCache, datasourceCache, promClients, naming, ctx, alertStats)

	// 初始化一个通知分发实例
	dp := dispatch.NewDispatch(alertRuleCache, userCache, userGroupCache, alertSubscribeCache, targetCache, notifyConfigCache, alertc.Alerting, ctx)
	// 初始化一个告警消费实例
	consumer := dispatch.NewConsumer(alertc.Alerting, ctx, dp)

	// 加载通知模版 和 初始化通知发送者
	go dp.ReloadTpls()
	// 消费告警列表
	go consumer.LoopConsume()

	// EventQueue长度的指标信息
	go queue.ReportQueueSize(alertStats)
	// 初始化一个邮件发送实例
	go sender.StartEmailSender(notifyConfigCache.GetSMTP()) // todo
}
