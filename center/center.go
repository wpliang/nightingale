package center

import (
	"context"
	"fmt"

	"github.com/ccfos/nightingale/v6/alert"
	"github.com/ccfos/nightingale/v6/alert/astats"
	"github.com/ccfos/nightingale/v6/alert/process"
	"github.com/ccfos/nightingale/v6/center/cconf"
	"github.com/ccfos/nightingale/v6/center/metas"
	"github.com/ccfos/nightingale/v6/center/sso"
	"github.com/ccfos/nightingale/v6/conf"
	"github.com/ccfos/nightingale/v6/memsto"
	"github.com/ccfos/nightingale/v6/models"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
	"github.com/ccfos/nightingale/v6/pkg/httpx"
	"github.com/ccfos/nightingale/v6/pkg/i18nx"
	"github.com/ccfos/nightingale/v6/pkg/logx"
	"github.com/ccfos/nightingale/v6/prom"
	"github.com/ccfos/nightingale/v6/pushgw/idents"
	"github.com/ccfos/nightingale/v6/pushgw/writer"
	"github.com/ccfos/nightingale/v6/storage"

	alertrt "github.com/ccfos/nightingale/v6/alert/router"
	centerrt "github.com/ccfos/nightingale/v6/center/router"
	pushgwrt "github.com/ccfos/nightingale/v6/pushgw/router"
)

func Initialize(configDir string, cryptoKey string) (func(), error) {
	// 初始化配置文件
	config, err := conf.InitConfig(configDir, cryptoKey)
	if err != nil {
		return nil, fmt.Errorf("failed to init config: %v", err)
	}

	// 加载指标的说明
	cconf.LoadMetricsYaml(config.Center.MetricsYamlFile)
	// 加载菜单信息
	cconf.LoadOpsYaml(config.Center.OpsYamlFile)
	// 初始化日志配置
	logxClean, err := logx.Init(config.Log)
	if err != nil {
		return nil, err
	}

	// 初始化国际化错误提示
	i18nx.Init()

	// 初始化数据库实例
	db, err := storage.New(config.DB)
	if err != nil {
		return nil, err
	}
	// db包装到context中
	ctx := ctx.NewContext(context.Background(), db)
	// 初始化root用户密码
	models.InitRoot(ctx)

	// 初始化Redis客户端
	redis, err := storage.NewRedis(config.Redis)
	if err != nil {
		return nil, err
	}

	// 主机信息保存到redis中
	// 主机信息来自于心跳请求，如categraf里配置心跳请求 http://127.0.0.1:17000/v1/n9e/heartbeat
	metas := metas.New(redis)
	// 服务器信息保存到数据库target表中，更新时间，后面根据更新时间判断服务是否存活
	// 服务信息来自于指标上报的数据，从指标数据的标签中解析出ident， 如categraf的agent_hostname，telegraf的host
	idents := idents.New(db)

	// 统计信息
	syncStats := memsto.NewSyncStats()
	alertStats := astats.NewSyncStats()

	// 单点登录初始化
	sso := sso.Init(config.Center, ctx)

	// 缓存一些常用的数据
	// 业务组信息
	busiGroupCache := memsto.NewBusiGroupCache(ctx, syncStats)
	// 服务器信息和主机信息合并
	targetCache := memsto.NewTargetCache(ctx, syncStats, redis)
	// 数据源信息
	dsCache := memsto.NewDatasourceCache(ctx, syncStats)
	// 屏蔽规则信息，按业务组分类保存
	// 一个屏蔽规则只能适用在一个业务组 【这里是否可以扩展，可配置适用所有的业务组的屏蔽规则】
	alertMuteCache := memsto.NewAlertMuteCache(ctx, syncStats)
	// 告警规则信息
	alertRuleCache := memsto.NewAlertRuleCache(ctx, syncStats)
	// 通知配置信息
	notifyConfigCache := memsto.NewNotifyConfigCache(ctx)

	// 初始化Prom的读写客户端
	promClients := prom.NewPromClient(ctx, config.Alert.Heartbeat)

	externalProcessors := process.NewExternalProcessors()
	// 开启告警服务
	alert.Start(config.Alert, config.Pushgw, syncStats, alertStats, externalProcessors, targetCache, busiGroupCache, alertMuteCache, alertRuleCache, notifyConfigCache, dsCache, ctx, promClients, true)

	// 初始化push-gateway，用于指标接收并发送到时序数据库里
	writers := writer.NewWriters(config.Pushgw)

	// 创建三个模块的路由
	alertrtRouter := alertrt.New(config.HTTP, config.Alert, alertMuteCache, targetCache, busiGroupCache, alertStats, ctx, externalProcessors)
	centerRouter := centerrt.New(config.HTTP, config.Center, cconf.Operations, dsCache, notifyConfigCache, promClients, redis, sso, ctx, metas, targetCache)
	pushgwRouter := pushgwrt.New(config.HTTP, config.Pushgw, targetCache, busiGroupCache, idents, writers, ctx)

	// 初始化Gin实例
	r := httpx.GinEngine(config.Global.RunMode, config.HTTP)
	// 路由配置到Gin实例中
	centerRouter.Config(r)
	alertrtRouter.Config(r)
	pushgwRouter.Config(r)
	// 开始http服务
	httpClean := httpx.Init(config.HTTP, r)
	// 返回清理函数
	return func() {
		logxClean()
		httpClean()
	}, nil
}
