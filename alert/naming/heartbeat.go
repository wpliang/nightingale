package naming

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ccfos/nightingale/v6/alert/aconf"
	"github.com/ccfos/nightingale/v6/models"
	"github.com/ccfos/nightingale/v6/pkg/ctx"

	"github.com/toolkits/pkg/logger"
)

type Naming struct {
	ctx             *ctx.Context
	heartbeatConfig aconf.HeartbeatConfig
	isCenter        bool
}

func NewNaming(ctx *ctx.Context, heartbeat aconf.HeartbeatConfig, isCenter bool) *Naming {
	naming := &Naming{
		ctx:             ctx,
		heartbeatConfig: heartbeat,
		isCenter:        isCenter,
	}
	naming.Heartbeats()
	return naming
}

// local servers
var localss map[int64]string

func (n *Naming) Heartbeats() error {
	localss = make(map[int64]string)
	// 启动时先进行一次心跳处理
	if err := n.heartbeat(); err != nil {
		fmt.Println("failed to heartbeat:", err)
		return err
	}

	// 定时进行心跳处理
	go n.loopHeartbeat()
	// 定时删除不活跃实例
	go n.loopDeleteInactiveInstances()
	return nil
}

func (n *Naming) loopDeleteInactiveInstances() {
	interval := time.Duration(10) * time.Minute
	for {
		time.Sleep(interval)
		// 删除不活跃的N9E服务
		n.DeleteInactiveInstances()
	}
}

func (n *Naming) DeleteInactiveInstances() {
	err := models.DB(n.ctx).Where("clock < ?", time.Now().Unix()-600).Delete(new(models.AlertingEngines)).Error
	if err != nil {
		logger.Errorf("delete inactive instances err:%v", err)
	}
}

func (n *Naming) loopHeartbeat() {
	interval := time.Duration(n.heartbeatConfig.Interval) * time.Millisecond
	for {
		time.Sleep(interval)
		if err := n.heartbeat(); err != nil {
			logger.Warning(err)
		}
	}
}

func (n *Naming) heartbeat() error {
	var datasourceIds []int64
	var err error

	// 通过EngineName【告警引擎集群】查询对应的数据源
	// 在创建数据源的时候会配置告警引擎集群
	// 在页面上维护实例和集群的对应关系
	datasourceIds, err = models.GetDatasourceIdsByEngineName(n.ctx, n.heartbeatConfig.EngineName)
	if err != nil {
		return err
	}

	if len(datasourceIds) == 0 {
		// 如果没有配置数据源，数据源ID保存成0，
		// 这样就有了N9E服务和告警引擎集群信息，在配置数据源时就可以选择了，然后第二次心跳时，上面就能查询到数据源ID了
		err := models.AlertingEngineHeartbeatWithCluster(n.ctx, n.heartbeatConfig.Endpoint, n.heartbeatConfig.EngineName, 0)
		if err != nil {
			logger.Warningf("heartbeat with cluster %s err:%v", "", err)
		}
	} else {
		// 如果配置了数据源，为每个数据源设置告警引擎集群和N9E服务的关系
		for i := 0; i < len(datasourceIds); i++ {
			err := models.AlertingEngineHeartbeatWithCluster(n.ctx, n.heartbeatConfig.Endpoint, n.heartbeatConfig.EngineName, datasourceIds[i])
			if err != nil {
				logger.Warningf("heartbeat with cluster %d err:%v", datasourceIds[i], err)
			}
		}
	}

	for i := 0; i < len(datasourceIds); i++ {
		// 查询所有活跃的N9E服务
		servers, err := n.ActiveServers(datasourceIds[i])
		if err != nil {
			logger.Warningf("hearbeat %d get active server err:%v", datasourceIds[i], err)
			continue
		}

		sort.Strings(servers)
		newss := strings.Join(servers, " ")

		oldss, exists := localss[datasourceIds[i]]
		if exists && oldss == newss {
			continue
		}

		// 把所有活跃服务放到一致性hash环里
		RebuildConsistentHashRing(datasourceIds[i], servers)
		localss[datasourceIds[i]] = newss
	}

	// 如果时中心节点，不是alert单独部署的
	if n.isCenter {
		// 如果是中心节点，还需要处理 host 类型的告警规则，host 类型告警规则，和数据源无关，想复用下数据源的 hash ring，想用一个虚假的数据源 id 来处理
		// if is center node, we need to handle host type alerting rules, host type alerting rules are not related to datasource, we want to reuse the hash ring of datasource, we want to use a fake datasource id to handle it
		err := models.AlertingEngineHeartbeatWithCluster(n.ctx, n.heartbeatConfig.Endpoint, n.heartbeatConfig.EngineName, HostDatasource)
		if err != nil {
			logger.Warningf("heartbeat with cluster %s err:%v", "", err)
		}

		servers, err := n.ActiveServers(HostDatasource)
		if err != nil {
			logger.Warningf("hearbeat %d get active server err:%v", HostDatasource, err)
			return nil
		}

		sort.Strings(servers)
		newss := strings.Join(servers, " ")

		oldss, exists := localss[HostDatasource]
		if exists && oldss == newss {
			return nil
		}

		RebuildConsistentHashRing(HostDatasource, servers)
		localss[HostDatasource] = newss
	}

	return nil
}

func (n *Naming) ActiveServers(datasourceId int64) ([]string, error) {
	if datasourceId == -1 {
		return nil, fmt.Errorf("cluster is empty")
	}

	// 30秒内有心跳，就认为是活的
	return models.AlertingEngineGetsInstances(n.ctx, "datasource_id = ? and clock > ?", datasourceId, time.Now().Unix()-30)
}
