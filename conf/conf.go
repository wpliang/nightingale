package conf

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/ccfos/nightingale/v6/alert/aconf"
	"github.com/ccfos/nightingale/v6/center/cconf"
	"github.com/ccfos/nightingale/v6/pkg/cfg"
	"github.com/ccfos/nightingale/v6/pkg/httpx"
	"github.com/ccfos/nightingale/v6/pkg/logx"
	"github.com/ccfos/nightingale/v6/pkg/ormx"
	"github.com/ccfos/nightingale/v6/pushgw/pconf"
	"github.com/ccfos/nightingale/v6/storage"
)

type ConfigType struct {
	Global GlobalConfig
	Log    logx.Config
	HTTP   httpx.Config
	DB     ormx.DBConfig
	Redis  storage.RedisConfig

	Pushgw pconf.Pushgw
	Alert  aconf.Alert
	Center cconf.Center
}

type GlobalConfig struct {
	RunMode string
}

// 加载配置文件
func InitConfig(configDir, cryptoKey string) (*ConfigType, error) {
	var config = new(ConfigType)

	if err := cfg.LoadConfigByDir(configDir, config); err != nil {
		return nil, fmt.Errorf("failed to load configs of directory: %s error: %s", configDir, err)
	}

	// 配置项检查，默认值检查和设置
	config.Pushgw.PreCheck()
	config.Alert.PreCheck()
	config.Center.PreCheck()

	// 加密项解密处理
	err := decryptConfig(config, cryptoKey)
	if err != nil {
		return nil, err
	}

	// IP为空，自动探测
	if config.Alert.Heartbeat.IP == "" {
		// auto detect
		config.Alert.Heartbeat.IP = fmt.Sprint(GetOutboundIP())
		if config.Alert.Heartbeat.IP == "" {
			hostname, err := os.Hostname()
			if err != nil {
				fmt.Println("failed to get hostname:", err)
				os.Exit(1)
			}

			if strings.Contains(hostname, "localhost") {
				fmt.Println("Warning! hostname contains substring localhost, setting a more unique hostname is recommended")
			}

			config.Alert.Heartbeat.IP = hostname
		}
	}
	// 使用IP和Port作为Endpoint， 每个N9E服务为一个Endpoint
	config.Alert.Heartbeat.Endpoint = fmt.Sprintf("%s:%d", config.Alert.Heartbeat.IP, config.HTTP.Port)

	return config, nil
}

func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "223.5.5.5:80")
	if err != nil {
		fmt.Println("auto get outbound ip fail:", err)
		return []byte{}
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}
