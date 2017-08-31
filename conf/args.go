package conf

import (
	"github.com/spf13/viper"
	"github.com/sirupsen/logrus"
)

var Args Arguments

type RunMode string

const (
	LOCAL       RunMode = "local"
	REMOTE      RunMode = "remote"
	DISTRIBUTED RunMode = "distributed"
	SMART       RunMode = "smart"
)

type Arguments struct {
	RpcServers  []string `mapstructure:"rpc_servers"`
	RunMode     RunMode `mapstructure:"run_mode"`
	Concurrency int `mapstructure:"concurrency"`
	CpuUsageThreshold float64 `mapstructure:"cpu_usage_threshold"`
}

func init() {
	setDefaults()
	v := viper.New()
	v.SetConfigName("stock") // name of config file (without extension)
	v.AddConfigPath("$GOPATH/bin")
	v.AddConfigPath(".") // optionally look for config in the working directory
	v.AddConfigPath("$HOME")
	err := v.ReadInConfig()
	if err != nil {
		logrus.Errorf("config file error: %+v", err)
		return
	}
	err = v.Unmarshal(&Args)
	if err != nil {
		logrus.Errorf("config file error: %+v", err)
		return
	}
	logrus.Printf("Configuration: %+v", Args)
	v.WatchConfig()
}

func setDefaults() {
	Args.RunMode = LOCAL
	Args.Concurrency = 16
}
