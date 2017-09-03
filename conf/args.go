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
	RpcServers        []string `mapstructure:"rpc_servers"`
	RunMode           RunMode `mapstructure:"run_mode"`
	Concurrency       int `mapstructure:"concurrency"`
	CpuUsageThreshold float64 `mapstructure:"cpu_usage_threshold"`
	LogLevel          string `mapstructure:"log_level"`
	//TODO logrus log to file
}

func init() {
	setDefaults()
	viper.SetConfigName("stock") // name of config file (without extension)
	viper.AddConfigPath("$GOPATH/bin")
	viper.AddConfigPath(".") // optionally look for config in the working directory
	viper.AddConfigPath("$HOME")
	err := viper.ReadInConfig()
	if err != nil {
		logrus.Errorf("config file error: %+v", err)
		return
	}
	err = viper.Unmarshal(&Args)
	if err != nil {
		logrus.Errorf("config file error: %+v", err)
		return
	}
	logrus.Printf("Configuration: %+v", Args)
	switch Args.LogLevel {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warning":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	case "fatal":
		logrus.SetLevel(logrus.FatalLevel)
	case "panic":
		logrus.SetLevel(logrus.PanicLevel)
	}
	//viper.WatchConfig()
	//viper.OnConfigChange(func(e fsnotify.Event) {
	//	fmt.Println("Config file changed:", e.Name)
	//})
}

func setDefaults() {
	Args.RunMode = LOCAL
	Args.Concurrency = 16
	Args.LogLevel = "info"
	Args.CpuUsageThreshold = 40
}
