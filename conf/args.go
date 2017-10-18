package conf

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Args Global Application Arguments
var Args Arguments

// RunMode Running mode
type RunMode string

const (
	//LOCAL run on local power
	LOCAL RunMode = "local"
	//REMOTE run on remote server
	REMOTE RunMode = "remote"
	//DISTRIBUTED run in distributed mode
	DISTRIBUTED RunMode = "distributed"
	//AUTO automatically decide which mode to run on
	AUTO RunMode = "auto"
)

//Data sources
const (
	THS          string = "ths"
	TENCENT      string = "tencent"
	TENCENT_CSRC string = "tencent.csrc"
	TENCENT_TC   string = "tencent.tc"
)

//Arguments arguments struct type
type Arguments struct {
	//RPCServers rpc server address strings
	RPCServers        []string `mapstructure:"rpc_servers"`
	RunMode           RunMode  `mapstructure:"run_mode"`
	Concurrency       int      `mapstructure:"concurrency"`
	CPUUsageThreshold float64  `mapstructure:"cpu_usage_threshold"`
	LogLevel          string   `mapstructure:"log_level"`
	SqlFileLocation   string   `mapstructure:"sql_file_location"`
	Kdjv struct {
		SampleSizeMin  int `mapstructure:"sample_size_min"`
		StatsRetroSpan int `mapstructure:"stats_retro_span"`
	}
	Datasource struct {
		Kline    string `mapstructure:"kline"`
		Index    string `mapstructure:"index"`
		Industry string `mapstructure:"industry"`
		ThsCookie string `mapstructure:"ths_cookie"`
	}
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
	Args.CPUUsageThreshold = 40
	Args.Kdjv.SampleSizeMin = 5
	Args.Kdjv.StatsRetroSpan = 600
	Args.Datasource.Kline = THS
	Args.Datasource.Index = TENCENT
	Args.Datasource.Industry = TENCENT_CSRC
}
