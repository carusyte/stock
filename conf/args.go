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
	THS_CDP      string = "ths.cdp"
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
	SQLFileLocation   string   `mapstructure:"sql_file_location"`
	Kdjv              struct {
		SampleSizeMin  int `mapstructure:"sample_size_min"`
		StatsRetroSpan int `mapstructure:"stats_retro_span"`
	}
	ChromeDP struct {
		Debug    bool   `mapstructure:"debug"`
		Path     string `mapstructure:"path"`
		PoolSize int    `mapstructure:"pool_size"`
		Headless bool   `mapstructure:"headless"`
		Timeout  int64  `mapstructure:"timeout"`
	}
	Datasource struct {
		Kline                 string `mapstructure:"kline"`
		Index                 string `mapstructure:"index"`
		Industry              string `mapstructure:"industry"`
		ThsCookie             string `mapstructure:"ths_cookie"`
		SkipStocks            bool   `mapstructure:"skip_stocks"`
		SkipFinance           bool   `mapstructure:"skip_finance"`
		SkipKlinePre          bool   `mapstructure:"skip_kline_pre"`
		SkipFinancePrediction bool   `mapstructure:"skip_finance_prediction"`
		SkipXdxr              bool   `mapstructure:"skip_xdxr"`
		SkipKlines            bool   `mapstructure:"skip_klines"`
		SkipIndices           bool   `mapstructure:"skip_indices"`
		SkipBasicsUpdate      bool   `mapstructure:"skip_basics_update"`
		SkipIndexCalculation  bool   `mapstructure:"skip_index_calculation"`
		SkipFinMark           bool   `mapstructure:"skip_fin_mark"`
		SampleKdjFeature      bool   `mapstructure:"sample_kdj_feature"`
	}
	Scorer struct {
		RunScorer            bool     `mapstructure:"run_scorer"`
		Highlight            []string `mapstructure:"highlight"`
		FetchData            bool     `mapstructure:"fetch_data"`
		BlueWeight           float64  `mapstructure:"blue_weight"`
		KdjStWeight          float64  `mapstructure:"kdjst_weight"`
		HidBlueBaseRatio     float64  `mapstructure:"hid_blue_base_ratio"`
		HidBlueStarRatio     float64  `mapstructure:"hid_blue_star_ratio"`
		HidBlueRearWarnRatio float64  `mapstructure:"hid_blue_rear_warn_ratio"`
	}
	Sampler struct {
		Sample            bool `mapstructure:"sample"`
		Resample          int  `mapstructure:"resample"`
		TestSetBatchSize  int  `mapstructure:"test_set_batch_size"`
		TrainSetBatchSize int  `mapstructure:"train_set_batch_size"`
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
	Args.Scorer.FetchData = true
	Args.Scorer.BlueWeight = 0.8
	Args.Scorer.KdjStWeight = 0.67
	Args.Scorer.HidBlueBaseRatio = 0.2
	Args.Scorer.HidBlueStarRatio = 0.05
	Args.Scorer.HidBlueRearWarnRatio = 0.1
	Args.ChromeDP.PoolSize = Args.Concurrency
	Args.ChromeDP.Headless = true
	Args.ChromeDP.Timeout = 45
	Args.Sampler.Resample = 5
	Args.Sampler.Sample = true
	Args.Sampler.TestSetBatchSize = 3000
	Args.Sampler.TrainSetBatchSize = 200
}
