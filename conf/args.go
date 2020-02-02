package conf

import (
	"go/build"
	"log"
	"os"
	"path/filepath"

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
	XQ          string = "xq"
	EM          string = "em"
	THS         string = "ths"
	ThsCDP      string = "ths.cdp"
	TENCENT     string = "tencent"
	TencentCSRC string = "tencent.csrc"
	TencentTC   string = "tencent.tc"
	WHT         string = "wht"
)

//Arguments arguments struct type
type Arguments struct {
	//RPCServers rpc server address strings
	DefaultRetry      int      `mapstructure:"default_retry"`
	RPCServers        []string `mapstructure:"rpc_servers"`
	RunMode           RunMode  `mapstructure:"run_mode"`
	Concurrency       int      `mapstructure:"concurrency"`
	CPUUsageThreshold float64  `mapstructure:"cpu_usage_threshold"`
	LogLevel          string   `mapstructure:"log_level"`
	Profiling         string   `mapstructure:"profiling"`
	SQLFileLocation   string   `mapstructure:"sql_file_location"`
	DeadlockRetry     int      `mapstructure:"deadlock_retry"`
	DBQueueCapacity   int      `mapstructure:"db_queue_capacity"`
	LogFile           string   `mapstructure:"log_file"`
	Database          struct {
		Host     string `mapstructure:"host"`
		Port     int    `mapstructure:"port"`
		Schema   string `mapstructure:"schema"`
		UserName string `mapstructure:"user_name"`
		Password string `mapstructure:"password"`
	}
	Network struct {
		MasterProxyAddr            string  `mapstructure:"master_proxy_addr"`
		MasterHTTPProxy            string  `mapstructure:"master_http_proxy"`
		RotateProxyBypassRatio     float32 `mapstructure:"rotate_proxy_bypass_ratio"`
		RotateProxyRefreshInterval float64 `mapstructure:"rotate_proxy_refresh_interval"`
		RotateProxyFreshnessMin    int     `mapstructure:"rotate_proxy_freshness_min"`
		RotateProxyScoreThreshold  float32 `mapstructure:"rotate_proxy_score_threshold"`
		DefaultUserAgent           string  `mapstructure:"default_user_agent"`
		UserAgents                 string  `mapstructure:"user_agents"`
		UserAgentLifespan          int     `mapstructure:"user_agent_lifespan"`
		HTTPTimeout                int     `mapstructure:"http_timeout"`
	}
	GCS struct {
		Connection  int    `mapstructure:"connection"`
		UseProxy    bool   `mapstructure:"use_proxy"`
		Bucket      string `mapstructure:"bucket"`
		UploadQueue int    `mapstructure:"upload_queue"`
		Timeout     int    `mapstructure:"timeout"`
	}
	Kdjv struct {
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
	DataSource struct {
		Kline                 string    `mapstructure:"kline"`
		KlineValidateSource   string    `mapstructure:"kline_validate_source"`
		DropInconsistent      bool      `mapstructure:"drop_inconsistent"`
		KlineFailureRetry     int       `mapstructure:"kline_failure_retry"`
		Index                 string    `mapstructure:"index"`
		Industry              string    `mapstructure:"industry"`
		ThsCookie             string    `mapstructure:"ths_cookie"`
		ThsConcurrency        int       `mapstructure:"ths_concurrency"`
		ThsFailureKeyword     string    `mapstructure:"ths_failure_keyword"`
		WhtURL                string    `mapstructure:"wht_url"`
		SkipStocks            bool      `mapstructure:"skip_stocks"`
		SkipFinance           bool      `mapstructure:"skip_finance"`
		SkipKlineVld          bool      `mapstructure:"skip_kline_vld"`
		SkipKlinePre          bool      `mapstructure:"skip_kline_pre"`
		SkipFinancePrediction bool      `mapstructure:"skip_finance_prediction"`
		SkipXdxr              bool      `mapstructure:"skip_xdxr"`
		SkipKlines            bool      `mapstructure:"skip_klines"`
		SkipFsStats           bool      `mapstructure:"skip_fs_stats"`
		SkipIndices           bool      `mapstructure:"skip_indices"`
		SkipBasicsUpdate      bool      `mapstructure:"skip_basics_update"`
		SkipIndexCalculation  bool      `mapstructure:"skip_index_calculation"`
		SkipFinMark           bool      `mapstructure:"skip_fin_mark"`
		SampleKdjFeature      bool      `mapstructure:"sample_kdj_feature"`
		IndicatorSource       string    `mapstructure:"indicator_source"`
		LimitPriceDayLr       []float64 `mapstructure:"limit_price_day_lr"`
		FeatureScaling        string    `mapstructure:"feature_scaling"`
		EM                    struct {
			//DirectProxyWeight is an array of weights for direct connection / master proxy / rotated proxy
			DirectProxyWeight []float64 `mapstructure:"direct_proxy_weight"`
		}
		XQ struct {
			//DirectProxyWeight is an array of weights for direct connection / master proxy / rotated proxy
			DirectProxyWeight []float64 `mapstructure:"direct_proxy_weight"`
		}
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
		Sample              bool     `mapstructure:"sample"`
		PriorLength         int      `mapstructure:"prior_length"`
		Resample            int      `mapstructure:"resample"`
		Grader              string   `mapstructure:"grader"`
		GraderTimeFrames    []int    `mapstructure:"grader_time_frames"`
		GraderScoreClass    int      `mapstructure:"grader_score_class"`
		RefreshGraderStats  bool     `mapstructure:"refresh_grader_stats"`
		TestSetBatchSize    int      `mapstructure:"test_set_batch_size"`
		TestSetGroups       int      `mapstructure:"test_set_groups"`
		TrainSetBatchSize   int      `mapstructure:"train_set_batch_size"`
		VolSize             int      `mapstructure:"vol_size"`
		NumExporter         int      `mapstructure:"num_exporter"`
		ExporterMaxRestTime int      `mapstructure:"exporter_max_rest_time"`
		CorlStartYear       string   `mapstructure:"corl_start_year"`
		CorlPrior           int      `mapstructure:"corl_prior"`
		CorlPortion         float64  `mapstructure:"corl_portion"`
		CorlSpan            int      `mapstructure:"corl_span"`
		CorlTimeSteps       int      `mapstructure:"corl_time_steps"`
		CorlTimeShift       int      `mapstructure:"corl_time_shift"`
		XCorlShift          int      `mapstructure:"xcorl_shift"`
		WccMaxShift         int      `mapstructure:"wcc_max_shift"`
		FeatureCols         []string `mapstructure:"feature_cols"`
	}
}

func init() {
	setDefaults()

	viper.SetConfigName("stock") // name of config file (without extension)

	gopath := os.Getenv("GOPATH")
	if "" == gopath {
		gopath = build.Default.GOPATH
	}
	viper.AddConfigPath(filepath.Join(gopath, "bin"))
	viper.AddConfigPath(".") // optionally look for config in the working directory

	e := viper.ReadInConfig()
	if e != nil {
		log.Panicf("config file error: %+v", e)
	}
	e = viper.Unmarshal(&Args)
	if e != nil {
		log.Panicf("config file error: %+v", e)
	}
	// log.Printf("Configuration: %+v", Args)
	//viper.WatchConfig()
	//viper.OnConfigChange(func(e fsnotify.Event) {
	//	fmt.Println("Config file changed:", e.Name)
	//})
	checkConfig()
}

func checkConfig() {
	shift := Args.Sampler.XCorlShift
	if shift < 0 {
		log.Panicf("Sampler.XCorlShift must be >= 0, but is %d", shift)
	}
	prior := Args.Sampler.PriorLength
	if prior < 0 {
		log.Panicf("Sampler.PriorLength must be >= 0, but is %d", prior)
	}
	if shift > prior {
		log.Panicf(`invalid configuration setting, Sampler.PriorLength (%d) greater than `+
			`Sampler.XCorlShift (%d)`, prior, shift)
	}
	if len(Args.DataSource.XQ.DirectProxyWeight) != 3 {
		log.Panicf(`invalid direct_proxy_weight, must be a float number array of 3 elements: %+v`,
			Args.DataSource.XQ.DirectProxyWeight)
	}
}

func setDefaults() {
	Args.RunMode = LOCAL
	Args.Concurrency = 16
	Args.LogLevel = "info"
	Args.CPUUsageThreshold = 40
	Args.Kdjv.SampleSizeMin = 5
	Args.Kdjv.StatsRetroSpan = 600
	Args.Network.HTTPTimeout = 60
	Args.DataSource.Kline = THS
	Args.DataSource.Index = TENCENT
	Args.DataSource.Industry = TencentCSRC
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
	Args.LogFile = "stock.log"
}
