package model

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/util"
	"github.com/pkg/errors"
	"gopkg.in/gorp.v2"
)

var log = global.Log

type DBTab string

//CYTP represents cycle type.
type CYTP string

//Rtype represents reinstatement type
type Rtype string

const (
	DAY   CYTP = "D"
	WEEK  CYTP = "W"
	MONTH CYTP = "M"
	M120  CYTP = "M120"
	M60   CYTP = "M60"
	M30   CYTP = "M30"
	M15   CYTP = "M15"
	M5    CYTP = "M5"
	M1    CYTP = "M1"
)

const (
	INDICATOR_DAY   DBTab = "indicator_d"
	INDICATOR_WEEK  DBTab = "indicator_w"
	INDICATOR_MONTH DBTab = "indicator_m"
	//KLINE_DAY obsolete, use KLINE_DAY_F instead
	KLINE_DAY          DBTab = "kline_d"
	KLINE_DAY_F        DBTab = "kline_d_f"
	KLINE_DAY_F_LR     DBTab = "kline_d_f_lr"
	KLINE_DAY_F_MA     DBTab = "kline_d_f_ma"
	KLINE_DAY_F_MA_LR  DBTab = "kline_d_f_ma_lr"
	KLINE_DAY_VLD      DBTab = "kline_d_v"
	KLINE_DAY_NR       DBTab = "kline_d_n"
	KLINE_DAY_NR_LR    DBTab = "kline_d_n_lr"
	KLINE_DAY_NR_MA    DBTab = "kline_d_n_ma"
	KLINE_DAY_NR_MA_LR DBTab = "kline_d_n_ma_lr"
	KLINE_DAY_B        DBTab = "kline_d_b"
	KLINE_DAY_B_LR     DBTab = "kline_d_b_lr"
	KLINE_DAY_B_MA     DBTab = "kline_d_b_ma"
	KLINE_DAY_B_MA_LR  DBTab = "kline_d_b_ma_lr"
	//KLINE_WEEK obsolete, use KLINE_WEEK_F instead
	KLINE_WEEK          DBTab = "kline_w"
	KLINE_WEEK_F        DBTab = "kline_w_f"
	KLINE_WEEK_F_LR     DBTab = "kline_w_f_lr"
	KLINE_WEEK_F_MA     DBTab = "kline_w_f_ma"
	KLINE_WEEK_F_MA_LR  DBTab = "kline_w_f_ma_lr"
	KLINE_WEEK_VLD      DBTab = "kline_w_v"
	KLINE_WEEK_NR       DBTab = "kline_w_n"
	KLINE_WEEK_NR_LR    DBTab = "kline_w_n_lr"
	KLINE_WEEK_NR_MA    DBTab = "kline_w_n_ma"
	KLINE_WEEK_NR_MA_LR DBTab = "kline_w_n_ma_lr"
	KLINE_WEEK_B        DBTab = "kline_w_b"
	KLINE_WEEK_B_LR     DBTab = "kline_w_b_lr"
	KLINE_WEEK_B_MA     DBTab = "kline_w_b_ma"
	KLINE_WEEK_B_MA_LR  DBTab = "kline_w_b_ma_lr"
	//KLINE_MONTH obsolete, use KLINE_MONTH_F instead
	KLINE_MONTH          DBTab = "kline_m"
	KLINE_MONTH_F        DBTab = "kline_m_f"
	KLINE_MONTH_F_LR     DBTab = "kline_m_f_lr"
	KLINE_MONTH_F_MA     DBTab = "kline_m_f_ma"
	KLINE_MONTH_F_MA_LR  DBTab = "kline_m_f_ma_lr"
	KLINE_MONTH_VLD      DBTab = "kline_m_v"
	KLINE_MONTH_NR       DBTab = "kline_m_n"
	KLINE_MONTH_NR_LR    DBTab = "kline_m_n_lr"
	KLINE_MONTH_NR_MA    DBTab = "kline_m_n_ma"
	KLINE_MONTH_NR_MA_LR DBTab = "kline_m_n_ma_lr"
	KLINE_MONTH_B        DBTab = "kline_m_b"
	KLINE_MONTH_B_LR     DBTab = "kline_m_b_lr"
	KLINE_MONTH_B_MA     DBTab = "kline_m_b_ma"
	KLINE_MONTH_B_MA_LR  DBTab = "kline_m_b_ma_lr"
	KLINE_60M            DBTab = "kline_60m"
)

const (
	Forward  Rtype = "forward"
	Backward Rtype = "backward"
	None     Rtype = "none"
)

//Stock represents basic stock info.
type Stock struct {
	Code             string
	Name             string
	Market           sql.NullString
	Industry         sql.NullString
	IndLv1           sql.NullString `db:"ind_lv1"`
	IndLv2           sql.NullString `db:"ind_lv2"`
	IndLv3           sql.NullString `db:"ind_lv3"`
	Area             sql.NullString
	Pe               sql.NullFloat64
	Pu               sql.NullFloat64
	Po               sql.NullFloat64
	Outstanding      sql.NullFloat64
	Totals           sql.NullFloat64
	TotalAssets      sql.NullFloat64
	LiquidAssets     sql.NullFloat64
	FixedAssets      sql.NullFloat64
	Reserved         sql.NullFloat64
	ReservedPerShare sql.NullFloat64
	Esp              sql.NullFloat64
	Bvps             sql.NullFloat64
	Pb               sql.NullFloat64
	TimeToMarket     sql.NullString
	Undp             sql.NullFloat64
	Perundp          sql.NullFloat64
	Rev              sql.NullFloat64
	Profit           sql.NullFloat64
	Gpr              sql.NullFloat64
	Npr              sql.NullFloat64
	Holders          sql.NullInt64
	Price            sql.NullFloat64
	Varate           sql.NullFloat64
	Var              sql.NullFloat64
	Xrate            sql.NullFloat64
	Volratio         sql.NullFloat64
	Ampl             sql.NullFloat64
	Turnover         sql.NullFloat64
	Accer            sql.NullFloat64
	CircMarVal       sql.NullFloat64
	ShareSum         sql.NullFloat64 `db:"share_sum"`
	AShareSum        sql.NullFloat64 `db:"a_share_sum"`
	AShareExch       sql.NullFloat64 `db:"a_share_exch"`
	AShareR          sql.NullFloat64 `db:"a_share_r"`
	BShareSum        sql.NullFloat64 `db:"b_share_sum"`
	BShareExch       sql.NullFloat64 `db:"b_share_exch"`
	BShareR          sql.NullFloat64 `db:"b_share_r"`
	HShareSum        sql.NullFloat64 `db:"h_share_sum"`
	HShareExch       sql.NullFloat64 `db:"h_share_exch"`
	HShareR          sql.NullFloat64 `db:"h_share_r"`
	UDate            sql.NullString
	UTime            sql.NullString
	// source of index
	Source string
}

func (s *Stock) String() string {
	return toJSONString(s)
}

type Stocks struct {
	Map   map[string]*Stock
	List  []*Stock
	Codes []string
}

func (l *Stocks) Diff(a *Stocks) (same bool, diff []string) {
	if a == nil {
		return false, nil
	}
	if l.Size() == 0 && a.Size() == 0 {
		return true, []string{}
	}
	diff = make([]string, 0, int(math.Max(16, math.Abs(float64(l.Size()-a.Size())))))
	for _, c := range l.Codes {
		if _, exists := a.Map[c]; !exists {
			diff = append(diff, c)
		}
	}
	for _, c := range a.Codes {
		if _, exists := l.Map[c]; !exists {
			diff = append(diff, c)
		}
	}
	return len(diff) == 0, diff
}

func (l *Stocks) Size() int {
	return len(l.Codes)
}

func (l *Stocks) SetMarket(m string) {
	for _, s := range l.List {
		s.Market.Valid = true
		s.Market.String = m
	}
}

func (l *Stocks) Add(stks ...*Stock) {
	if stks == nil || len(stks) == 0 {
		return
	}
	if l.Codes == nil {
		l.Codes = make([]string, 0, 16)
	}
	if l.List == nil {
		l.List = make([]*Stock, 0, 16)
	}
	if l.Map == nil {
		l.Map = make(map[string]*Stock)
	}
	for _, s := range stks {
		l.Map[s.Code] = s
		l.List = append(l.List, s)
		l.Codes = append(l.Codes, s.Code)
	}
}

func (l *Stocks) String() string {
	return toJSONString(l)
}

func (l *Stocks) UnmarshalJSON(b []byte) error {
	var f interface{}
	json.Unmarshal(b, &f)
	m := f.(map[string]interface{})
	page := m["pageHelp"].(map[string]interface{})
	tot := int(page["total"].(float64))
	data := page["data"].([]interface{})
	if len(data) != tot {
		return fmt.Errorf("unmatched total numbers: %d/%d", len(data), tot)
	}
	l.List = make([]*Stock, len(data))
	l.Codes = make([]string, len(data))
	l.Map = make(map[string]*Stock, len(data))
	for i, da := range data {
		s := &Stock{}
		d := da.(map[string]interface{})
		// if v, e := strconv.ParseFloat(d["totalFlowShares"].(string), 64); e == nil {
		// 	s.Outstanding.Float64 = v / 10000.0
		// 	s.Outstanding.Valid = true
		// } else {
		// 	return fmt.Errorf("failed to parse totalFlowShares: %+v, %+v", d["totalFlowShares"], e)
		// }
		if v, ok := d["LISTING_DATE"].(string); ok {
			s.TimeToMarket.String = v
			s.TimeToMarket.Valid = true
		} else {
			return fmt.Errorf("failed to parse LISTING_DATE: %+v", d["LISTING_DATE"])
		}
		if v, ok := d["SECURITY_CODE_A"].(string); ok {
			s.Code = v
		} else {
			return fmt.Errorf("failed to parse SECURITY_CODE_A: %+v", d["SECURITY_CODE_A"])
		}
		if v, ok := d["SECURITY_ABBR_A"].(string); ok {
			s.Name = v
		} else {
			return fmt.Errorf("failed to parse SECURITY_ABBR_A: %+v", d["SECURITY_ABBR_A"])
		}
		// if v, e := strconv.ParseFloat(d["totalShares"].(string), 64); e == nil {
		// 	s.Totals.Float64 = v / 10000.0
		// 	s.Totals.Valid = true
		// } else {
		// 	return fmt.Errorf("failed to parse totalShares: %+v, %+v", d["totalShares"], e)
		// }
		dt, tm := util.TimeStr()
		s.UDate.Valid = true
		s.UTime.Valid = true
		s.UDate.String = dt
		s.UTime.String = tm
		l.List[i] = s
		l.Codes[i] = s.Code
		l.Map[s.Code] = s
	}
	return nil
}

type Xdxr struct {
	Code string
	Name string
	Idx  int
	//公告日期
	NoticeDate sql.NullString `db:"notice_date"`
	//报告期
	ReportYear sql.NullString `db:"report_year"`
	//董事会日期
	BoardDate sql.NullString `db:"board_date"`
	//每10股分红金额
	Divi sql.NullFloat64 `db:"divi"`
	//每10股分红金额（税后）
	DiviAtx sql.NullFloat64
	//分红截止日期
	DiviEndDate sql.NullString
	//分红率
	Dyr sql.NullFloat64 `db:"dyr"`
	//分红对象
	DiviTarget sql.NullString
	//每十股送红股
	SharesAllot sql.NullFloat64
	//红股上市日期
	SharesAllotDate sql.NullString
	//每十股转增股本
	SharesCvt sql.NullFloat64
	//转增股本上市日
	SharesCvtDate sql.NullString
	//派息股本基数
	SharesBase sql.NullInt64
	//股东大会日期
	GmsDate sql.NullString `db:"gms_date"`
	//实施日期
	ImplDate sql.NullString `db:"impl_date"`
	//分红方案说明
	Plan sql.NullString
	//股权登记日
	RegDate sql.NullString `db:"reg_date"`
	//除权除息日
	XdxrDate sql.NullString `db:"xdxr_date"`
	//股息到账日
	PayoutDate sql.NullString `db:"payout_date"`
	//最后交易日
	EndTrdDate sql.NullString
	//方案进度
	Progress sql.NullString `db:"progress"`
	//股利支付率 Dividend Payout Ratio
	Dpr sql.NullFloat64 `db:"dpr"`
	//股价刷新标记
	Xprice sql.NullString `db:"xprice"`
	//最后更新日期
	Udate sql.NullString
	//最后更新时间
	Utime sql.NullString
}

func (x *Xdxr) String() string {
	return toJSONString(x)
}

type Finance struct {
	Code string
	Year string
	//Earnings Per Share 每股收益
	Eps sql.NullFloat64
	//EPS Growth Rate Year-on-Year 每股收益同比增长率
	EpsYoy sql.NullFloat64 `db:"eps_yoy"`
	//Net Profit (1/10 Billion) 净利润（亿）
	Np sql.NullFloat64
	//Net Profit Growth Rate Year-on-Year 净利润同比增长率
	NpYoy sql.NullFloat64 `db:"np_yoy"`
	//Net Profit Ring Growth 净利润环比增长率
	NpRg sql.NullFloat64 `db:"np_rg"`
	//Net Profit After Deduction of Non-profits 扣除非经常性损益后的净利润
	NpAdn sql.NullFloat64 `db:"np_adn"`
	//Net Profit After Deduction of Non-profits Growth Rate Year-on-Year 扣非净利润同比增长率
	NpAdnYoy sql.NullFloat64 `db:"np_adn_yoy"`
	//Gross Revenue (1/10 Billion) 营业总收入（亿）
	Gr sql.NullFloat64
	//Gross Revenue Growth Rate Year-on-Year 营业总收入同比增长率
	GrYoy sql.NullFloat64 `db:"gr_yoy"`
	//Net Asset Value Per Share  每股净资产
	Navps sql.NullFloat64
	//Return on Equity 净资产收益率
	Roe sql.NullFloat64
	// ROE Growth Rate Year-on-Year 净资产收益率同比增长率
	RoeYoy sql.NullFloat64 `db:"roe_yoy"`
	//Return on Equity Diluted 净资产收益率-摊薄
	RoeDlt sql.NullFloat64 `db:"roe_dlt"`
	//Debt to Asset Ratio 资产负载比
	Dar sql.NullFloat64
	//Capital Reserves Per Share 每股资本公积
	Crps sql.NullFloat64
	//Undistributed Profit Per Share 每股未分配利润
	Udpps sql.NullFloat64
	// UDPPS Growth Rate Year-on-Year 每股未分配利润同比���������������������长率
	UdppsYoy sql.NullFloat64 `db:"udpps_yoy"`
	//Operational Cash Flow Per Share 每股经营现金流
	Ocfps sql.NullFloat64
	// OCFPS Growth Rate Year-on-Year 每股经营现金流同比增长率
	OcfpsYoy sql.NullFloat64 `db:"ocfps_yoy"`
	//Gross Profit Margin 毛利率
	Gpm sql.NullFloat64
	//Net Profit Margin 净利率
	Npm sql.NullFloat64
	//Inventory Turnover Ratio 存货周转率
	Itr sql.NullFloat64
	//最后更新日期
	Udate sql.NullString
	//最后更新时间
	Utime sql.NullString
}

type FinReport struct {
	Code  string
	Items []*Finance
}

func (fin *FinReport) SetCode(code string) {
	fin.Code = code
	for _, f := range fin.Items {
		f.Code = code
	}
}

func (fin *FinReport) UnmarshalJSON(b []byte) error {
	var f interface{}
	json.Unmarshal(b, &f)
	m := f.(map[string]interface{})
	titles := m["title"].([]interface{})
	iEps, iNp, iNpYoy, iNpRg, iNpAdn, iNpAdnYoy, iGr, iGrYoy, iNavps, iRoe, iRoeDlt, iAlr, iCrps, iUdpps, iOcfps,
		iGpm, iNpm, iItr := -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
	mNp, mNpAdn, mGr := .1, .1, .1
	for i, t := range titles {
		var v string
		if elems, ok := t.([]interface{}); ok {
			v = fmt.Sprintf("%v %v", elems[0], elems[1])
			v = strings.TrimSpace(v)
		} else if str, ok := t.(string); ok {
			v = str
		} else {
			return errors.Errorf(`unable to parse element in "title", unhandled type: %T`, t)
		}
		switch v {
		case "基本每股收益":
		case "基本每股收益 元":
			iEps = i
		case "净利润 万元":
			mNp = 0.0001
			fallthrough
		case "净利润 元":
			iNp = i
		case "净利润同比增长率 %":
		case "净利润同比增长率":
			iNpYoy = i
		case "净利润环比增长率 %":
			fallthrough
		case "净利润环比增长率":
			iNpRg = i
		case "扣非净利润 万元":
			mNpAdn = 0.0001
			fallthrough
		case "扣非净利润 元":
			iNpAdn = i
		case "扣非净利润同比增长率 %":
		case "扣非净利润同比增长率":
			iNpAdnYoy = i
		case "营业总收入 万元":
			mGr = 0.0001
			fallthrough
		case "营业总收入 元":
			iGr = i
		case "营业总收入同比增长率 %":
		case "营业总收入同比增长率":
			iGrYoy = i
		case "每股净资产":
		case "每股净资产 元":
			iNavps = i
		case "净资产收益率 %":
		case "净资产收益率":
			iRoe = i
		case "净资产收益率-摊薄 %":
		case "净资产收益率-摊薄":
			iRoeDlt = i
		case "资产负债比率 %":
		case "资产负债比率":
			iAlr = i
		case "每股资本公积金":
		case "每股资本公积金 元":
			iCrps = i
		case "每股未分配利润":
		case "每股未分配利润 元":
			iUdpps = i
		case "每股经营现金流":
		case "每股经营现金流 元":
			iOcfps = i
		case "销售毛利率 %":
		case "销售毛利率":
			iGpm = i
		case "存货周转率":
			iItr = i
		case "销售净利率 %":
			fallthrough
		case "销售净利率":
			iNpm = i
		case `科目\时间`:
			//do nothing
		default:
			log.Printf("%s unidentified finance report item: %s", fin.Code, v)
		}
	}
	rpt := m["report"].([]interface{})
	for i, r := range rpt {
		if i == 0 {
			//parse year
			for _, iy := range r.([]interface{}) {
				fi := &Finance{}
				fi.Year = iy.(string)
				fin.Items = append(fin.Items, fi)
			}
		} else {
			//parse data
			for j, y := range r.([]interface{}) {
				if s, ok := y.(string); ok {
					fi := fin.Items[j]
					switch i {
					case iEps:
						fi.Eps = util.Str2Fnull(s)
					case iNp:
						fi.Np = util.Str2FBilMod(s, mNp)
					case iNpYoy:
						fi.NpYoy = util.Pct2Fnull(s)
					case iNpRg:
						fi.NpRg = util.Pct2Fnull(s)
					case iNpAdn:
						fi.NpAdn = util.Str2FBilMod(s, mNpAdn)
					case iNpAdnYoy:
						fi.NpAdnYoy = util.Pct2Fnull(s)
					case iGr:
						fi.Gr = util.Str2FBilMod(s, mGr)
					case iGrYoy:
						fi.GrYoy = util.Pct2Fnull(s)
					case iNavps:
						fi.Navps = util.Str2Fnull(s)
					case iRoe:
						fi.Roe = util.Pct2Fnull(s)
					case iRoeDlt:
						fi.RoeDlt = util.Pct2Fnull(s)
					case iAlr:
						fi.Dar = util.Pct2Fnull(s)
					case iCrps:
						fi.Crps = util.Str2Fnull(s)
					case iUdpps:
						fi.Udpps = util.Str2Fnull(s)
					case iOcfps:
						fi.Ocfps = util.Str2Fnull(s)
					case iGpm:
						fi.Gpm = util.Pct2Fnull(s)
					case iNpm:
						fi.Npm = util.Pct2Fnull(s)
					case iItr:
						fi.Itr = util.Str2Fnull(s)
					default:
						log.Printf("%s unidentified row index %d, %+v", fin.Code, i, y)
					}
				}
			}
		}
	}
	return nil
}

//TradeDataBase models the basic trading data such as OHLCV etc.
type TradeDataBase struct {
	Code          string
	Date          string
	Klid          int
	Open          float64
	High          float64
	Close         float64
	Low           float64
	Volume        sql.NullFloat64
	Amount        sql.NullFloat64
	Xrate         sql.NullFloat64
	Varate        sql.NullFloat64
	VarateHigh    sql.NullFloat64 `db:"varate_h"`
	VarateOpen    sql.NullFloat64 `db:"varate_o"`
	VarateLow     sql.NullFloat64 `db:"varate_l"`
	VarateRgl     sql.NullFloat64 `db:"varate_rgl"`
	VarateRglHigh sql.NullFloat64 `db:"varate_rgl_h"`
	VarateRglOpen sql.NullFloat64 `db:"varate_rgl_o"`
	VarateRglLow  sql.NullFloat64 `db:"varate_rgl_l"`
	Udate         sql.NullString
	Utime         sql.NullString
}

func (d *TradeDataBase) String() string {
	return toJSONString(d)
}

//TradeDataLogRtn models Log Returns for the trading data.
type TradeDataLogRtn struct {
	Code      string
	Date      string
	Klid      int
	Amount    sql.NullFloat64
	Xrate     sql.NullFloat64
	Lr        sql.NullFloat64 //Log Returns
	Open      sql.NullFloat64
	High      sql.NullFloat64
	Low       sql.NullFloat64
	Volume    sql.NullFloat64
	HighClose sql.NullFloat64 `db:"high_close"`
	OpenClose sql.NullFloat64 `db:"open_close"`
	LowClose  sql.NullFloat64 `db:"low_close"`
	Udate     sql.NullString
	Utime     sql.NullString
}

func (d *TradeDataLogRtn) String() string {
	return toJSONString(d)
}

//TradeDataMovAvg models Moving Average for the trading data.
type TradeDataMovAvg struct {
	Code   string
	Date   string
	Klid   int
	Ma5    sql.NullFloat64
	Ma10   sql.NullFloat64
	Ma20   sql.NullFloat64
	Ma30   sql.NullFloat64
	Ma60   sql.NullFloat64
	Ma120  sql.NullFloat64
	Ma200  sql.NullFloat64
	Ma250  sql.NullFloat64
	Vol5   sql.NullFloat64
	Vol10  sql.NullFloat64
	Vol20  sql.NullFloat64
	Vol30  sql.NullFloat64
	Vol60  sql.NullFloat64
	Vol120 sql.NullFloat64
	Vol200 sql.NullFloat64
	Vol250 sql.NullFloat64
	Udate  sql.NullString
	Utime  sql.NullString
}

func (d *TradeDataMovAvg) String() string {
	return toJSONString(d)
}

//TradeDataMovAvgLogRtn models Moving Average Log Return for the trading data.
type TradeDataMovAvgLogRtn struct {
	Code      string
	Date      string
	Klid      int
	Ma5       sql.NullFloat64
	Ma5Open   sql.NullFloat64
	Ma5High   sql.NullFloat64
	Ma5Low    sql.NullFloat64
	Ma10      sql.NullFloat64
	Ma10Open  sql.NullFloat64
	Ma10High  sql.NullFloat64
	Ma10Low   sql.NullFloat64
	Ma20      sql.NullFloat64
	Ma20Open  sql.NullFloat64
	Ma20High  sql.NullFloat64
	Ma20Low   sql.NullFloat64
	Ma30      sql.NullFloat64
	Ma30Open  sql.NullFloat64
	Ma30High  sql.NullFloat64
	Ma30Low   sql.NullFloat64
	Ma60      sql.NullFloat64
	Ma60Open  sql.NullFloat64
	Ma60High  sql.NullFloat64
	Ma60Low   sql.NullFloat64
	Ma120     sql.NullFloat64
	Ma120Open sql.NullFloat64
	Ma120High sql.NullFloat64
	Ma120Low  sql.NullFloat64
	Ma200     sql.NullFloat64
	Ma200Open sql.NullFloat64
	Ma200High sql.NullFloat64
	Ma200Low  sql.NullFloat64
	Ma250     sql.NullFloat64
	Ma250Open sql.NullFloat64
	Ma250High sql.NullFloat64
	Ma250Low  sql.NullFloat64
	Vol5      sql.NullFloat64
	Vol10     sql.NullFloat64
	Vol20     sql.NullFloat64
	Vol30     sql.NullFloat64
	Vol60     sql.NullFloat64
	Vol120    sql.NullFloat64
	Vol200    sql.NullFloat64
	Vol250    sql.NullFloat64
	Udate     sql.NullString
	Utime     sql.NullString
}

func (d *TradeDataMovAvgLogRtn) String() string {
	return toJSONString(d)
}

//TradeData models various aspects of the trading data.
type TradeData struct {
	Code          string
	Cycle         CYTP
	Reinstatement Rtype
	Base          []*TradeDataBase
	LogRtn        []*TradeDataLogRtn
	MovAvg        []*TradeDataMovAvg
	MovAvgLogRtn  []*TradeDataMovAvgLogRtn
}

func (td *TradeData) String() string {
	return toJSONString(td)
}

//Empty returns whether there is no valid data within this instance
func (td *TradeData) Empty() bool {
	return len(td.Base) == 0 && len(td.LogRtn) == 0 && len(td.MovAvg) == 0 && len(td.MovAvgLogRtn) == 0
}

//MaxLen returns the maximum length of slice in all types of trade data within the instance.
func (td *TradeData) MaxLen() (maxlen int) {
	if maxlen = 0; len(td.Base) > maxlen {
		maxlen = len(td.Base)
	}
	if len(td.LogRtn) > maxlen {
		maxlen = len(td.LogRtn)
	}
	if len(td.MovAvg) > maxlen {
		maxlen = len(td.MovAvg)
	}
	if len(td.MovAvgLogRtn) > maxlen {
		maxlen = len(td.MovAvgLogRtn)
	}
	return
}

//Remove the elements in the specified positions.
func (td *TradeData) Remove(positions ...int) {
	if len(positions) == 0 {
		return
	}
	maxLen := td.MaxLen()
	set := make(map[int]bool)
	for i := range positions {
		if i < maxLen {
			set[i] = true
		}
	}
	if len(td.Base) > 0 {
		var newArray []*TradeDataBase
		for i, d := range td.Base {
			if _, ok := set[i]; !ok {
				newArray = append(newArray, d)
			}
		}
		td.Base = newArray
	}
	if len(td.LogRtn) > 0 {
		var newArray []*TradeDataLogRtn
		for i, d := range td.LogRtn {
			if _, ok := set[i]; !ok {
				newArray = append(newArray, d)
			}
		}
		td.LogRtn = newArray
	}
	if len(td.MovAvgLogRtn) > 0 {
		var newArray []*TradeDataMovAvgLogRtn
		for i, d := range td.MovAvgLogRtn {
			if _, ok := set[i]; !ok {
				newArray = append(newArray, d)
			}
		}
		td.MovAvgLogRtn = newArray
	}
	if len(td.MovAvg) > 0 {
		var newArray []*TradeDataMovAvg
		for i, d := range td.MovAvg {
			if _, ok := set[i]; !ok {
				newArray = append(newArray, d)
			}
		}
		td.MovAvg = newArray
	}
}

//Keep the specified elements in the trade data arrays.
//***Warning***: Calling Keep with empty array will remove all elements.
func (td *TradeData) Keep(positions ...int) {
	if len(positions) == 0 {
		td.Base = make([]*TradeDataBase, 0, 16)
		td.MovAvg = make([]*TradeDataMovAvg, 0, 16)
		td.MovAvgLogRtn = make([]*TradeDataMovAvgLogRtn, 0, 16)
		td.LogRtn = make([]*TradeDataLogRtn, 0, 16)
		return
	}
	maxLen := td.MaxLen()
	set := make(map[int]bool)
	for i := range positions {
		if 0 <= i && i < maxLen {
			set[i] = true
		}
	}
	if len(td.Base) > 0 {
		var newArray []*TradeDataBase
		for i, d := range td.Base {
			if _, ok := set[i]; ok {
				newArray = append(newArray, d)
			}
		}
		td.Base = newArray
	}
	if len(td.LogRtn) > 0 {
		var newArray []*TradeDataLogRtn
		for i, d := range td.LogRtn {
			if _, ok := set[i]; ok {
				newArray = append(newArray, d)
			}
		}
		td.LogRtn = newArray
	}
	if len(td.MovAvgLogRtn) > 0 {
		var newArray []*TradeDataMovAvgLogRtn
		for i, d := range td.MovAvgLogRtn {
			if _, ok := set[i]; ok {
				newArray = append(newArray, d)
			}
		}
		td.MovAvgLogRtn = newArray
	}
	if len(td.MovAvg) > 0 {
		var newArray []*TradeDataMovAvg
		for i, d := range td.MovAvg {
			if _, ok := set[i]; ok {
				newArray = append(newArray, d)
			}
		}
		td.MovAvg = newArray
	}
}

//Quote represents various kline data
type Quote struct {
	Type          DBTab
	Code          string `db:",size:6"`
	Date          string `db:",size:10"`
	Klid          int
	Open          float64
	High          float64
	Close         float64
	Low           float64
	Volume        sql.NullFloat64
	Amount        sql.NullFloat64
	LrAmt         sql.NullFloat64 `db:"lr_amt"`
	Xrate         sql.NullFloat64
	LrXr          sql.NullFloat64 `db:"lr_xr"`
	Varate        sql.NullFloat64
	VarateHigh    sql.NullFloat64 `db:"varate_h"`
	VarateOpen    sql.NullFloat64 `db:"varate_o"`
	VarateLow     sql.NullFloat64 `db:"varate_l"`
	VarateRgl     sql.NullFloat64 `db:"varate_rgl"`
	VarateRglHigh sql.NullFloat64 `db:"varate_rgl_h"`
	VarateRglOpen sql.NullFloat64 `db:"varate_rgl_o"`
	VarateRglLow  sql.NullFloat64 `db:"varate_rgl_l"`
	Lr            sql.NullFloat64 //Log Returns
	LrHigh        sql.NullFloat64 `db:"lr_h"`
	LrHighClose   sql.NullFloat64 `db:"lr_h_c"`
	LrOpen        sql.NullFloat64 `db:"lr_o"`
	LrOpenClose   sql.NullFloat64 `db:"lr_o_c"`
	LrLow         sql.NullFloat64 `db:"lr_l"`
	LrLowClose    sql.NullFloat64 `db:"lr_l_c"`
	LrVol         sql.NullFloat64 `db:"lr_vol"` //Log Returns for Volume
	Ma5           sql.NullFloat64
	Ma10          sql.NullFloat64
	Ma20          sql.NullFloat64
	Ma30          sql.NullFloat64
	Ma60          sql.NullFloat64
	Ma120         sql.NullFloat64
	Ma200         sql.NullFloat64
	Ma250         sql.NullFloat64
	LrMa5         sql.NullFloat64 `db:"lr_ma5"`
	LrMa5Open     sql.NullFloat64 `db:"lr_ma5_o"`
	LrMa5High     sql.NullFloat64 `db:"lr_ma5_h"`
	LrMa5Low      sql.NullFloat64 `db:"lr_ma5_l"`
	LrMa10        sql.NullFloat64 `db:"lr_ma10"`
	LrMa10Open    sql.NullFloat64 `db:"lr_ma10_o"`
	LrMa10High    sql.NullFloat64 `db:"lr_ma10_h"`
	LrMa10Low     sql.NullFloat64 `db:"lr_ma10_l"`
	LrMa20        sql.NullFloat64 `db:"lr_ma20"`
	LrMa20Open    sql.NullFloat64 `db:"lr_ma20_o"`
	LrMa20High    sql.NullFloat64 `db:"lr_ma20_h"`
	LrMa20Low     sql.NullFloat64 `db:"lr_ma20_l"`
	LrMa30        sql.NullFloat64 `db:"lr_ma30"`
	LrMa30Open    sql.NullFloat64 `db:"lr_ma30_o"`
	LrMa30High    sql.NullFloat64 `db:"lr_ma30_h"`
	LrMa30Low     sql.NullFloat64 `db:"lr_ma30_l"`
	LrMa60        sql.NullFloat64 `db:"lr_ma60"`
	LrMa60Open    sql.NullFloat64 `db:"lr_ma60_o"`
	LrMa60High    sql.NullFloat64 `db:"lr_ma60_h"`
	LrMa60Low     sql.NullFloat64 `db:"lr_ma60_l"`
	LrMa120       sql.NullFloat64 `db:"lr_ma120"`
	LrMa120Open   sql.NullFloat64 `db:"lr_ma120_o"`
	LrMa120High   sql.NullFloat64 `db:"lr_ma120_h"`
	LrMa120Low    sql.NullFloat64 `db:"lr_ma120_l"`
	LrMa200       sql.NullFloat64 `db:"lr_ma200"`
	LrMa200Open   sql.NullFloat64 `db:"lr_ma200_o"`
	LrMa200High   sql.NullFloat64 `db:"lr_ma200_h"`
	LrMa200Low    sql.NullFloat64 `db:"lr_ma200_l"`
	LrMa250       sql.NullFloat64 `db:"lr_ma250"`
	LrMa250Open   sql.NullFloat64 `db:"lr_ma250_o"`
	LrMa250High   sql.NullFloat64 `db:"lr_ma250_h"`
	LrMa250Low    sql.NullFloat64 `db:"lr_ma250_l"`
	Vol5          sql.NullFloat64
	Vol10         sql.NullFloat64
	Vol20         sql.NullFloat64
	Vol30         sql.NullFloat64
	Vol60         sql.NullFloat64
	Vol120        sql.NullFloat64
	Vol200        sql.NullFloat64
	Vol250        sql.NullFloat64
	LrVol5        sql.NullFloat64 `db:"lr_vol5"`
	LrVol10       sql.NullFloat64 `db:"lr_vol10"`
	LrVol20       sql.NullFloat64 `db:"lr_vol20"`
	LrVol30       sql.NullFloat64 `db:"lr_vol30"`
	LrVol60       sql.NullFloat64 `db:"lr_vol60"`
	LrVol120      sql.NullFloat64 `db:"lr_vol120"`
	LrVol200      sql.NullFloat64 `db:"lr_vol200"`
	LrVol250      sql.NullFloat64 `db:"lr_vol250"`
	Udate         sql.NullString
	Utime         sql.NullString
}

func (q *Quote) String() string {
	return toJSONString(q)
}

type K60MinList struct {
	Quotes []*Quote
}

type Kline struct {
	Quote
	// Factor sql.NullFloat64
}

type KlineW struct {
	Quote
}

type KlineM struct {
	Quote
}

type Indicator struct {
	Code         string `db:",size:6"`
	Date         string `db:",size:10"`
	Klid         int
	KDJ_K        float64
	KDJ_D        float64
	KDJ_J        float64
	MACD         float64
	MACD_diff    float64
	MACD_dea     float64
	RSI1         float64
	RSI2         float64
	RSI3         float64
	BIAS1        float64
	BIAS2        float64
	BIAS3        float64
	BOLL_mid     float64
	BOLL_mid_o   float64
	BOLL_mid_h   float64
	BOLL_mid_l   float64
	BOLL_mid_c   float64
	BOLL_upper   float64
	BOLL_upper_o float64
	BOLL_upper_h float64
	BOLL_upper_l float64
	BOLL_upper_c float64
	BOLL_lower   float64
	BOLL_lower_o float64
	BOLL_lower_h float64
	BOLL_lower_l float64
	BOLL_lower_c float64
	//最后更新日期
	Udate sql.NullString
	//最后更新时间
	Utime sql.NullString
}

type IndicatorW struct {
	Indicator
}

type IndicatorM struct {
	Indicator
}

func (k *K60MinList) UnmarshalJSON(b []byte) error {
	var f interface{}
	json.Unmarshal(b, &f)
	clist := f.(map[string]interface{})["chartlist"].([]interface{})
	k.Quotes = make([]*Quote, len(clist))
	for i, ci := range clist {
		im := ci.(map[string]interface{})
		q := new(Quote)
		k.Quotes[i] = q
		for k := range im {
			switch k {
			case "volume":
				q.Volume.Valid = true
				q.Volume.Float64 = im[k].(float64)
			case "open":
				q.Open = im[k].(float64)
			case "high":
				q.High = im[k].(float64)
			case "close":
				q.Close = im[k].(float64)
			case "low":
				q.Low = im[k].(float64)
			default:
				//do nothing
			}
		}
	}
	return nil
}

func (k *KlineW) String() string {
	return toJSONString(k)
}

type KlAll struct {
	Total       int           `json:"total"`
	Start       string        `json:"start"`
	Name        string        `json:"name"`
	SortYear    []interface{} `json:"sortYear"`
	PriceFactor float64       `json:"priceFactor"`
	Price       string        `json:"price"`
	Volume      string        `json:"volumn"`
	Dates       string        `json:"dates"`
	//IssuePrice  string        `json:"dates"`
}

func (ka *KlAll) UnmarshalJSON(b []byte) (e error) {
	defer func() {
		if r := recover(); r != nil {
			if er, ok := r.(error); ok {
				log.Printf("%s\n%s", er, string(b))
				e = errors.Wrap(er, fmt.Sprintf("failed to unmarshal KlAll json: %s", string(b)))
			}
		}
	}()

	var f interface{}
	json.Unmarshal(b, &f)

	m := f.(map[string]interface{})

	for k, v := range m {
		switch k {
		case "total":
			if vi, ok := v.(int); ok {
				ka.Total = vi
			} else if vf, ok := v.(float64); ok {
				ka.Total = int(vf)
			} else if vs, ok := v.(string); ok {
				ka.Total, e = strconv.Atoi(vs)
				if e != nil {
					return e
				}
			}
		case "start":
			ka.Start = v.(string)
		case "name:":
			ka.Name = v.(string)
		case "sortYear":
			ka.SortYear = v.([]interface{})
		case "priceFactor":
			ka.PriceFactor = v.(float64)
		case "price":
			ka.Price = v.(string)
		case "volumn":
			ka.Volume = v.(string)
		case "dates":
			ka.Dates = v.(string)
		default:
			//do nothing
		}
	}

	// initial check
	if ka.Total > 0 && (ka.Volume == "" || ka.Dates == "") {
		return errors.New("invalid json data")
	}

	return nil
}

type Klast struct {
	//Rt         string `json:"rt"`
	Num int `json:"num"`
	//Total      int `json:"total"`
	Start string                 `json:"start"`
	Year  map[string]interface{} `json:"year"`
	Name  string                 `json:"name"`
	Khist
	//IssuePrice float32 `json:"issuePrice"`
}

func (kl *Klast) UnmarshalJSON(b []byte) error {
	var f interface{}
	json.Unmarshal(b, &f)

	m := f.(map[string]interface{})

	for k := range m {
		switch k {
		case "num":
			kl.Num = int(m[k].(float64))
		case "start":
			kl.Start = m[k].(string)
		case "year":
			if y, ok := m[k].(map[string]interface{}); ok {
				kl.Year = y
			} else {
				kl.Year = make(map[string]interface{}, 0)
			}
		case "name:":
			kl.Name = m[k].(string)
		case "data":
			kl.Data = m[k].(string)
			//case "issuePrice":
			//	if v, ok := m[k].(string);ok{
			//		kl.IssuePrice =
			//	}
			//case "total":
			//case "rt":
		default:
			//do nothing
		}
	}
	return nil
}

type Khist struct {
	Data string `json:"data"`
}

type Ktoday struct {
	Quote
}

func (kt *Ktoday) UnmarshalJSON(b []byte) (e error) {
	defer func() {
		if r := recover(); r != nil {
			if er, ok := r.(error); ok {
				log.Printf("%s\n%s", er, string(b))
				e = errors.Wrap(er, fmt.Sprintf("failed to unmarshal Ktoday json: %s", string(b)))
			}
		}
	}()
	var f interface{}
	json.Unmarshal(b, &f)

	m := f.(map[string]interface{})

	for k := range m {
		qm := m[k].(map[string]interface{})
		if dt, ok := qm["1"].(string); ok {
			kt.Code = k[3:]
			kt.Date = dt[:4] + "-" + dt[4:6] + "-" + dt[6:]
			kt.Open = util.Str2F64(qm["7"].(string))
			kt.High = util.Str2F64(qm["8"].(string))
			kt.Low = util.Str2F64(qm["9"].(string))
			kt.Close = util.Str2F64(qm["11"].(string))
			kt.Volume = sql.NullFloat64{qm["13"].(float64), true}
			kt.Amount = sql.NullFloat64{util.Str2F64(qm["19"].(string)), true}
			kt.Xrate = sql.NullFloat64{util.Str2F64(qm["1968584"].(string)), true}
		} else {
			e = errors.Errorf("failed to parse Ktoday json: %s", string(b))
			return
		}
	}

	return nil
}

type IndcFeatRaw struct {
	Code    string
	Indc    string
	Cytp    string
	Bysl    string
	SmpDate string `db:"smp_date"`
	SmpNum  int    `db:"smp_num"`
	Fid     string
	Mark    float64
	Tspan   int
	Mpt     float64
	Remarks sql.NullString
	Udate   string
	Utime   string
}

func (indf *IndcFeatRaw) GenFid() string {
	indf.Fid = fmt.Sprintf("%s%s%s", indf.Cytp, indf.Bysl, strings.Replace(indf.SmpDate, "-", "", -1))
	return indf.Fid
}

func (indf *IndcFeatRaw) String() string {
	return fmt.Sprintf("|%s,%s,%s,%f,%d,%f|", indf.Code, indf.Fid, indf.Bysl, indf.Mark, indf.Tspan, indf.Mpt)
}

type IndcFeat struct {
	Indc    string
	Fid     string
	Cytp    string
	Bysl    string
	SmpNum  int `db:"smp_num"`
	FdNum   int `db:"fd_num"`
	Weight  float64
	Remarks sql.NullString
	Udate   string
	Utime   string
}

type KDJfd struct {
	Fid   string
	Seq   int
	K     float64
	D     float64
	J     float64
	Udate string
	Utime string
}

type KDJfdView struct {
	Indc, Fid, Bysl, Remarks string
	Cytp                     CYTP
	SmpNum, FdNum            int
	Weight                   float64
	K                        []float64
	D                        []float64
	J                        []float64
}

func (kfv *KDJfdView) Add(k, d, j float64) {
	kfv.K = append(kfv.K, k)
	kfv.D = append(kfv.D, d)
	kfv.J = append(kfv.J, j)
}

func (v *KDJfdView) String() string {
	j, e := json.Marshal(v)
	if e != nil {
		fmt.Println(e)
	}
	return fmt.Sprintf("%v", string(j))
}

type KDJfdRaw struct {
	Code  string
	Fid   string
	Klid  int
	K     float64
	D     float64
	J     float64
	Udate string
	Utime string
	Feat  *IndcFeatRaw
}

type KDJfdrView struct {
	Code    string
	SmpDate string
	SmpNum  int
	Klid    []int
	K       []float64
	D       []float64
	J       []float64
}

func (kfv *KDJfdrView) Add(klid int, k, d, j float64) {
	kfv.Klid = append(kfv.Klid, klid)
	kfv.K = append(kfv.K, k)
	kfv.D = append(kfv.D, d)
	kfv.J = append(kfv.J, j)
}

type KDJVStat struct {
	Code, Frmdt, Todt, Udate, Utime             string
	Dod, Sl, Sh, Bl, Bh, Sor, Bor, Smean, Bmean float64
	Scnt, Bcnt                                  int
}

type XQJson struct {
	Stock struct {
		Symbol string
	}
	Success   string
	Chartlist []struct {
		Volume                                                                                int64
		Open, High, Low, Close, Chg, Percent, Turnrate, Ma5, Ma10, Ma20, Ma30, Dif, Dea, Macd float64
		LotVolume                                                                             int64 `json:"lot_volume"`
		Timestamp                                                                             int64
		Time                                                                                  string
	}
}

func (xqj *XQJson) Save(dbmap *gorp.DbMap, sklid int, table string) {
	if len(xqj.Chartlist) > 0 {
		valueStrings := make([]string, 0, len(xqj.Chartlist))
		valueArgs := make([]interface{}, 0, len(xqj.Chartlist)*13)
		var code string
		klid := sklid
		for _, q := range xqj.Chartlist {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, round(?,3), ?, ?)")
			valueArgs = append(valueArgs, xqj.Stock)
			valueArgs = append(valueArgs,
				time.Unix(q.Timestamp/int64(time.Microsecond), 0).Format("2006-01-02"))
			valueArgs = append(valueArgs, klid)
			valueArgs = append(valueArgs, q.Open)
			valueArgs = append(valueArgs, q.High)
			valueArgs = append(valueArgs, q.Close)
			valueArgs = append(valueArgs, q.Low)
			valueArgs = append(valueArgs, q.Volume)
			//valueArgs = append(valueArgs, q.Amount)
			//valueArgs = append(valueArgs, q.Xrate)
			//valueArgs = append(valueArgs, q.Varate)
			//valueArgs = append(valueArgs, q.Udate)
			//valueArgs = append(valueArgs, q.Utime)
			//code = q.Code
			klid++
		}
		stmt := fmt.Sprintf("INSERT INTO %s (code,date,klid,open,high,close,low,"+
			"volume,amount,xrate,varate,udate,utime) VALUES %s on duplicate key update date=values(date),"+
			"open=values(open),high=values(high),close=values(close),low=values(low),"+
			"volume=values(volume),amount=values(amount),xrate=values(xrate),varate=values(varate),udate=values"+
			"(udate),utime=values(utime)",
			table, strings.Join(valueStrings, ","))
		_, err := dbmap.Exec(stmt, valueArgs...)
		util.CheckErr(err, code+" failed to bulk insert "+table)
	}
}

//QQJson represents data structure fetched from QQ fincance. Must set Code and Period before unmarshalling json data
type QQJson struct {
	Fcode, Code, Period, Reinstate string
	TradeData                      *TradeData
}

//UnmarshalJSON unmarshals JSON payload to the struct
func (qj *QQJson) UnmarshalJSON(b []byte) error {
	var (
		f      interface{}
		m      map[string]interface{}
		retcde float64
		msg    string
		ok     bool
		e      error
	)
	e = json.Unmarshal(b, &f)
	if e != nil {
		return errors.Wrapf(e, "%s %s failed to unmarshal json data", qj.Code, qj.Period)
	}
	if m, ok = f.(map[string]interface{}); !ok {
		return errors.Errorf("unrecognized data structure, cant't cast to map: %+v", f)
	}
	retcde = m["code"].(float64)
	msg = m["msg"].(string)
	if retcde != 0 || msg != "" {
		return errors.Errorf("server failed with code %f, msg: %s", retcde, msg)
	}
	var (
		cdat   interface{}
		exists bool
	)
	if cdat, exists = m["data"].(map[string]interface{})[qj.Fcode]; !exists {
		return errors.Errorf("unrecognized data structure, can't find 'data' node or '%s': %+v", qj.Fcode, f)
	}
	pdat, exists := cdat.(map[string]interface{})[qj.Reinstate+qj.Period]
	if !exists {
		// for newly stocks no reinstatement type as prefix
		pdat, exists = cdat.(map[string]interface{})[qj.Period]
		if !exists {
			return errors.Errorf("unrecognized data structure, can't find %s %s: %+v", qj.Reinstate, qj.Period, f)
		}
	}
	ps := pdat.([]interface{})
	qj.TradeData = new(TradeData)
	for i, pd := range ps {
		pa := pd.([]interface{})
		q := new(TradeDataBase)
		q.Code = qj.Code
		q.Date = pa[0].(string)
		q.Open, e = strconv.ParseFloat(pa[1].(string), 64)
		if e != nil {
			return errors.Wrapf(e, "failed to parse OPEN value at index %d", i)
		}
		q.Close, e = strconv.ParseFloat(pa[2].(string), 64)
		if e != nil {
			return errors.Wrapf(e, "failed to parse CLOSE value at index %d", i)
		}
		q.High, e = strconv.ParseFloat(pa[3].(string), 64)
		if e != nil {
			return errors.Wrapf(e, "failed to parse HIGH value at index %d", i)
		}
		q.Low, e = strconv.ParseFloat(pa[4].(string), 64)
		if e != nil {
			return errors.Wrapf(e, "failed to parse LOW value at index %d", i)
		}
		q.Volume.Valid = true
		q.Volume.Float64, e = strconv.ParseFloat(pa[5].(string), 64)
		if e != nil {
			return errors.Wrapf(e, "failed to parse Volume value at index %d", i)
		}
		q.Volume.Float64 *= 100.
		qj.TradeData.Base = append(qj.TradeData.Base, q)
	}
	return nil
}

// IdxLst Index List
type IdxLst struct {
	Code, Name, Src string
}

// FinPredict financial prediction
type FinPredict struct {
	Code      string
	Year      string
	EpsNum    sql.NullInt64   `db:"eps_num"`
	EpsMin    sql.NullFloat64 `db:"eps_min"`
	EpsAvg    sql.NullFloat64 `db:"eps_avg"`
	EpsMax    sql.NullFloat64 `db:"eps_max"`
	EpsIndAvg sql.NullFloat64 `db:"eps_ind_avg"`
	EpsUpRt   sql.NullFloat64 `db:"eps_up_rt"`
	EpsDnRt   sql.NullFloat64 `db:"eps_dn_rt"`
	NpUpRt    sql.NullFloat64 `db:"np_up_rt"`
	NpDnRt    sql.NullFloat64 `db:"np_dn_rt"`
	NpNum     sql.NullInt64   `db:"np_num"`
	NpMin     sql.NullFloat64 `db:"np_min"`
	NpAvg     sql.NullFloat64 `db:"np_avg"`
	NpMax     sql.NullFloat64 `db:"np_max"`
	NpIndAvg  sql.NullFloat64 `db:"np_ind_avg"`
	Udate     sql.NullString
	Utime     sql.NullString
}

//KeyPoint mapped to database table kpts.
type KeyPoint struct {
	UUID     string
	Code     string
	Klid     int
	Date     string
	Score    float64
	SumFall  float64         `db:"sum_fall"`
	RgnRise  float64         `db:"rgn_rise"`
	RgnLen   int             `db:"rgn_len"`
	UnitRise float64         `db:"unit_rise"`
	Clr      sql.NullFloat64 // Compound Log Return
	RemaLr   sql.NullFloat64 `db:"rema_lr"` // Reversal EMA Log Return
	Flag     sql.NullString
	Udate    string
	Utime    string
}

//GraderStats represents grader statistics of specific time frame.
type GraderStats struct {
	Grader    string
	Frame     int
	Score     float64
	Threshold sql.NullFloat64
	UUID      sql.NullString
	Size      int
	Udate     sql.NullString
	Utime     sql.NullString
}

//XCorlTrn represents cross correlation training samples.
type XCorlTrn struct {
	UUID  string
	Code  string
	Klid  int
	Date  string
	Rcode string
	Corl  float64
	Flag  sql.NullString
	Udate sql.NullString
	Utime sql.NullString
}

func (x *XCorlTrn) String() string {
	return toJSONString(x)
}

//WccTrn represents Warping Correlation Coefficient training samples.
type WccTrn struct {
	UUID    int
	Code    string
	Klid    int
	Date    string
	Rcode   string
	Corl    float64
	CorlStz sql.NullFloat64 `db:"corl_stz"`
	MinDiff float64         `db:"min_diff"`
	MaxDiff float64         `db:"max_diff"`
	Flag    sql.NullString
	Bno     sql.NullInt64
	Udate   sql.NullString
	Utime   sql.NullString
}

func (x *WccTrn) String() string {
	return toJSONString(x)
}

//StockRel represents stock relations regarding the correlation coefficients at different times.
type StockRel struct {
	Code        string
	Date        sql.NullString
	Klid        int
	RcodePos    sql.NullString
	RcodePosHs  sql.NullString
	RcodeNeg    sql.NullString
	RcodeNegHs  sql.NullString
	PosCorl     sql.NullFloat64
	PosCorlHs   sql.NullFloat64
	NegCorl     sql.NullFloat64
	NegCorlHs   sql.NullFloat64
	RcodeSize   sql.NullInt64
	RcodeSizeHs sql.NullInt64
	Udate       sql.NullString
	Utime       sql.NullString
}

func (x *StockRel) String() string {
	return toJSONString(x)
}

//FsStats represents feature scaling statistics. A mapping of the fs_stats table
type FsStats struct {
	Method string
	Fields string
	Tab    sql.NullString
	Mean   sql.NullFloat64
	Std    sql.NullFloat64
	Vmax   sql.NullFloat64
	Udate  sql.NullString
	Utime  sql.NullString
}

func (x *FsStats) String() string {
	return toJSONString(x)
}

//WccInferRecord represents the wcc inference record in a WccInferResult.
type WccInferRecord struct {
	Code     string  `json:"code"`
	Klid     int     `json:"klid"`
	Positive string  `json:"positive"`
	Pcorl    float64 `json:"pcorl"`
	Negative string  `json:"negative"`
	Ncorl    float64 `json:"ncorl"`
}

func (x *WccInferRecord) String() string {
	return toJSONString(x)
}

//WccInferResult represents the wcc inference result file, in json format.
type WccInferResult struct {
	Records []*WccInferRecord `json:"records"`
}

func (x *WccInferResult) String() string {
	return toJSONString(x)
}

//XqSharesChg xueqiu shares change json payload.
type XqSharesChg struct {
	ErrorCode int    `json:"error_code"`
	ErrorDesc string `json:"error_description"`
	Data      struct {
		Items []struct {
			ChgDate                *float64 `json:"chg_date,omitempty"`
			TotalShare             *float64 `json:"total_shares,omitempty"`
			FloatShare             *float64 `json:"float_shares,omitempty"`
			FloatAShare            *float64 `json:"float_shares_float_ashare,omitempty"`
			FloatBShare            *float64 `json:"float_shares_float_bshare,omitempty"`
			ChgReasonID            *string  `json:"chg_reason_identifier,omitempty"`
			RestrictedShare        *float64 `json:"restricted_share,omitempty"`
			LimitAShare            *float64 `json:"limit_shares_limit_ashare,omitempty"`
			NationalLimitAShare    *float64 `json:"national_held_limit_ashare,omitempty"`
			SoapLimitAShare        *float64 `json:"soap_held_limit_ashare,omitempty"`
			DomesticLimitAShare    *float64 `json:"domestic_held_limit_ashare,omitempty"`
			DomesticCorpAShare     *float64 `json:"ashare_domestic_corp_held,omitempty"`
			DomesticNpLimitAShare  *float64 `json:"domestic_np_held_limit_ashare,omitempty"`
			ExecutiveLimitAShare   *float64 `json:"executive_held_limit_ashare,omitempty"`
			OrgLimitAShare         *float64 `json:"org_place_limit_ashare,omitempty"`
			FrgnLimitAShare        *float64 `json:"frgn_capital_held_limit_ashare,omitempty"`
			FrgnCorpAShare         *float64 `json:"ashare_frgn_corp_held,omitempty"`
			FrgnNpLimitAShare      *float64 `json:"frgn_np_held_limit_ashare,omitempty"`
			LimitBShare            *float64 `json:"limit_shares_limit_bshare,omitempty"`
			LimitHShare            *float64 `json:"limit_shares_limit_hshare,omitempty"`
			FloatHShare            *float64 `json:"float_shares_float_hshare,omitempty"`
			OthFloatShare          *float64 `json:"othr_float_shares,omitempty"`
			OVListFloatShare       *float64 `json:"overseas_listed_float_share,omitempty"`
			NeeqAShareFloat        *float64 `json:"neeq_ashare_float_shares,omitempty"`
			NeeqBShareFloat        *float64 `json:"neeq_bshare_float_shares,omitempty"`
			UnfloatShares          *float64 `json:"unfloat_shares,omitempty"`
			DomesticSponsorShareUS *float64 `json:"domestic_sponsor_shares_us,omitempty"`
			NationalHeld           *float64 `json:"national_held,omitempty"`
			StateOwnedCorpHeldUS   *float64 `json:"state_owned_corp_held_us,omitempty"`
			DomesticCorpHeldUS     *float64 `json:"domestic_corp_held_us,omitempty"`
			NaturalPersonHeldUS    *float64 `json:"natural_personel_held_us,omitempty"`
			RaiseCorpShareUS       *float64 `json:"raise_corp_share_us,omitempty"`
			NormalCorpShareUS      *float64 `json:"normal_corp_share_us,omitempty"`
			UnlistedFrgnCapitalUS  *float64 `json:"unlisted_frgn_capital_stock_us,omitempty"`
			StaffShareUS           *float64 `json:"staff_share_us,omitempty"`
			PreferredShareEtcUS    *float64 `json:"prefered_share_etc_us,omitempty"`
			ConversionShareUS      *float64 `json:"conversion_share_us,omitempty"`
			NaturalPersonShareUS   *float64 `json:"natual_person_share_us,omitempty"`
			StrategicInvestorUS    *float64 `json:"stragetic_investor_place_us,omitempty"`
			FundShareUS            *float64 `json:"fund_place_shares_us,omitempty"`
			NormalCorpPlaceShareUS *float64 `json:"normal_corp_place_share_us,omitempty"`
			OrigFloatShareUS       *float64 `json:"orig_staq_float_share_us,omitempty"`
			OrigNetFLoatShareUS    *float64 `json:"orig_net_float_share_us,omitempty"`
			OthUnfloatShareUS      *float64 `json:"other_unfloat_share_us,omitempty"`
			ChgReason              *string  `json:"chg_reason,omitempty"`
		} `json:"items"`
	} `json:"data"`
}

func (x *XqSharesChg) String() string {
	return toJSONString(x)
}

func toJSONString(i interface{}) string {
	j, e := json.Marshal(i)
	if e != nil {
		fmt.Println(e)
	}
	return fmt.Sprintf("%v", string(j))
}
