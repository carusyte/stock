CREATE DATABASE `secu` /*!40100 DEFAULT CHARACTER SET utf8 */;

CREATE TABLE `basics` (
  `code` varchar(6) NOT NULL COMMENT '股票代码',
  `name` varchar(10) DEFAULT NULL COMMENT '名称',
  `market` varchar(2) DEFAULT NULL COMMENT '市场',
  `industry` varchar(20) DEFAULT NULL COMMENT '所属行业',
  `area` varchar(20) DEFAULT NULL COMMENT '地区',
  `pe` double DEFAULT NULL COMMENT '市盈率',
  `pu` double DEFAULT NULL COMMENT 'Price / UDPPS',
  `po` double DEFAULT NULL COMMENT 'Price / OCFPS',
  `outstanding` double DEFAULT NULL COMMENT '流通股本（亿）',
  `totals` double DEFAULT NULL COMMENT '总股本（亿）',
  `totalAssets` double DEFAULT NULL COMMENT '总资产（万）',
  `liquidAssets` double DEFAULT NULL COMMENT '流动资产',
  `fixedAssets` double DEFAULT NULL COMMENT '固定资产',
  `reserved` double DEFAULT NULL COMMENT '公积金',
  `reservedPerShare` double DEFAULT NULL COMMENT '每股公积金',
  `esp` double DEFAULT NULL COMMENT '每股收益',
  `bvps` double DEFAULT NULL COMMENT '每股净资',
  `pb` double DEFAULT NULL COMMENT '市净率',
  `timeToMarket` varchar(10) DEFAULT NULL COMMENT '上市日期',
  `undp` double DEFAULT NULL COMMENT '未分配利润',
  `perundp` double DEFAULT NULL COMMENT '每股未分配利润',
  `rev` double DEFAULT NULL COMMENT '收入同比（%）',
  `profit` double DEFAULT NULL COMMENT '利润同比（%）',
  `gpr` double DEFAULT NULL COMMENT '毛利率（%）',
  `npr` double DEFAULT NULL COMMENT '净利润率（%）',
  `holders` bigint(20) DEFAULT NULL COMMENT '股东人数',
  `price` decimal(6,3) DEFAULT NULL COMMENT '现价',
  `varate` decimal(4,2) DEFAULT NULL COMMENT '涨跌幅（%）',
  `var` decimal(6,3) DEFAULT NULL COMMENT '涨跌',
  `xrate` decimal(5,2) DEFAULT NULL COMMENT '换手率（%）',
  `volratio` decimal(10,2) DEFAULT NULL COMMENT '量比',
  `ampl` decimal(5,2) DEFAULT NULL COMMENT '振幅（%）',
  `turnover` decimal(10,5) DEFAULT NULL COMMENT '成交额（亿）',
  `accer` decimal(5,2) DEFAULT NULL COMMENT '涨速（%）',
  `circMarVal` decimal(10,2) DEFAULT NULL COMMENT '流通市值',
  `udate` varchar(10) DEFAULT NULL COMMENT '最后更新日期',
  `utime` varchar(8) DEFAULT NULL COMMENT '最后更新时间',
  PRIMARY KEY (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `finance` (
  `code` varchar(8) NOT NULL COMMENT '股票代码',
  `year` varchar(10) NOT NULL COMMENT '报告年度',
  `eps` double DEFAULT NULL COMMENT '基本每股收益(元)',
  `eps_yoy` double DEFAULT NULL COMMENT '基本每股收益同比增长率',
  `np` double DEFAULT NULL COMMENT '净利润(亿)',
  `np_yoy` double DEFAULT NULL COMMENT '净利润同比增长率',
  `np_rg` double DEFAULT NULL COMMENT '净利润环比增长率',
  `np_adn` double DEFAULT NULL COMMENT '扣非净利润(亿)',
  `np_adn_yoy` double DEFAULT NULL COMMENT '扣非净利润同比增长率',
  `gr` double DEFAULT NULL COMMENT '营业总收入(亿)',
  `gr_yoy` double DEFAULT NULL COMMENT '营业总收入同比增长率',
  `navps` double DEFAULT NULL COMMENT '每股净资产(元)',
  `roe` double DEFAULT NULL COMMENT '净资产收益率',
  `roe_yoy` double DEFAULT NULL COMMENT '净资产收益率同比增长率',
  `roe_dlt` double DEFAULT NULL COMMENT '净资产收益率-摊薄',
  `dar` double DEFAULT NULL COMMENT 'Debt to Asset Ratio, 资产负债比率',
  `crps` double DEFAULT NULL COMMENT '每股资本公积金(元)',
  `udpps` double DEFAULT NULL COMMENT '每股未分配利润(元)',
  `udpps_yoy` double DEFAULT NULL COMMENT '每股未分配利润同比增长率',
  `ocfps` double DEFAULT NULL COMMENT '每股经营现金流(元)',
  `ocfps_yoy` double DEFAULT NULL COMMENT '每股经营现金流同比增长率',
  `gpm` double DEFAULT NULL COMMENT '销售毛利率',
  `npm` double DEFAULT NULL COMMENT '销售净利率',
  `itr` double DEFAULT NULL COMMENT '存货周转率',
  `udate` varchar(10) DEFAULT NULL COMMENT '更新日期',
  `utime` varchar(8) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`code`,`year`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='财务信息';

CREATE TABLE `idxlst` (
  `code` varchar(8) NOT NULL COMMENT '代码',
  `name` varchar(10) NOT NULL COMMENT '指数名称',
  `src` varchar(60) DEFAULT NULL COMMENT '来源',
  PRIMARY KEY (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='指数列表';

CREATE TABLE `indc_feat` (
  `indc` varchar(10) NOT NULL COMMENT '指标类型',
  `fid` varchar(50) NOT NULL COMMENT '特征ID(UUID)',
  `cytp` varchar(5) NOT NULL COMMENT '周期类型（D:天/W:周/M:月）',
  `bysl` varchar(2) NOT NULL COMMENT 'BY：买/SL：卖',
  `smp_num` int(3) NOT NULL COMMENT '采样数量',
  `fd_num` int(10) NOT NULL COMMENT '同类样本数量',
  `weight` double DEFAULT NULL COMMENT '权重',
  `remarks` varchar(200) DEFAULT NULL COMMENT '备注',
  `udate` varchar(10) NOT NULL COMMENT '更新日期',
  `utime` varchar(8) NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`indc`,`cytp`,`bysl`,`smp_num`,`fid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='指标特征数据总表';

CREATE TABLE `indc_feat_raw` (
  `code` varchar(8) NOT NULL COMMENT '股票代码',
  `indc` varchar(10) NOT NULL COMMENT '指标类型',
  `fid` varchar(15) NOT NULL COMMENT '特征ID(周期+买卖+采样起始日期)',
  `cytp` varchar(5) NOT NULL COMMENT '周期类型（D:天/W:周/M:月）',
  `bysl` varchar(2) NOT NULL COMMENT 'BY：买/SL：卖',
  `smp_date` varchar(10) NOT NULL COMMENT '采样开始日期',
  `smp_num` int(3) NOT NULL COMMENT '采样数量',
  `mark` double DEFAULT NULL COMMENT '标记获利/亏损幅度',
  `tspan` int(11) DEFAULT NULL COMMENT '获利/亏损的时间跨度',
  `mpt` double DEFAULT NULL COMMENT '单位时间的获利/亏损（Mark/TSpan）',
  `remarks` varchar(200) DEFAULT NULL COMMENT '备注',
  `udate` varchar(10) NOT NULL COMMENT '更新日期',
  `utime` varchar(8) NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`code`,`fid`,`indc`),
  KEY `INDEX` (`smp_num`,`cytp`,`bysl`,`indc`,`smp_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='指标特征原始数据总表';

CREATE TABLE `indicator_d` (
  `Code` varchar(8) NOT NULL,
  `Date` varchar(10) NOT NULL,
  `Klid` int(11) NOT NULL,
  `KDJ_K` decimal(6,3) DEFAULT NULL,
  `KDJ_D` decimal(6,3) DEFAULT NULL,
  `KDJ_J` decimal(6,3) DEFAULT NULL,
  `udate` varchar(10) DEFAULT NULL COMMENT '更新日期',
  `utime` varchar(8) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`Code`,`Klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `indicator_m` (
  `Code` varchar(8) NOT NULL,
  `Date` varchar(10) NOT NULL,
  `Klid` int(11) NOT NULL,
  `KDJ_K` decimal(6,3) DEFAULT NULL,
  `KDJ_D` decimal(6,3) DEFAULT NULL,
  `KDJ_J` decimal(6,3) DEFAULT NULL,
  `udate` varchar(10) DEFAULT NULL COMMENT '更新日期',
  `utime` varchar(8) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`Code`,`Klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `indicator_w` (
  `Code` varchar(8) NOT NULL,
  `Date` varchar(10) NOT NULL,
  `Klid` int(11) NOT NULL,
  `KDJ_K` decimal(6,3) DEFAULT NULL,
  `KDJ_D` decimal(6,3) DEFAULT NULL,
  `KDJ_J` decimal(6,3) DEFAULT NULL,
  `udate` varchar(10) DEFAULT NULL COMMENT '更新日期',
  `utime` varchar(8) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`Code`,`Klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `kdj_feat_dat` (
  `fid` varchar(50) NOT NULL COMMENT '特征ID',
  `seq` int(11) NOT NULL COMMENT '序号',
  `K` double NOT NULL,
  `D` double NOT NULL,
  `J` double NOT NULL,
  `udate` varchar(10) NOT NULL COMMENT '更新日期',
  `utime` varchar(8) NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`fid`,`seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='KDJ指标特征数据';

CREATE TABLE `kdj_feat_dat_raw` (
  `code` varchar(8) NOT NULL COMMENT '股票代码',
  `fid` varchar(15) NOT NULL COMMENT '特征ID',
  `klid` int(11) NOT NULL COMMENT '序号',
  `K` double NOT NULL,
  `D` double NOT NULL,
  `J` double NOT NULL,
  `udate` varchar(10) NOT NULL COMMENT '更新日期',
  `utime` varchar(8) NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`code`,`fid`,`klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='KDJ指标特征原始数据';

CREATE TABLE `kdjv_stats` (
  `code` varchar(8) NOT NULL COMMENT '股票代码',
  `dod` double DEFAULT NULL COMMENT 'Degree of Distinction',
  `sl` double DEFAULT NULL COMMENT 'Sell Low',
  `sh` double DEFAULT NULL COMMENT 'Sell High',
  `bl` double DEFAULT NULL COMMENT 'Buy Low',
  `bh` double DEFAULT NULL COMMENT 'Buy High',
  `sor` double DEFAULT NULL COMMENT 'Sell Overlap Ratio',
  `bor` double DEFAULT NULL COMMENT 'Buy Overlap Ratio',
  `scnt` int(5) DEFAULT NULL COMMENT 'Sell Count',
  `bcnt` int(5) DEFAULT NULL COMMENT 'Buy Count',
  `smean` double DEFAULT NULL COMMENT 'Sell Mean',
  `bmean` double DEFAULT NULL COMMENT 'Buy Mean',
  `frmdt` varchar(10) DEFAULT NULL COMMENT 'Data Date Start',
  `todt` varchar(10) DEFAULT NULL COMMENT 'Data Date End',
  `udate` varchar(10) DEFAULT NULL COMMENT 'Update Date',
  `utime` varchar(8) DEFAULT NULL COMMENT 'Update Time',
  PRIMARY KEY (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='KDJV Scorer Performance Statistics';

CREATE TABLE `kline_60m` (
  `code` varchar(8) NOT NULL,
  `date` varchar(20) NOT NULL,
  `time` varchar(8) NOT NULL,
  `klid` int(11) NOT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `volume` double DEFAULT NULL,
  `amount` double DEFAULT NULL,
  `xrate` double DEFAULT NULL,
  `varate` double DEFAULT NULL COMMENT '涨跌幅(%)',
  `ma5` double DEFAULT NULL,
  `ma10` double DEFAULT NULL,
  `ma20` double DEFAULT NULL,
  `ma30` double DEFAULT NULL,
  `udate` varchar(10) DEFAULT NULL COMMENT '更新日期',
  `utime` varchar(8) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`code`,`klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='60分钟K线（前复权）';

CREATE TABLE `kline_d` (
  `code` varchar(8) NOT NULL,
  `date` varchar(20) NOT NULL,
  `klid` int(11) NOT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `volume` double DEFAULT NULL,
  `amount` double DEFAULT NULL,
  `xrate` double DEFAULT NULL,
  `varate` double DEFAULT NULL COMMENT '涨跌幅(%)',
  `udate` varchar(10) DEFAULT NULL COMMENT '更新日期',
  `utime` varchar(8) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`code`,`klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='日K线（前复权）';

CREATE TABLE `kline_d_n` (
  `code` varchar(8) NOT NULL,
  `date` varchar(20) NOT NULL,
  `klid` int(11) NOT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `volume` double DEFAULT NULL,
  `amount` double DEFAULT NULL,
  `factor` double DEFAULT NULL,
  `xrate` double DEFAULT NULL,
  `varate` double DEFAULT NULL COMMENT '涨跌幅(%)',
  `udate` varchar(10) DEFAULT NULL COMMENT '更新日期',
  `utime` varchar(8) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`code`,`klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='日K线（不复权）';

CREATE TABLE `kline_m` (
  `Code` varchar(8) NOT NULL,
  `Date` varchar(10) NOT NULL,
  `Klid` int(11) NOT NULL,
  `Open` double DEFAULT NULL,
  `High` double DEFAULT NULL,
  `Close` double DEFAULT NULL,
  `Low` double DEFAULT NULL,
  `Volume` double DEFAULT NULL,
  `Amount` double DEFAULT NULL,
  `Xrate` double DEFAULT NULL,
  `varate` double DEFAULT NULL COMMENT '涨跌幅(%)',
  `udate` varchar(10) DEFAULT NULL COMMENT '更新日期',
  `utime` varchar(8) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`Code`,`Klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `kline_w` (
  `Code` varchar(8) NOT NULL,
  `Date` varchar(10) NOT NULL,
  `Klid` int(11) NOT NULL,
  `Open` double DEFAULT NULL,
  `High` double DEFAULT NULL,
  `Close` double DEFAULT NULL,
  `Low` double DEFAULT NULL,
  `Volume` double DEFAULT NULL,
  `Amount` double DEFAULT NULL,
  `Xrate` double DEFAULT NULL,
  `varate` double DEFAULT NULL COMMENT '涨跌幅(%)',
  `udate` varchar(10) DEFAULT NULL COMMENT '更新日期',
  `utime` varchar(8) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`Code`,`Klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `stats` (
  `code` varchar(6) NOT NULL,
  `start` varchar(20) DEFAULT NULL,
  `end` varchar(20) DEFAULT NULL,
  `dur` decimal(10,5) DEFAULT NULL,
  PRIMARY KEY (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `tradecal` (
  `index` bigint(20) DEFAULT NULL,
  `calendarDate` date DEFAULT NULL,
  `isOpen` int(11) DEFAULT NULL,
  `udate` varchar(10) DEFAULT NULL COMMENT '更新日期',
  `utime` varchar(8) DEFAULT NULL COMMENT '更新时间',
  KEY `ix_tradecal_index` (`index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `xdxr` (
  `code` varchar(6) NOT NULL COMMENT '股票代码',
  `name` varchar(10) DEFAULT NULL COMMENT '股票名称',
  `idx` int(10) NOT NULL COMMENT '序号',
  `notice_date` varchar(10) DEFAULT NULL COMMENT '公告日期',
  `report_year` varchar(10) DEFAULT NULL COMMENT '报告期',
  `board_date` varchar(10) DEFAULT NULL COMMENT '董事会日期',
  `gms_date` varchar(10) DEFAULT NULL COMMENT '股东大会日期',
  `impl_date` varchar(10) DEFAULT NULL COMMENT '实施日期',
  `plan` varchar(300) DEFAULT NULL COMMENT '分红方案说明',
  `divi` double DEFAULT NULL COMMENT '分红金额（每10股）',
  `divi_atx` double DEFAULT NULL COMMENT '每10股现金(税后)',
  `dyr` double DEFAULT NULL COMMENT '股息率(Dividend Yield Ratio)',
  `dpr` double DEFAULT NULL COMMENT '股利支付率(Dividend Payout Ratio)',
  `divi_end_date` varchar(10) DEFAULT NULL COMMENT '分红截止日期',
  `shares_allot` double DEFAULT NULL COMMENT '每10股送红股',
  `shares_allot_date` varchar(10) DEFAULT NULL COMMENT '红股上市日',
  `shares_cvt` double DEFAULT NULL COMMENT '每10股转增股本',
  `shares_cvt_date` varchar(10) DEFAULT NULL COMMENT '转增股本上市日',
  `reg_date` varchar(10) DEFAULT NULL COMMENT '股权登记日',
  `xdxr_date` varchar(10) DEFAULT NULL COMMENT '除权除息日',
  `payout_date` varchar(10) DEFAULT NULL COMMENT '股息到帐日',
  `progress` varchar(45) DEFAULT NULL COMMENT '方案进度',
  `divi_target` varchar(45) DEFAULT NULL COMMENT '分红对象',
  `shares_base` bigint(20) DEFAULT NULL COMMENT '派息股本基数',
  `end_trddate` varchar(10) DEFAULT NULL COMMENT '最后交易日',
  `xprice` varchar(1) DEFAULT NULL COMMENT '是否已更新过前复权价格信息',
  `udate` varchar(10) DEFAULT NULL COMMENT '更新日期',
  `utime` varchar(8) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`code`,`idx`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `fin_predict` (
  `code` varchar(8) NOT NULL COMMENT '股票代码',
  `year` varchar(4) NOT NULL COMMENT '年份',
  `eps_num` int(11) DEFAULT NULL COMMENT '每股收益预测机构数',
  `eps_min` double DEFAULT NULL COMMENT '每股收益最小值',
  `eps_avg` double DEFAULT NULL COMMENT '每股收益平均值',
  `eps_max` double DEFAULT NULL COMMENT '每股收益最大值',
  `eps_ind_avg` double DEFAULT NULL COMMENT '每股收益行业平均',
  `eps_up_rt` double DEFAULT NULL COMMENT 'EPS预测上调机构占比',
  `eps_dn_rt` double DEFAULT NULL COMMENT 'EPS预测下调机构占比',
  `np_num` int(11) DEFAULT NULL COMMENT '净利润预测机构数',
  `np_min` double DEFAULT NULL COMMENT '净利润最小值 (亿元）',
  `np_avg` double DEFAULT NULL COMMENT '净利润平均值 (亿元）',
  `np_max` double DEFAULT NULL COMMENT '净利润最大值 (亿元）',
  `np_ind_avg` double DEFAULT NULL COMMENT '净利润行业平均值 (亿元）',
  `np_up_rt` double DEFAULT NULL COMMENT '净利润预测上调机构占比',
  `np_dn_rt` double DEFAULT NULL COMMENT '净利润预测下调机构占比',
  `udate` varchar(10) DEFAULT NULL COMMENT '更新日期',
  `utime` varchar(8) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`code`,`year`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='业绩预测简表';

