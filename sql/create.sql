CREATE DATABASE `secu` /*!40100 DEFAULT CHARACTER SET utf8 */;

CREATE TABLE `basics` (
  `code` varchar(6) NOT NULL COMMENT '股票代码',
  `name` text COMMENT '名称',
  `industry` text COMMENT '所属行业',
  `area` text COMMENT '地区',
  `pe` double DEFAULT NULL COMMENT '市盈率',
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
  `timeToMarket` bigint(20) DEFAULT NULL COMMENT '上市日期',
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
  PRIMARY KEY (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `div` (
  `code` varchar(6) NOT NULL COMMENT '股票代码',
  `name` varchar(10) DEFAULT NULL COMMENT '股票名称',
  `index` int(10) NOT NULL COMMENT '序号',
  `report_year` varchar(10) DEFAULT NULL COMMENT '报告期',
  `board_date` varchar(100) DEFAULT NULL COMMENT '董事会日期',
  `gms_date` varchar(300) DEFAULT NULL COMMENT '股东大会日期',
  `impl_date` varchar(300) DEFAULT NULL COMMENT '实施日期',
  `plan` varchar(300) DEFAULT NULL COMMENT '分红方案说明',
  `divi` double DEFAULT NULL COMMENT '分红金额（每10股）',
  `shares` double DEFAULT NULL COMMENT '转增和送股数（每10股）',
  `record_date` varchar(10) DEFAULT NULL COMMENT '股权登记日',
  `xdxr_date` varchar(10) DEFAULT NULL COMMENT '除权除息日',
  `payout_date` varchar(45) DEFAULT NULL COMMENT '股派息日',
  `progress` varchar(45) DEFAULT NULL COMMENT '方案进度',
  `payout_ratio` double DEFAULT NULL COMMENT '股利支付率',
  `div_rate` double DEFAULT NULL COMMENT '分红率',
  PRIMARY KEY (`code`,`index`),
  KEY `ix_div_index` (`index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `indicator_d` (
  `Code` varchar(6) NOT NULL,
  `Date` varchar(10) NOT NULL,
  `Klid` int(11) NOT NULL,
  `KDJ_K` decimal(6,3) DEFAULT NULL,
  `KDJ_D` decimal(6,3) DEFAULT NULL,
  `KDJ_J` decimal(6,3) DEFAULT NULL,
  PRIMARY KEY (`Code`,`Klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `indicator_m` (
  `Code` varchar(6) NOT NULL,
  `Date` varchar(10) NOT NULL,
  `Klid` int(11) NOT NULL,
  `KDJ_K` decimal(6,3) DEFAULT NULL,
  `KDJ_D` decimal(6,3) DEFAULT NULL,
  `KDJ_J` decimal(6,3) DEFAULT NULL,
  PRIMARY KEY (`Code`,`Klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `indicator_w` (
  `Code` varchar(6) NOT NULL,
  `Date` varchar(10) NOT NULL,
  `Klid` int(11) NOT NULL,
  `KDJ_K` decimal(6,3) DEFAULT NULL,
  `KDJ_D` decimal(6,3) DEFAULT NULL,
  `KDJ_J` decimal(6,3) DEFAULT NULL,
  PRIMARY KEY (`Code`,`Klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `kline_d` (
  `code` varchar(6) NOT NULL,
  `date` varchar(20) NOT NULL,
  `klid` int(11) NOT NULL,
  `open` decimal(6,3) DEFAULT NULL,
  `high` decimal(6,3) DEFAULT NULL,
  `close` decimal(6,3) DEFAULT NULL,
  `low` decimal(6,2) DEFAULT NULL,
  `volume` decimal(12,0) DEFAULT NULL,
  `amount` decimal(14,2) DEFAULT NULL,
  `factor` decimal(8,3) DEFAULT NULL,
  `xrate` decimal(6,3) DEFAULT NULL,
  PRIMARY KEY (`code`,`date`,`klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `kline_m` (
  `Code` varchar(6) NOT NULL,
  `Date` varchar(10) NOT NULL,
  `Klid` int(11) NOT NULL,
  `Open` decimal(16,2) DEFAULT NULL,
  `High` decimal(16,2) DEFAULT NULL,
  `Close` decimal(16,2) DEFAULT NULL,
  `Low` decimal(16,2) DEFAULT NULL,
  `Volume` decimal(16,2) DEFAULT NULL,
  `Amount` decimal(16,2) DEFAULT NULL,
  `Xrate` decimal(8,3) DEFAULT NULL,
  PRIMARY KEY (`Code`,`Klid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `kline_w` (
  `Code` varchar(6) NOT NULL,
  `Date` varchar(10) NOT NULL,
  `Klid` int(11) NOT NULL,
  `Open` decimal(10,2) DEFAULT NULL,
  `High` decimal(10,2) DEFAULT NULL,
  `Close` decimal(10,2) DEFAULT NULL,
  `Low` decimal(10,2) DEFAULT NULL,
  `Volume` decimal(16,2) DEFAULT NULL,
  `Amount` decimal(16,2) DEFAULT NULL,
  `Xrate` decimal(6,3) DEFAULT NULL,
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
  KEY `ix_tradecal_index` (`index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

