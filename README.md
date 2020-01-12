# Stock

China A-share analysis tool written in go

## Overall Design

- a web crawler to fetch data from stock exchange
- a mapper to store in database (MySQL)
- methods and functions apply on offline data for regression

## Data Source

- http://10jqka.com.cn
- http://sse.com.cn


## Dependencies

- [golang](https://golang.org/) version >= 1.13.6
- [sirupsen/logrus](https://github.com/sirupsen/logrus) for logging
- [spf13/cobra](https://github.com/spf13/cobra") for cli interfaces

## Usage

