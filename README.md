# Stock

China A-share analysis tool written in go

## Highlight Features


## Overall Design

- a web crawler to fetch data from stock exchange
- a mapper to store in database (MySQL)
- methods and functions apply on offline data for regression


## Data Source

- http://10jqka.com.cn
- http://sse.com.cn


## Dependencies

[golang](https://golang.org/) version >= 1.13.6

github.com
- [sirupsen/logrus](https://github.com/sirupsen/logrus) for logging
- [spf13/cobra](https://github.com/spf13/cobra") for cli interfaces

golang.org
- [x/text/encoding/simplifiedchinese](https://golang.org/x/text/encoding/simplifiedchinese)
- [x/text/encoding/transform](https://golang.org/x/text/encoding/transform)
- [x/net/proxy](https://golang.org/x/net/proxy)


## Usage

### Build

    # have go-lang installed, (v1.13.6)
    # have latest git installed (v2.24), consider use scoop to manage / install if using windows

    # this might take few minutes
    go get ...
    go build

### Run


## Change Log

## FAQ

fix golang/text by following command, ref https://github.com/golang/text
, or direclty remove correspoding package folder and rerun things like

    go get -u golang.org/x/text