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
    # have latest git installed (v2.24)

    # this might take few minutes
    cd %GOPATH%\src\github.com\carusyte\stock
    go get ...

    # known issue https://github.com/grpc/grpc-go/issues/3312
    cd %GOPATH%\src\google.golang.org\grpc
    git revert 336cf8d       

    cd %GOPATH%\src\github.com\carusyte\stock
    go build



### Run

    go run main.go
                      


## FAQ

### can't find package xx at 'go get'
re-fetch via 'go get -u', or direclty remove correspoding package folder and run it again

    go get -u golang.org/x/text