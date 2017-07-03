package getd

import (
	"testing"
	"log"
)

func TestGetStockInfo(t *testing.T){
	GetStockInfo()
}

func TestGetFromExchanges(t *testing.T) {
	allstk := getFromExchanges()
	log.Printf("found stocks: %d",allstk.Size())
}
