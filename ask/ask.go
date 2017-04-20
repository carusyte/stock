package main

import (
	"fmt"
	"github.com/carusyte/stock/advisor"
	"log"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		log.Println("advisor is required")
		os.Exit(1)
	}

	var t *advisor.Table
	avr := advisor.New()
	arg := os.Args[1];

	switch {
	case strings.EqualFold("HiDivi", arg):
		t = avr.HiDivi(25)
	default:
		os.Exit(1)
	}

	fmt.Printf("%v", t)
}
