package main

import (
	"log"
	"time"
)

func main() {
	t1, _ := time.Parse("2000-01-01 00:00:00", "2017-01-01 00:00:00")
	t2, _ := time.Parse("2000-01-01", "2017-01-01")
	log.Println(t1.Before(t2))
	log.Println(t1.After(t2))
	log.Println(t1.Equal(t2))
}
