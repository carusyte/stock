package cmd

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/getd"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/score"
	"github.com/spf13/cobra"
)

var (
	scorer string
)

func init() {
	scoreCmd.Flags().StringVarP(&scorer, "scorer", "s", "",
		"specify the scorer to score the stocks.")
	scoreCmd.MarkFlagRequired("scorer")
	rootCmd.AddCommand(scoreCmd)
}

var scoreCmd = &cobra.Command{
	Use:   "score",
	Short: "Score stocks using specified scorer.",
	Run: func(cmd *cobra.Command, args []string) {
		s := strings.ToLower(scorer)
		switch s {
		case "blue":
			blue()
		case "empirical":
			empirical()
		case "kdjfirst":
			kdjFirst()
		case "kdjonly":
			kdjOnly()
		case "hidbluekdjst":
			hidBlueKdjSt()
		case "bluekdjv":
			blueKdjv()
		default:
			log.Panicf("unsupported scorer: %s", s)
		}
	},
}

func kdjOnly(code ...string) {
	start := time.Now()
	r1 := new(score.KdjV).Get(code, -1, true)
	log.Printf("\n%+v", r1)
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func kdjFirst() {
	start := time.Now()
	r1 := new(score.KdjV).Geta().Sort().Shrink(50)
	r2 := new(score.HiD).Get(r1.Stocks(), -1, false)
	r2.Weight = 0.2
	r3 := new(score.BlueChip).Get(r1.Stocks(), -1, false)
	r3.Weight = 0.8
	log.Printf("\n%+v", score.Combine(r2, r3, r1).Sort())
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func empirical() {
	start := time.Now()
	kdjv := new(score.KdjV)
	idxlst, e := getd.GetIdxLst()
	if e != nil {
		panic(e)
	}
	idxc := make([]string, len(idxlst))
	for i, idx := range idxlst {
		idxc[i] = idx.Code
	}
	r1 := new(score.HiD).Geta()
	r1.Weight = 0.5
	r2 := new(score.BlueChip).Geta()
	r2.Weight = 0.5
	r1r2 := score.Combine(r1, r2).Sort().Shrink(300)
	r1r2.Weight = 0
	r3 := kdjv.Get(r1r2.Stocks(), -1, false)
	r3.Weight = 1
	log.Printf("\n%+v", kdjv.Get(idxc, -1, false))
	fmt.Println()
	log.Printf("\n%+v", score.Combine(r1r2, r3).Sort())
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func blue() {
	r := new(score.BlueChip).Get(nil, -1, true)
	log.Printf("\n%+v", r)
}

func blueKdjv() {
	start := time.Now()
	r2 := new(score.BlueChip).Geta().Sort().Shrink(1000)
	r2.Weight = 0
	r3 := new(score.KdjV).Get(r2.Stocks(), -1, false)
	r3.Weight = 1
	log.Printf("\n%+v", score.Combine(r2, r3).Sort())
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func hidBlueKdjSt() {
	start := time.Now()
	kdjst := new(score.KdjSt)
	idxlst, e := getd.GetIdxLst()
	if e != nil {
		panic(e)
	}
	idxc := make([]string, len(idxlst))
	for i, idx := range idxlst {
		idxc[i] = idx.Code
	}
	c, e := global.Dbmap.SelectInt("select round(count(*) * ? ) from basics",
		conf.Args.Scorer.HidBlueBaseRatio)
	if e != nil {
		log.Println("failed to count from basics")
		log.Println(e)
	}
	r1 := new(score.HiD).Geta()
	r1.Weight = 1. - conf.Args.Scorer.BlueWeight
	r2 := new(score.BlueChip).Geta()
	r2.Weight = conf.Args.Scorer.BlueWeight
	r1r2 := score.Combine(r1, r2).Sort().Shrink(int(c))
	n := int(math.Max(0, math.Floor(float64(c)*conf.Args.Scorer.HidBlueStarRatio)))
	r1r2.Mark(n, score.StarMark)
	n = -int(math.Max(0, math.Floor(float64(c)*conf.Args.Scorer.HidBlueRearWarnRatio)))
	r1r2.Mark(n, score.WarnMark)
	r1r2.Weight = 1. - conf.Args.Scorer.KdjStWeight
	r3 := kdjst.Get(r1r2.Stocks(), -1, false)
	r3.Weight = conf.Args.Scorer.KdjStWeight
	log.Printf("\n%+v", kdjst.Get(idxc, -1, false))
	fmt.Println()
	log.Printf("\n%+v", score.Combine(r1r2, r3).
		Sort().Highlight(conf.Args.Scorer.Highlight...))
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}
