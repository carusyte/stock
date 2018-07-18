package cmd

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "stock",
	Short: "Stock is a tool for China A-share market investment.",
	Long:  `Please provide subcommand to take further actions.`,
}

//Execute is the entrance of this command-line framework
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
