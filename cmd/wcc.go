package cmd

import (
	"github.com/carusyte/stock/sampler"

	"github.com/spf13/cobra"
)

func init() {
	wccCmd.AddCommand(updateWccCmd)
}

var wccCmd = &cobra.Command{
	Use:   "wcc",
	Short: "Process Warping Correlation Coefficient sampling.",
}

var updateWccCmd = &cobra.Command{
	Use:   "update",
	Short: "Update fields in the wcc_trn table.",
	Run: func(cmd *cobra.Command, args []string) {
		sampler.UpdateWcc()
	},
}
