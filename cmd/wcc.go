package cmd

import (
	"os"

	"github.com/carusyte/stock/sampler"

	"github.com/spf13/cobra"
)

var (
	expInferFile, upload, nocache bool
	localPath, rbase              string
)

func init() {
	pcalWccCmd.Flags().BoolVarP(&expInferFile, "exp", "e", true,
		"specify whether to export inference file")
	pcalWccCmd.Flags().BoolVarP(&upload, "upload", "u", true,
		"specify whether to upload inference file.")
	pcalWccCmd.Flags().BoolVarP(&nocache, "nocache", "n", true,
		"specify whether to delete local exported file after successful upload")
	pcalWccCmd.Flags().StringVarP(&localPath, "path", "p", os.TempDir(),
		"specify local directory for exported inference file")
	pcalWccCmd.Flags().StringVar(&rbase, "rbase", "",
		"specify remote base directory to upload the exported file")

	wccCmd.AddCommand(updateWccCmd)
	wccCmd.AddCommand(stzWccCmd)
	wccCmd.AddCommand(pcalWccCmd)
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

var stzWccCmd = &cobra.Command{
	Use:   "stz",
	Short: "Standardize corl value in the wcc_trn table.",
	Run: func(cmd *cobra.Command, args []string) {
		sampler.StzWcc()
	},
}

var pcalWccCmd = &cobra.Command{
	Use:   "pcal",
	Short: "Pre-calculate eligible wcc and optionally export and upload inference file for cloud inference.",
	Run: func(cmd *cobra.Command, args []string) {
		sampler.PcalWcc(expInferFile, upload, nocache, localPath, rbase)
	},
}
