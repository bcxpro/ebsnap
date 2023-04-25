package cmd

import (
	"ebsnapshot/globals"

	"github.com/spf13/cobra"
)

func NewVersionCommand() *cobra.Command {

	var cmd = &cobra.Command{
		Use:   "version",
		Short: "Returns the version of this command.",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Println(globals.VersionInfo)
		},
	}

	return cmd
}
