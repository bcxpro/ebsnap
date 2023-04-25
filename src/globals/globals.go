package globals

import (
	"os"
	"path/filepath"

	"ebsnapshot/version"
)

var (
	VersionInfo = version.NewInfo(filepath.Base(os.Args[0]))
)