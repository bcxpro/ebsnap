package version

import (
	"fmt"
)

var (
	defaultVersionString = "0.0.0-unknown"
	versionString        = ""
)

type Info struct {
	Application   string
	VersionString string
}

func NewInfo(application string) *Info {
	return &Info{
		Application:   application,
		VersionString: versionString,
	}
}

func (i *Info) String() string {
	return fmt.Sprintf("%v-%v", i.Application, i.VersionString)
}

func init() {
	if versionString == "" {
		versionString = defaultVersionString
	}
}
