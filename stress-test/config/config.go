package config

import "stressTest/defs"

func GetDefaultLabelSelector() string {
	return "env=test"
}
func GetDefultNameSpace() string {
	return "myx-test"
}
func GetDefultAuthor() string {
	return "Bearer " + defs.Token
}
