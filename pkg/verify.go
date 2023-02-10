package pkg

import (
	"strings"
)

func CheckPackage(bs string) (ok bool) {
	ok = strings.Contains(bs, "end_string")
	return
}
