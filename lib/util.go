package lib

import (
	"strings"
	"time"
)

func SetTimeZone(t time.Time) string {
	loc, _ := time.LoadLocation(TimeLoc)
	return t.In(loc).Format(TimeFmt)
}

func Split(topics string) []string {
	var result []string
	for _, t := range strings.Split(topics, ",") {
		tmp := strings.TrimSpace(t)
		if len(tmp) == 0 {
			continue
		}
		result = append(result, strings.TrimSpace(t))
	}
	return result
}
