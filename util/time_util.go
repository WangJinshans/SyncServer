package util

import "time"

func GetTodayLeftSecond() int {
	var cstSh, _ = time.LoadLocation("Asia/Shanghai")
	timeTemplate := "2006-01-02 15:04:05"
	timeStr := time.Now().Format("2006-01-02")
	currentTimeStr := time.Now().In(cstSh).Format(timeTemplate)
	todayEndTimeStr := timeStr + " 23:59:59"

	formatTime1, _ := time.Parse(timeTemplate, currentTimeStr)
	formatTime2, _ := time.Parse(timeTemplate, todayEndTimeStr)

	t1 := formatTime1.Unix()
	t2 := formatTime2.Unix()

	return int(t2 - t1)
}
