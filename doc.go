// Package implements a basic stats logger, pushing points to influxdb.
//
// To use, first register a reporter via:
//	import stats "github.com/anupcshan/influx-stats"
// 	go stats.RegisterInfluxStatsPusher(
//		context.Background(),
//		10*time.Second,
//		"http://influx.domain:8086",
//		"metricsdb",
//		"metricsdbuser",
//		"metricsdbpass",
//	)
//
// To log a metric, call:
//	stats.Log("counter", 1)
package stats
