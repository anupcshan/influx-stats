package stats

import (
	"context"
	"log"
	"time"

	client "github.com/anupcshan/influxdb1-client/v2client"
)

const DefaultPushBatchSize = 1000
const DefaultQueueSize = 10000

type Reporter interface {
	Log(string, interface{})
}

type dataPoint struct {
	name      string
	timestamp time.Time
	stat      interface{}
}

type influxReporter struct {
	c       client.Client
	db      string
	q       chan dataPoint
	backlog []dataPoint
}

func (ir *influxReporter) Log(name string, stat interface{}) {
	d := dataPoint{
		name:      name,
		timestamp: time.Now(),
		stat:      stat,
	}

	// If we cannot buffer any more metrics, silently drop it
	select {
	case ir.q <- d:
	default:
	}
}

func (ir *influxReporter) periodicallyPushStats(ctx context.Context, pushInterval time.Duration) {
	pushTicker := time.NewTicker(pushInterval)
	for {
		select {
		case <-ctx.Done():
			// TODO: Should probably deregister defaultReporter before exiting.
			return
		case <-pushTicker.C:
			err := ir.push(DefaultPushBatchSize)
			if err != nil {
				log.Println("Error pushing stats", err)
			}
		}
	}
}

func (ir *influxReporter) push(maxPoints int) error {
	bp, bpErr := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  ir.db,
		Precision: "us",
	})
	if bpErr != nil {
		return bpErr
	}

	points := 0
	for _, dp := range ir.backlog {
		pt, err := client.NewPoint(
			dp.name,
			nil,
			map[string]interface{}{
				"value": dp.stat,
			},
			dp.timestamp,
		)
		if err != nil {
			log.Println("Skipping point", pt, "because", err)
			continue
		}
		bp.AddPoint(pt)
		points++
	}

	for points < maxPoints {
		select {
		case dp := <-ir.q:
			pt, err := client.NewPoint(
				dp.name,
				nil,
				map[string]interface{}{
					"value": dp.stat,
				},
				dp.timestamp,
			)
			if err != nil {
				log.Println("Skipping point", pt, "because", err)
				continue
			}
			bp.AddPoint(pt)
			ir.backlog = append(ir.backlog, dp)
		default:
			// If there are no more buffered stats, push what we have right now
			break
		}

		points++
	}

	if points > 0 {
		writeErr := ir.c.Write(bp)
		if writeErr == nil {
			ir.backlog = ir.backlog[:0]
		}

		return writeErr
	}

	return nil
}

type blackholeReporter struct{}

func (br blackholeReporter) Log(name string, stat interface{}) {}

var defaultReporter Reporter = blackholeReporter{}

func Log(name string, stat interface{}) {
	defaultReporter.Log(name, stat)
}

func RegisterInfluxStatsPusher(ctx context.Context, pushInterval time.Duration, url string, db string, username string, password string) {
	c, clientErr := client.NewHTTPClient(client.HTTPConfig{
		Addr:     url,
		Username: username,
		Password: password,
	})

	if clientErr != nil {
		log.Println("Error creating influxdb client", clientErr)
	}

	ir := &influxReporter{
		c:       c,
		q:       make(chan dataPoint, DefaultQueueSize),
		db:      db,
		backlog: make([]dataPoint, 0, DefaultPushBatchSize),
	}
	defaultReporter = ir
	ir.periodicallyPushStats(ctx, pushInterval)
}
