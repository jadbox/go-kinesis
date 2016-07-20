// Package kinesis implements a batch producer built on top of the official AWS SDK.
package kinesis

import (
	"log"
	"time"

	fh "github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/jpillora/backoff"
)

// Size limits as defined by http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html.
// const (
// 	maxRecordSize        = 1 << 20 // 1MiB
// 	maxRequestSize       = 5 << 20 // 5MiB
// 	maxRecordsPerRequest = 500
// )

// const (
// 	maxRecordSize        = 1 << 20 // 1MiB
// 	maxRequestSize       = 4 << 20 // 5MiB
// 	maxRecordsPerRequest = 500
// )

// Errors.
var (
// ErrRecordSizeExceeded = errors.New("firehose: record size exceeded")
)

type FirehoseConfig struct {
	// StreamName is the Firehose stream.
	StreamName string

	// FlushInterval is a regular interval for flushing the buffer. Defaults to 1s.
	FlushInterval time.Duration

	// BufferSize determines the batch request size. Must not exceed 500. Defaults to 500.
	BufferSize int

	// BacklogSize determines the channel capacity before Put() will begin blocking. Defaults to 500.
	BacklogSize int

	// Backoff determines the backoff strategy for record failures.
	Backoff backoff.Backoff

	// Client is the Firehose API implementation.
	Client firehoseiface.FirehoseAPI

	// Compact messages in records
	Compact bool

	// Max concurrent calls to Firehose, default 1
	ConcurrentRequests int
}

// defaults for configuration.
func (c *FirehoseConfig) defaults() {
	if c.ConcurrentRequests == 0 {
		c.ConcurrentRequests = 1
	}

	if c.BufferSize == 0 {
		c.BufferSize = maxRecordsPerRequest
	}

	if c.BufferSize > maxRecordsPerRequest {
		panic("kinesis: BufferSize exceeds 500")
	}

	if c.BacklogSize == 0 {
		c.BacklogSize = maxRecordsPerRequest
	}

	if c.FlushInterval == 0 {
		c.FlushInterval = time.Second
	}
}

// FirehoseProducer batches records.
type FirehoseProducer struct {
	FirehoseConfig
	records chan *fh.Record
	done    chan struct{}
}

// NewFirehose producer with the given config.
func NewFirehose(config FirehoseConfig) *FirehoseProducer {
	config.defaults()
	return &FirehoseProducer{
		FirehoseConfig: config,
		records:        make(chan *fh.Record, config.BacklogSize),
		done:           make(chan struct{}),
	}
}

// Put record `data`. This method is thread-safe.
func (p *FirehoseProducer) Put(data []byte) error {
	if len(data) > maxRecordSize {
		return ErrRecordSizeExceeded
	}

	p.records <- &fh.Record{
		Data: data,
	}

	return nil
}

// Start the producer.
func (p *FirehoseProducer) Start() {
	go p.loop()
}

// Stop the producer. Flushes any in-flight data.
func (p *FirehoseProducer) Stop() {
	log.Println("backlog", len(p.records), "stopping producer")

	// drain
	p.done <- struct{}{}
	close(p.records)

	// wait
	<-p.done

	log.Println("stopped producer")
}

// loop and flush at the configured interval, or when the buffer is exceeded.
func (p *FirehoseProducer) loop() {
	buf := make([]*fh.Record, 0, p.BufferSize)
	semRequests := make(chan bool, p.ConcurrentRequests)
	bufBytes := 0
	tick := time.NewTicker(p.FlushInterval)
	drain := false

	defer tick.Stop()
	defer close(p.done)

	for {
		select {
		case record := <-p.records:
			recordBytes := len(record.Data)
			// Check if new record exeeds batch byte size, flush current buf if new entry exceeds size
			if bufBytes + recordBytes >= maxRequestSize {
				semRequests <- true
				go p.flush(buf, "bufByteSize", semRequests)
				buf = nil
				bufBytes = 0
			}

			bufBytes += recordBytes

			// Add it to batch
			var lastEntry *fh.Record = nil
			if len(buf) > 0 { lastEntry = buf[len(buf)-1] }
			// Use Compact? Is there a last entry? Does adding this entry to the last be under the maxRecordSize?
			if p.Compact == true && lastEntry != nil && len(lastEntry.Data) + recordBytes < maxRecordSize {
				lastEntry.Data = append(lastEntry.Data, record.Data...)
			} else {
				buf = append(buf, record)
			}

			// If buf len is reached
			if len(buf) >= p.BufferSize {
				// Check if compatch is needed
				semRequests <- true
				go p.flush(buf, "bufferNumSize", semRequests)
				buf = nil
				bufBytes = 0
			}

			if drain && len(p.records) == 0 {
				log.Println("drained")
				return
			}
		case <-tick.C:
			if len(buf) > 0 {
				// go log.Println("backlog:", len(p.records))
				semRequests <- true
				go p.flush(buf, "interval", semRequests)
				buf = nil
				bufBytes = 0
			}
		case <-p.done:
			drain = true

			if len(p.records) == 0 {
				return
			}
		}
	}
}

// flush records and retry failures if necessary.
func (p *FirehoseProducer) flush(records []*fh.Record, reason string, semRequests chan bool) {

	if reason != "interval" {
		go log.Println("flush:",
			"records:", len(records),
			"reason:", reason,
			"backlog:", len(p.records),
		)
	}

	out, err := p.Client.PutRecordBatch(&fh.PutRecordBatchInput{
		DeliveryStreamName: &p.StreamName,
		Records:            records,
	})

	if err != nil {
		// TODO(tj): confirm that the AWS SDK handles retries of request-level errors
		// otherwise we need to backoff here as well.
		log.Println("flush:", err)
		p.Backoff.Reset()
		<- semRequests
		return
	}

	failed := *out.FailedPutCount
	if failed == 0 {
		<- semRequests
		return
	}

	backoff := p.Backoff.Duration()

	go log.Println("put failures:",
		"failures:", failed,
		"backoff:", backoff,
	)

	time.Sleep(backoff)

	p.flush(ffailures(records, out.RequestResponses), "retry", semRequests)
}

// failures returns the failed records as indicated in the response.
func ffailures(records []*fh.Record, response []*fh.PutRecordBatchResponseEntry) (out []*fh.Record) {
	for i, record := range response {
		if record.ErrorCode != nil {
			out = append(out, records[i])
		}
	}
	return
}
