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
}

// defaults for configuration.
func (c *FirehoseConfig) defaults() {

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
	tick := time.NewTicker(p.FlushInterval)
	drain := false

	defer tick.Stop()
	defer close(p.done)

	for {
		select {
		case record := <-p.records:
			buf = append(buf, record)

			if len(buf) >= p.BufferSize {
				p.flush(buf, "buffer size")
				buf = nil
			}

			if drain && len(p.records) == 0 {
				log.Println("drained")
				return
			}
		case <-tick.C:
			go log.Println("backlog:", len(p.records))
			if len(buf) > 0 {
				p.flush(buf, "interval")
				buf = nil
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
func (p *FirehoseProducer) flush(records []*fh.Record, reason string) {
	go log.Println("flush:",
		"records:", len(records),
		"reason:", reason,
	)

	out, err := p.Client.PutRecordBatch(&fh.PutRecordBatchInput{
		DeliveryStreamName: &p.StreamName,
		Records:            records,
	})

	if err != nil {
		// TODO(tj): confirm that the AWS SDK handles retries of request-level errors
		// otherwise we need to backoff here as well.
		log.Println("flush:", err)
		p.Backoff.Reset()
		return
	}

	failed := *out.FailedPutCount
	if failed == 0 {
		return
	}

	backoff := p.Backoff.Duration()

	go log.Println("put failures:",
		"failures:", failed,
		"backoff:", backoff,
	)

	time.Sleep(backoff)

	p.flush(ffailures(records, out.RequestResponses), "retry")
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
