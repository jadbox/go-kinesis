// Package kinesis implements a batch producer built on top of the official AWS SDK.
package kinesis

import (
	"errors"
	"time"

	"github.com/apex/log"
	k "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/jpillora/backoff"
)

// Size limits as defined by http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html.
const (
	maxRecordSize        = 1024000 // 1MiB, as Reported by AWS sdk
	maxRequestSize       = maxRecordSize * 4 // 4MiB for kinesis
	maxRecordsPerRequest = 500
)

// Errors.
var (
	ErrRecordSizeExceeded = errors.New("kinesis: record size exceeded")
)

type Config struct {
	// StreamName is the Kinesis stream.
	StreamName string

	// FlushInterval is a regular interval for flushing the buffer. Defaults to 1s.
	FlushInterval time.Duration

	// BufferSize determines the batch request size. Must not exceed 500. Defaults to 500.
	BufferSize int

	// BacklogSize determines the channel capacity before Put() will begin blocking. Defaults to 500.
	BacklogSize int

	// Backoff determines the backoff strategy for record failures.
	Backoff backoff.Backoff

	// Logger is the logger used. Defaults to log.Log.
	Logger log.Interface

	// Client is the Kinesis API implementation.
	Client kinesisiface.KinesisAPI
}

// defaults for configuration.
func (c *Config) defaults() {
	if c.Logger == nil {
		c.Logger = log.Log
	}

	c.Logger = c.Logger.WithFields(log.Fields{
		"package": "kinesis",
		"stream":  c.StreamName,
	})

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

// Producer batches records.
type KinesisProducer struct {
	Config
	records chan *k.PutRecordsRequestEntry
	done    chan struct{}
}

// New producer with the given config.
func New(config Config) *KinesisProducer {
	config.defaults()
	return &KinesisProducer{
		Config:  config,
		records: make(chan *k.PutRecordsRequestEntry, config.BacklogSize),
		done:    make(chan struct{}),
	}
}

// Put record `data` using `partitionKey`. This method is thread-safe.
func (p *KinesisProducer) Put(data []byte, partitionKey string) error {
	if len(data) > maxRecordSize {
		return ErrRecordSizeExceeded
	}

	p.records <- &k.PutRecordsRequestEntry{
		Data:         data,
		PartitionKey: &partitionKey,
	}

	return nil
}

// Start the producer.
func (p *KinesisProducer) Start() {
	go p.loop()
}

// Stop the producer. Flushes any in-flight data.
func (p *KinesisProducer) Stop() {
	p.Logger.WithField("backlog", len(p.records)).Info("stopping producer")

	// drain
	p.done <- struct{}{}
	close(p.records)

	// wait
	<-p.done

	p.Logger.Info("stopped producer")
}

// loop and flush at the configured interval, or when the buffer is exceeded.
func (p *KinesisProducer) loop() {
	buf := make([]*k.PutRecordsRequestEntry, 0, p.BufferSize)
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
				p.Logger.Info("drained")
				return
			}
		case <-tick.C:
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
func (p *KinesisProducer) flush(records []*k.PutRecordsRequestEntry, reason string) {
	p.Logger.WithFields(log.Fields{
		"records": len(records),
		"reason":  reason,
	}).Info("flush")

	out, err := p.Client.PutRecords(&k.PutRecordsInput{
		StreamName: &p.StreamName,
		Records:    records,
	})

	if err != nil {
		// TODO(tj): confirm that the AWS SDK handles retries of request-level errors
		// otherwise we need to backoff here as well.
		p.Logger.WithError(err).Error("flush")
		p.Backoff.Reset()
		return
	}

	failed := *out.FailedRecordCount
	if failed == 0 {
		return
	}

	backoff := p.Backoff.Duration()

	p.Logger.WithFields(log.Fields{
		"failures": failed,
		"backoff":  backoff,
	}).Warn("put failures")

	time.Sleep(backoff)

	p.flush(failures(records, out.Records), "retry")
}

// failures returns the failed records as indicated in the response.
func failures(records []*k.PutRecordsRequestEntry, response []*k.PutRecordsResultEntry) (out []*k.PutRecordsRequestEntry) {
	for i, record := range response {
		if record.ErrorCode != nil {
			out = append(out, records[i])
		}
	}
	return
}
