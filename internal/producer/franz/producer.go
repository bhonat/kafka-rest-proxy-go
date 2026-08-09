package franz

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/example/kafka-rest-proxy-go/internal/config"
	"github.com/example/kafka-rest-proxy-go/internal/limits"
	"github.com/example/kafka-rest-proxy-go/internal/producer"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Producer struct {
	client     *kgo.Client
	admit      *limits.Admission
	closeFlag  atomic.Bool
	recordPool sync.Pool
}

func New(cfg config.KafkaConfig) (*Producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RecordPartitioner(newExplicitPartitioner()),
		kgo.MaxBufferedRecords(cfg.MaxBufferedRecords),
		kgo.MaxBufferedBytes(cfg.MaxBufferedBytes),
		kgo.ProducerLinger(cfg.Linger),
		kgo.ProducerBatchMaxBytes(cfg.BatchMaxBytes),
		kgo.ProduceRequestTimeout(cfg.RequestTimeout),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
		kgo.RequiredAcks(requiredAcks(cfg.RequiredAcks)),
	}

	compressions, err := compressionPreference(cfg.Compression)
	if err != nil {
		return nil, err
	}
	if len(compressions) > 0 {
		opts = append(opts, kgo.ProducerBatchCompression(compressions...))
	}

	if cfg.TLS.Enable {
		tlsConfig, err := newTLSConfig(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	mech, err := saslMechanism(cfg.SASL)
	if err != nil {
		return nil, err
	}
	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	admit := limits.NewAdmission(int64(cfg.MaxBufferedRecords), int64(cfg.MaxBufferedBytes))
	return &Producer{client: client, admit: admit}, nil
}

func (p *Producer) Produce(ctx context.Context, records []producer.Record) ([]producer.Result, error) {
	if p.closeFlag.Load() {
		return nil, kgo.ErrClientClosed
	}
	if len(records) == 0 {
		return []producer.Result{}, nil
	}

	var bytes int64
	for _, r := range records {
		bytes += r.SizeBytes()
	}

	if !p.admit.TryAcquire(int64(len(records)), bytes) {
		return nil, producer.ErrOverloaded
	}

	state := newRequestState(len(records), p.admit, int64(len(records)), bytes)
	for i, r := range records {
		record := p.acquireRecord(r)

		index := i
		p.client.TryProduce(ctx, record, func(rec *kgo.Record, err error) {
			state.complete(index, rec, err)
			p.releaseRecord(record)
		})
	}

	select {
	case <-state.done:
		return state.results, nil
	case <-ctx.Done():
		state.release()
		return nil, ctx.Err()
	}
}

func (p *Producer) Ping(ctx context.Context) error {
	if p.closeFlag.Load() {
		return kgo.ErrClientClosed
	}
	return p.client.Ping(ctx)
}

func (p *Producer) Close() {
	if p.closeFlag.CompareAndSwap(false, true) {
		p.client.Close()
	}
}

func (p *Producer) AdmissionSnapshot() (usedRecords, maxRecords, usedBytes, maxBytes int64) {
	return p.admit.Snapshot()
}

func (p *Producer) BufferedSnapshot() (records, bytes int64) {
	return p.client.BufferedProduceRecords(), p.client.BufferedProduceBytes()
}

func (p *Producer) acquireRecord(r producer.Record) *kgo.Record {
	rec, _ := p.recordPool.Get().(*kgo.Record)
	if rec == nil {
		rec = &kgo.Record{}
	}
	*rec = kgo.Record{
		Topic:     r.Topic,
		Key:       r.Key,
		Value:     r.Value,
		Headers:   toKgoHeaders(r.Headers),
		Partition: unspecifiedPartition,
	}
	if r.Partition != nil {
		rec.Partition = *r.Partition
	}
	return rec
}

func (p *Producer) releaseRecord(rec *kgo.Record) {
	if rec == nil {
		return
	}
	*rec = kgo.Record{}
	p.recordPool.Put(rec)
}

type requestState struct {
	remaining atomic.Int64
	results   []producer.Result
	done      chan struct{}
	admit     *limits.Admission
	records   int64
	bytes     int64
	released  atomic.Bool
}

func newRequestState(n int, admit *limits.Admission, records, bytes int64) *requestState {
	st := &requestState{
		results: make([]producer.Result, n),
		done:    make(chan struct{}),
		admit:   admit,
		records: records,
		bytes:   bytes,
	}
	st.remaining.Store(int64(n))
	return st
}

func (s *requestState) complete(index int, rec *kgo.Record, err error) {
	result := producer.Result{Err: err}
	if rec != nil {
		result.Partition = rec.Partition
		result.Offset = rec.Offset
	}
	if err != nil {
		if code, ok := kafkaErrorCode(err); ok {
			result.ErrorCode = &code
		}
	}
	s.results[index] = result

	if s.remaining.Add(-1) == 0 {
		s.release()
		close(s.done)
	}
}

func (s *requestState) release() {
	if s.released.CompareAndSwap(false, true) {
		s.admit.Release(s.records, s.bytes)
	}
}

func toKgoHeaders(headers []producer.Header) []kgo.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	out := make([]kgo.RecordHeader, 0, len(headers))
	for _, h := range headers {
		out = append(out, kgo.RecordHeader{
			Key:   h.Key,
			Value: h.Value,
		})
	}
	return out
}

func requiredAcks(v string) kgo.Acks {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "0", "none", "noack", "no-ack":
		return kgo.NoAck()
	case "1", "leader":
		return kgo.LeaderAck()
	default:
		return kgo.AllISRAcks()
	}
}

func compressionPreference(v string) ([]kgo.CompressionCodec, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "default":
		return nil, nil
	case "none", "off", "false":
		return []kgo.CompressionCodec{kgo.NoCompression()}, nil
	case "gzip":
		return []kgo.CompressionCodec{kgo.GzipCompression()}, nil
	case "snappy":
		return []kgo.CompressionCodec{kgo.SnappyCompression()}, nil
	case "lz4":
		return []kgo.CompressionCodec{kgo.Lz4Compression(), kgo.NoCompression()}, nil
	case "zstd":
		return []kgo.CompressionCodec{kgo.ZstdCompression(), kgo.Lz4Compression(), kgo.NoCompression()}, nil
	default:
		return nil, fmt.Errorf("unsupported KAFKA_COMPRESSION %q", v)
	}
}

func newTLSConfig(cfg config.TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // explicit config knob for non-prod clusters.
	}

	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read KAFKA_TLS_CA_FILE: %w", err)
		}
		roots := x509.NewCertPool()
		if ok := roots.AppendCertsFromPEM(pem); !ok {
			return nil, fmt.Errorf("KAFKA_TLS_CA_FILE did not contain any PEM certificates")
		}
		tlsConfig.RootCAs = roots
	}

	if cfg.CertFile != "" || cfg.KeyFile != "" {
		if cfg.CertFile == "" || cfg.KeyFile == "" {
			return nil, fmt.Errorf("KAFKA_TLS_CERT_FILE and KAFKA_TLS_KEY_FILE must be set together")
		}
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load Kafka TLS client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func saslMechanism(cfg config.SASLConfig) (sasl.Mechanism, error) {
	switch strings.ToUpper(strings.TrimSpace(cfg.Mechanism)) {
	case "":
		return nil, nil
	case "PLAIN":
		return plain.Auth{User: cfg.Username, Pass: cfg.Password}.AsMechanism(), nil
	case "SCRAM-SHA-256", "SCRAM_SHA_256":
		return scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512", "SCRAM_SHA_512":
		return scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported KAFKA_SASL_MECHANISM %q", cfg.Mechanism)
	}
}

func kafkaErrorCode(err error) (int, bool) {
	if err == nil {
		return 0, false
	}
	return 50002, true
}
