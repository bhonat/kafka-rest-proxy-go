package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr              string
	PprofEnable           bool
	ReadHeaderTimeout     time.Duration
	ShutdownTimeout       time.Duration
	ProduceTimeout        time.Duration
	RequestMaxBytes       int64
	RequestMaxRecords     int
	RequestMaxRecordBytes int64
	RequestMaxKeyBytes    int64
	RequestMaxHeaders     int
	RequestMaxHeaderBytes int64
	TopicAllowlist        []string
	AuthBearerTokens      []string
	ClusterID             string
	RateLimit             RateLimitConfig
	SchemaRegistry        SchemaRegistryConfig
	Kafka                 KafkaConfig
}

type RateLimitConfig struct {
	RequestsPerSecond float64
	RequestsBurst     int64
	BytesPerSecond    float64
	BytesBurst        int64
}

type SchemaRegistryConfig struct {
	URL      string
	Username string
	Password string
}

type KafkaConfig struct {
	Brokers            []string
	ClientID           string
	RequiredAcks       string
	Compression        string
	Linger             time.Duration
	DeliveryTimeout    time.Duration
	RequestTimeout     time.Duration
	BatchMaxBytes      int32
	MaxBufferedRecords int
	MaxBufferedBytes   int
	TLS                TLSConfig
	SASL               SASLConfig
}

type TLSConfig struct {
	Enable             bool
	InsecureSkipVerify bool
	CAFile             string
	CertFile           string
	KeyFile            string
}

type SASLConfig struct {
	Mechanism string
	Username  string
	Password  string
}

func LoadFromEnv() (Config, error) {
	cfg := Config{
		HTTPAddr:              envString("HTTP_ADDR", ":8080"),
		PprofEnable:           envBool("PPROF_ENABLE", false),
		ReadHeaderTimeout:     envDuration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
		ShutdownTimeout:       envDuration("SHUTDOWN_TIMEOUT", 20*time.Second),
		ProduceTimeout:        envDuration("PRODUCE_TIMEOUT", 30*time.Second),
		RequestMaxBytes:       envInt64("REQUEST_MAX_BYTES", 8*1024*1024),
		RequestMaxRecords:     envInt("REQUEST_MAX_RECORDS", 1000),
		RequestMaxRecordBytes: envInt64("REQUEST_MAX_RECORD_BYTES", 1*1024*1024),
		RequestMaxKeyBytes:    envInt64("REQUEST_MAX_KEY_BYTES", 1024*1024),
		RequestMaxHeaders:     envInt("REQUEST_MAX_HEADERS", 64),
		RequestMaxHeaderBytes: envInt64("REQUEST_MAX_HEADER_BYTES", 64*1024),
		TopicAllowlist:        envCSV("TOPIC_ALLOWLIST"),
		AuthBearerTokens:      envCSV("AUTH_BEARER_TOKENS"),
		ClusterID:             envString("KAFKA_CLUSTER_ID", "local"),
		RateLimit: RateLimitConfig{
			RequestsPerSecond: envFloat("RATE_LIMIT_REQUESTS_PER_SECOND", 0),
			RequestsBurst:     envInt64("RATE_LIMIT_REQUESTS_BURST", 0),
			BytesPerSecond:    envFloat("RATE_LIMIT_BYTES_PER_SECOND", 0),
			BytesBurst:        envInt64("RATE_LIMIT_BYTES_BURST", 0),
		},
		SchemaRegistry: SchemaRegistryConfig{
			URL:      envString("SCHEMA_REGISTRY_URL", ""),
			Username: envString("SCHEMA_REGISTRY_USERNAME", ""),
			Password: envString("SCHEMA_REGISTRY_PASSWORD", ""),
		},
		Kafka: KafkaConfig{
			Brokers:            envCSVDefault("KAFKA_BROKERS", []string{"localhost:9092"}),
			ClientID:           envString("KAFKA_CLIENT_ID", "kafka-rest-proxy-go"),
			RequiredAcks:       envString("KAFKA_REQUIRED_ACKS", "all"),
			Compression:        envString("KAFKA_COMPRESSION", "lz4"),
			Linger:             envDuration("KAFKA_LINGER", 0),
			DeliveryTimeout:    envDuration("KAFKA_DELIVERY_TIMEOUT", 30*time.Second),
			RequestTimeout:     envDuration("KAFKA_REQUEST_TIMEOUT", 10*time.Second),
			BatchMaxBytes:      int32(envInt("KAFKA_BATCH_MAX_BYTES", 1_048_576)),
			MaxBufferedRecords: envInt("KAFKA_MAX_BUFFERED_RECORDS", 100_000),
			MaxBufferedBytes:   envInt("KAFKA_MAX_BUFFERED_BYTES", 128*1024*1024),
			TLS: TLSConfig{
				Enable:             envBool("KAFKA_TLS_ENABLE", false),
				InsecureSkipVerify: envBool("KAFKA_TLS_INSECURE_SKIP_VERIFY", false),
				CAFile:             envString("KAFKA_TLS_CA_FILE", ""),
				CertFile:           envString("KAFKA_TLS_CERT_FILE", ""),
				KeyFile:            envString("KAFKA_TLS_KEY_FILE", ""),
			},
			SASL: SASLConfig{
				Mechanism: envString("KAFKA_SASL_MECHANISM", ""),
				Username:  envString("KAFKA_SASL_USERNAME", ""),
				Password:  envString("KAFKA_SASL_PASSWORD", ""),
			},
		},
	}

	if len(cfg.Kafka.Brokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must include at least one broker")
	}
	if cfg.RequestMaxRecords <= 0 {
		return Config{}, fmt.Errorf("REQUEST_MAX_RECORDS must be positive")
	}
	if cfg.RequestMaxBytes <= 0 {
		return Config{}, fmt.Errorf("REQUEST_MAX_BYTES must be positive")
	}
	if cfg.RequestMaxRecordBytes <= 0 {
		return Config{}, fmt.Errorf("REQUEST_MAX_RECORD_BYTES must be positive")
	}
	if cfg.RequestMaxKeyBytes <= 0 {
		return Config{}, fmt.Errorf("REQUEST_MAX_KEY_BYTES must be positive")
	}
	if cfg.RequestMaxHeaders < 0 {
		return Config{}, fmt.Errorf("REQUEST_MAX_HEADERS must be zero or positive")
	}
	if cfg.RequestMaxHeaderBytes <= 0 {
		return Config{}, fmt.Errorf("REQUEST_MAX_HEADER_BYTES must be positive")
	}
	if cfg.RateLimit.RequestsPerSecond < 0 {
		return Config{}, fmt.Errorf("RATE_LIMIT_REQUESTS_PER_SECOND must be zero or positive")
	}
	if cfg.RateLimit.RequestsBurst < 0 {
		return Config{}, fmt.Errorf("RATE_LIMIT_REQUESTS_BURST must be zero or positive")
	}
	if cfg.RateLimit.BytesPerSecond < 0 {
		return Config{}, fmt.Errorf("RATE_LIMIT_BYTES_PER_SECOND must be zero or positive")
	}
	if cfg.RateLimit.BytesBurst < 0 {
		return Config{}, fmt.Errorf("RATE_LIMIT_BYTES_BURST must be zero or positive")
	}
	if cfg.Kafka.MaxBufferedRecords <= 0 {
		return Config{}, fmt.Errorf("KAFKA_MAX_BUFFERED_RECORDS must be positive")
	}
	if cfg.Kafka.MaxBufferedBytes <= 0 {
		return Config{}, fmt.Errorf("KAFKA_MAX_BUFFERED_BYTES must be positive")
	}
	if (cfg.Kafka.TLS.CertFile == "") != (cfg.Kafka.TLS.KeyFile == "") {
		return Config{}, fmt.Errorf("KAFKA_TLS_CERT_FILE and KAFKA_TLS_KEY_FILE must be set together")
	}
	if cfg.Kafka.SASL.Mechanism != "" && (cfg.Kafka.SASL.Username == "" || cfg.Kafka.SASL.Password == "") {
		return Config{}, fmt.Errorf("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required when KAFKA_SASL_MECHANISM is set")
	}

	return cfg, nil
}

func envString(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}

func envCSV(name string) []string {
	return cleanCSV(os.Getenv(name))
}

func envCSVDefault(name string, def []string) []string {
	if v := cleanCSV(os.Getenv(name)); len(v) > 0 {
		return v
	}
	return def
}

func cleanCSV(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envBool(name string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return def
	}
}

func envInt(name string, def int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envInt64(name string, def int64) int64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func envFloat(name string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return n
}

func envDuration(name string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err == nil {
		return d
	}
	ms, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return time.Duration(ms) * time.Millisecond
}
