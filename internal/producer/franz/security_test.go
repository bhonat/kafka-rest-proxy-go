package franz

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bhonat/kafka-rest-proxy-go/internal/config"
)

func TestNewTLSConfigLoadsCAFile(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, testCA(t), 0o600); err != nil {
		t.Fatal(err)
	}

	tlsConfig, err := newTLSConfig(config.TLSConfig{
		CAFile: caPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if tlsConfig.RootCAs == nil {
		t.Fatal("RootCAs = nil, want custom CA pool")
	}
}

func TestNewTLSConfigRejectsPartialClientCertificate(t *testing.T) {
	_, err := newTLSConfig(config.TLSConfig{
		CertFile: "client.pem",
	})
	if err == nil {
		t.Fatal("expected partial client certificate config error")
	}
}

func TestSASLMechanismSupportsSCRAM(t *testing.T) {
	for _, mechanism := range []string{"SCRAM-SHA-256", "SCRAM_SHA_256", "SCRAM-SHA-512", "SCRAM_SHA_512"} {
		t.Run(mechanism, func(t *testing.T) {
			mech, err := saslMechanism(config.SASLConfig{
				Mechanism: mechanism,
				Username:  "user",
				Password:  "pass",
			})
			if err != nil {
				t.Fatal(err)
			}
			if mech == nil {
				t.Fatal("mechanism = nil")
			}
		})
	}
}

func testCA(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kafka-rest-proxy-go-test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
