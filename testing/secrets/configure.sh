#!/usr/bin/env sh
set -eu

out_dir="${1:-testing/secrets/generated}"
mkdir -p "$out_dir"

command -v openssl >/dev/null 2>&1 || {
  echo "openssl is required to generate local TLS fixtures" >&2
  exit 1
}
command -v keytool >/dev/null 2>&1 || {
  echo "keytool is required to generate local Kafka PKCS12 truststores" >&2
  exit 1
}

printf '%s\n' 'test-secret' >"$out_dir/broker_keystore_creds"
printf '%s\n' 'test-secret' >"$out_dir/broker_sslkey_creds"
printf '%s\n' 'test-secret' >"$out_dir/broker_truststore_creds"
printf '%s\n' 'test-secret' >"$out_dir/client_keystore_creds"
printf '%s\n' 'test-secret' >"$out_dir/client_sslkey_creds"

if [ -f docker/security/kafka_server_jaas.conf ]; then
  cp docker/security/kafka_server_jaas.conf "$out_dir/kafka_server_jaas.conf"
fi

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$out_dir/ca.key" \
  -out "$out_dir/ca.crt" \
  -days 365 \
  -subj "/CN=kafka-rest-proxy-go-test-ca"

cat >"$out_dir/server.ext" <<'EOF'
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:kafka-sasl-ssl,DNS:kafka-mtls,DNS:localhost,IP:127.0.0.1
EOF

openssl req -newkey rsa:2048 -nodes \
  -keyout "$out_dir/server.key" \
  -out "$out_dir/server.csr" \
  -subj "/CN=kafka-sasl-ssl"

openssl x509 -req \
  -in "$out_dir/server.csr" \
  -CA "$out_dir/ca.crt" \
  -CAkey "$out_dir/ca.key" \
  -CAcreateserial \
  -out "$out_dir/server.crt" \
  -days 365 \
  -extfile "$out_dir/server.ext"

openssl pkcs12 -export \
  -in "$out_dir/server.crt" \
  -inkey "$out_dir/server.key" \
  -chain \
  -CAfile "$out_dir/ca.crt" \
  -name kafka-sasl-ssl \
  -out "$out_dir/broker.keystore.p12" \
  -password pass:test-secret

rm -f "$out_dir/broker.truststore.p12"
keytool -importcert -noprompt \
  -alias kafka-rest-proxy-go-test-ca \
  -file "$out_dir/ca.crt" \
  -keystore "$out_dir/broker.truststore.p12" \
  -storetype PKCS12 \
  -storepass test-secret

openssl req -newkey rsa:2048 -nodes \
  -keyout "$out_dir/client.key" \
  -out "$out_dir/client.csr" \
  -subj "/CN=kafka-rest-proxy-go-client"

openssl x509 -req \
  -in "$out_dir/client.csr" \
  -CA "$out_dir/ca.crt" \
  -CAkey "$out_dir/ca.key" \
  -CAcreateserial \
  -out "$out_dir/client.crt" \
  -days 365

openssl pkcs12 -export \
  -in "$out_dir/client.crt" \
  -inkey "$out_dir/client.key" \
  -chain \
  -CAfile "$out_dir/ca.crt" \
  -name kafka-rest-proxy-go-client \
  -out "$out_dir/client.keystore.p12" \
  -password pass:test-secret

chmod 0644 "$out_dir"/*

echo "Generated local TLS fixtures in $out_dir"
