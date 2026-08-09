#!/usr/bin/env sh
set -eu

version="$(cat VERSION)"
out_dir="${1:-dist/release}"

mkdir -p "$out_dir"

build_one() {
  goos="$1"
  goarch="$2"
  suffix="$goos-$goarch"
  bin="$out_dir/kafka-rest-proxy-go-$version-$suffix"
  if [ "$goos" = "windows" ]; then
    bin="$bin.exe"
  fi
  echo "building $bin"
  GOOS="$goos" GOARCH="$goarch" CGO_ENABLED=0 go build -mod=readonly \
    -trimpath \
    -ldflags="-s -w -X main.version=$version" \
    -o "$bin" \
    ./cmd/kafka-rest-proxy-go
}

build_one linux amd64
build_one linux arm64
build_one darwin amd64
build_one darwin arm64

(
  cd "$out_dir"
  LC_ALL=C LANG=C shasum -a 256 kafka-rest-proxy-go-"$version"-* > SHA256SUMS
)
