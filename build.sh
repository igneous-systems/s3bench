#!/bin/bash

set -e

now=$(date +'%Y-%m-%d-%T')
githash=$(git rev-parse HEAD)

echo "Building version $now-$githash..."

go build -ldflags "-X main.gitHash=$githash -X main.buildDate=$now"

echo "Complete"
