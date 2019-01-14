#!/bin/bash

set -x

go test -mod=readonly -v -tags test -race ./...
