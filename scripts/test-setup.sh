#!/bin/bash

set -x

if [ ! -z "$(govendor list -no-status +outside)" ]; then
    echo "External dependencies found, only vendored dependencies supported"
    exit 1
fi
