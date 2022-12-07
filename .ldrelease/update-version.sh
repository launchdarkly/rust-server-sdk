#!/bin/bash

set -ue

sed -i "/^version\b/c version = \"${LD_RELEASE_VERSION}\"" ./launchdarkly-server-sdk/Cargo.toml
