#!/usr/bin/env bash

set -o errexit
set -o pipefail


# cargo-llvm-cov requires the nightly compiler to be installed
rustup toolchain install nightly
rustup component add llvm-tools-preview --toolchain nightly
cargo install cargo-llvm-cov

# generate coverage report to command line by default; otherwise allow
# CI to pass in '--html' (or other formats).

if [ -n "$1" ]; then
  cargo llvm-cov --all-features --workspace "$1"
else
  cargo llvm-cov --all-features --workspace
fi

