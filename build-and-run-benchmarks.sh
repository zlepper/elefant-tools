#!/usr/bin/env bash

set -e

cargo build --release --all-features

nix-shell --run "./benchmarks/run_benchmarks.sh"
