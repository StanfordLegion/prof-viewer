#!/usr/bin/env bash
# This scripts runs various CI-like checks in a convenient way.
set -eux

cargo check --workspace --no-default-features --lib
cargo check --workspace --no-default-features --features client --all-targets
cargo check --workspace --no-default-features --features server --lib
cargo check --workspace --all-features --all-targets

cargo check --workspace --no-default-features --lib --target wasm32-unknown-unknown
cargo check --workspace --no-default-features --features client --lib --target wasm32-unknown-unknown

cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --  -D warnings
cargo test --workspace --all-targets --all-features
cargo test --workspace --doc
trunk build
