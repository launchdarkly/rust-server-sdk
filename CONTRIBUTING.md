# Contributing to the LaunchDarkly Server-Side SDK for Rust

LaunchDarkly has published an [SDK contributor's guide](https://docs.launchdarkly.com/docs/sdk-contributors-guide) that provides a detailed explanation of how our SDKs work. See below for additional information on how to contribute to this SDK.

## Submitting bug reports and feature requests

The LaunchDarkly SDK team monitors the [issue tracker](https://github.com/launchdarkly/rust-server-sdk/issues) in the SDK repository. Bug reports and feature requests specific to this SDK should be filed in this issue tracker. The SDK team will respond to all newly filed issues within two business days.

## Submitting pull requests

We encourage pull requests and other contributions from the community. Before submitting pull requests, ensure that all temporary or unintended code is removed. Don't worry about adding reviewers to the pull request; the LaunchDarkly SDK team will add themselves. The SDK team will acknowledge all pull requests within two business days.

## Build instructions

This SDK uses the standard `cargo` build system.

### Building

To build the SDK without running any tests:

```sh
cargo build
```

To check and report errors without compiling object files, run:

```sh
cargo check
```

### Testing

To run all tests (unit, doc, etc), run:

```sh
cargo test
```

If you want to only run the much faster unit tests, run:

```sh
cargo test --lib
```
