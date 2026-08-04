# LaunchDarkly Server-Side SDK for Rust

[![Run CI](https://github.com/launchdarkly/rust-server-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/launchdarkly/rust-server-sdk/actions/workflows/ci.yml)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/launchdarkly-server-sdk)](https://crates.io/crates/launchdarkly-server-sdk)
[![Crates.io Version](https://img.shields.io/crates/v/launchdarkly-server-sdk)](https://crates.io/crates/launchdarkly-server-sdk)


The LaunchDarkly Server-Side SDK for Rust is designed primarily for use in multi-user systems such as web servers and applications. It follows the server-side LaunchDarkly model for multi-user contexts. It is not intended for use in desktop and embedded systems applications.

## LaunchDarkly overview

[LaunchDarkly](https://www.launchdarkly.com) is a feature management platform that serves trillions of feature flags daily to help teams build better software, faster. [Get started](https://docs.launchdarkly.com/docs/getting-started) using LaunchDarkly today!

[![Twitter Follow](https://img.shields.io/twitter/follow/launchdarkly.svg?style=social&label=Follow&maxAge=2592000)](https://twitter.com/intent/follow?screen_name=launchdarkly)

## Getting started

Refer to the [SDK documentation](https://docs.launchdarkly.com/sdk/server-side/rust#getting-started) for instructions on getting started with using the SDK.

This repository also contains several small [example implementations](./examples). You can run these with:

```sh
cargo run --example EXAMPLE_NAME
```

## Cargo features

| Feature | Default | Description |
| --- | --- | --- |
| `hyper-rustls-native-roots` | yes | Uses `hyper` for HTTP, with `rustls` for TLS and the platform's native certificate roots. |
| `hyper-rustls-webpki-roots` | no | Uses `hyper` for HTTP, with `rustls` for TLS and the bundled `webpki` certificate roots. |
| `native-tls` | no | Uses `hyper` for HTTP, with the platform's native TLS implementation. |
| `hyper` | no | Uses `hyper` for HTTP without selecting a TLS implementation. |
| `crypto-aws-lc-rs` | yes | Uses `aws-lc-rs` for cryptographic operations. |
| `crypto-openssl` | no | Uses `openssl` for cryptographic operations. |
| `event-compression` | yes | Compresses analytics event payloads before sending them to LaunchDarkly. |
| `float-roundtrip` | yes | Enables `serde_json`'s `float_roundtrip` feature so that fractional JSON numbers deserialize to the same `f64` that Go's `encoding/json` produces. |

### Disabling `float-roundtrip`

`float-roundtrip` is enabled by default because it is what keeps numeric flag values and numeric context attributes consistent with the other LaunchDarkly SDKs. Without it, `serde_json` uses a faster best-effort float parser that can land one unit in the last place away from the correctly-rounded value, so a numeric evaluation could in principle differ from what another SDK computes for the same flag.

Disable it if you would rather have the faster parser and do not depend on that cross-SDK consistency. Because it is a default feature, opting out means turning the defaults off and re-listing the ones you want:

```toml
launchdarkly-server-sdk = { version = "3", default-features = false, features = [
    "hyper-rustls-native-roots",
    "crypto-aws-lc-rs",
    "event-compression",
] }
```

Note that Cargo feature unification is additive and applies to the whole build, so `float_roundtrip` remains enabled if any other crate in your dependency graph asks for it.

## Learn more

Read our [documentation](https://docs.launchdarkly.com) for in-depth instructions on configuring and using LaunchDarkly. You can also head straight to the [complete reference guide for this SDK](https://docs.launchdarkly.com/sdk/server-side/rust).

## Minimum Supported Rust Version

This project aims to maintain compatibility with the latest stable release of Rust in addition to the two prior minor releases.

Version updates may occur more frequently than the policy guideline states if external forces require it. For example, a CVE in a downstream dependency requiring an MSRV bump would be considered an acceptable reason to violate the six month guideline.

## Testing

We run integration tests for all our SDKs using a centralized test harness. This approach gives us the ability to test for consistency across SDKs, as well as test networking behavior in a long-running application. These tests cover each method in the SDK, and verify that event sending, flag evaluation, stream reconnection, and other aspects of the SDK all behave correctly.

## Contributing

We encourage pull requests and other contributions from the community. Check out our [contributing guidelines](CONTRIBUTING.md) for instructions on how to contribute to this SDK.

## About LaunchDarkly

* LaunchDarkly is a continuous delivery platform that provides feature flags as a service and allows developers to iterate quickly and safely. We allow you to easily flag your features and manage them from the LaunchDarkly dashboard.  With LaunchDarkly, you can:
    * Roll out a new feature to a subset of your users (like a group of users who opt-in to a beta tester group), gathering feedback and bug reports from real-world use cases.
    * Gradually roll out a feature to an increasing percentage of users, and track the effect that the feature has on key metrics (for instance, how likely is a user to complete a purchase if they have feature A versus feature B?).
    * Turn off a feature that you realize is causing performance problems in production, without needing to re-deploy, or even restart the application with a changed configuration file.
    * Grant access to certain features based on user attributes, like payment plan (eg: users on the ‘gold’ plan get access to more features than users in the ‘silver’ plan). Disable parts of your application to facilitate maintenance, without taking everything offline.
* LaunchDarkly provides feature flag SDKs for a wide variety of languages and technologies. Read [our documentation](https://docs.launchdarkly.com/docs) for a complete list.
* Explore LaunchDarkly
    * [launchdarkly.com](https://www.launchdarkly.com/ "LaunchDarkly Main Website") for more information
    * [docs.launchdarkly.com](https://docs.launchdarkly.com/  "LaunchDarkly Documentation") for our documentation and SDK reference guides
    * [apidocs.launchdarkly.com](https://apidocs.launchdarkly.com/  "LaunchDarkly API Documentation") for our API documentation
    * [launchdarkly.com/blog](https://launchdarkly.com/blog/  "LaunchDarkly Blog Documentation") for the latest product updates
