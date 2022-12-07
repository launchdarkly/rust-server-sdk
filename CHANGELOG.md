# Change log

All notable changes to the LaunchDarkly Rust server-side SDK will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org).

## [1.0.0] - 2022-12-07
The latest version of this SDK supports LaunchDarkly's new custom contexts feature. Contexts are an evolution of a previously-existing concept, "users." Contexts let you create targeting rules for feature flags based on a variety of different information, including attributes pertaining to users, organizations, devices, and more. You can even combine contexts to create "multi-contexts." 

This feature is only available to members of LaunchDarkly's Early Access Program (EAP). If you're in the EAP, you can use contexts by updating your SDK to the latest version and, if applicable, updating your Relay Proxy. Outdated SDK versions do not support contexts, and will cause unpredictable flag evaluation behavior.

If you are not in the EAP, only use single contexts of kind "user", or continue to use the user type if available. If you try to create contexts, the context will be sent to LaunchDarkly, but any data not related to the user object will be ignored.


For detailed information about this version, please refer to the list below. For information on how to upgrade from the previous version, please read the [migration guide](https://docs.launchdarkly.com/sdk/server-side/rust/implementation-v1).


### Added:
- Added: `hyper` @ `0.14.17`
- Added: `hyper-rustls` @ `0.23.1`
- Added: types `Context`, `ContextBuilder`, `MultiContextBuilder`, `Reference`, `Kind`

### Changed:
- All SDK methods that accepted users now accept contexts. 
- The [MSRV](https://rust-lang.github.io/rfcs/2495-min-rust-version.html) is now 1.60.0
- Updated: `lru` from `0.7.2` to `0.8.1`
- Updated: `launchdarkly-server-sdk-evaluation` from `1.0.0-beta.5` to `1.0.0`
- Updated: `moka` from `0.7.1` to `0.9.6`
- Updated: `uuid` from `1.0.0-alpha.1` to `1.2.2`

### Fixed:
- Fixed handling of unexpected HTTP status codes.

### Removed:
- Removed `alias` method
- Removed types `User`, `UserBuilder`. See `Context`, `ContextBuilder`, and `MultiContextBuilder` instead.
- Removed `reqwest` dependency
- Removed `threadpool` dependency

## [1.0.0-beta.4] - 2022-11-07
### Added:
- Enforce ApplicationInfo length restrictions.
- Set missing HTTP headers for event payloads.

### Fixed:
- Correct handling of specific temporary network failures.
- Prevent shutdown broadcast error when in polling mode.
- Reset event summaries correctly.

## [1.0.0-beta.3] - 2022-04-06
### Added
- Add support for persistent data stores, which allow flag and segment information to be stored in external databases like redis.
- Introduced `ApplicationInfo`, for configuration of application metadata that may be used in LaunchDarkly analytics or other product features. This does not affect feature flag evaluations.
- Support private attribute configuration for redacting user properties from analytic events.

### Changed
- Pin evaluation crate version to specific beta release.
- Bump versions in multiple dependencies.
- Update to using Rust 2021 edition.
- Update deserialization to handle invalid variation indexes.
- Remove reliance on spectral test crate.
- Allow re-using a connector object between clients to reduce initialization overhead.

### Fixed
- Fixed missing README in published crate.
- Enable HTTPS support for reqwest

## [1.0.0-beta.2] - 2022-02-02
### Added
- Initial support for contract test harness.
- Ensure we can build using musl build tools.

### Changed
- Drop OpenSSL dependency in favor of rustls.

### Fixed
- Creation date is now included in alias events.
- Emit index events for feature events correctly.

## [1.0.0-beta.1] - 2022-01-21
Initial beta release of the LaunchDarkly Server-Side SDK for Rust.
