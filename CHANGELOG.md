# Change log

All notable changes to the LaunchDarkly Rust server-side SDK will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org).

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
