# Change log

All notable changes to the LaunchDarkly Rust server-side SDK will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org).


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
