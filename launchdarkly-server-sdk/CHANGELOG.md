# Change log

All notable changes to the LaunchDarkly Rust server-side SDK will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org).

## [2.6.0](https://github.com/launchdarkly/rust-server-sdk/compare/2.5.1...2.6.0) (2025-07-08)


### Features

* Inline context for custom and migration op events ([#120](https://github.com/launchdarkly/rust-server-sdk/issues/120)) ([7a5da5c](https://github.com/launchdarkly/rust-server-sdk/commit/7a5da5cc8ed898e16a5be8d75bdceeb72712d5d7))


### Bug Fixes

* Bump MSRV to 1.82.0 ([#121](https://github.com/launchdarkly/rust-server-sdk/issues/121)) ([e55c176](https://github.com/launchdarkly/rust-server-sdk/commit/e55c1764f502368775d6835637a4e56bdef0f2c1))

## [2.5.1](https://github.com/launchdarkly/rust-server-sdk/compare/2.5.0...2.5.1) (2025-03-24)


### Bug Fixes

* Bump eventsource-client 0.13 -&gt; 0.14 ([ae56d7c](https://github.com/launchdarkly/rust-server-sdk/commit/ae56d7c0d0d7d86f58b3fd2c27d93b04901721b8))
* Bump lru 0.12 -&gt; 0.13 ([ae56d7c](https://github.com/launchdarkly/rust-server-sdk/commit/ae56d7c0d0d7d86f58b3fd2c27d93b04901721b8))
* Bump rand 0.8 -&gt; 0.9 ([ae56d7c](https://github.com/launchdarkly/rust-server-sdk/commit/ae56d7c0d0d7d86f58b3fd2c27d93b04901721b8))
* Bump thiserror 1.0 -&gt; 2.0 ([ae56d7c](https://github.com/launchdarkly/rust-server-sdk/commit/ae56d7c0d0d7d86f58b3fd2c27d93b04901721b8))

## [2.5.0](https://github.com/launchdarkly/rust-server-sdk/compare/2.4.1...2.5.0) (2025-03-21)


### Features

* Add support for daemon mode ([#110](https://github.com/launchdarkly/rust-server-sdk/issues/110)) ([1a16e39](https://github.com/launchdarkly/rust-server-sdk/commit/1a16e395a677b0a395279e25bc671e0149526e68))


### Bug Fixes

* Bump MSRV to 1.81 ([#109](https://github.com/launchdarkly/rust-server-sdk/issues/109)) ([1b472ff](https://github.com/launchdarkly/rust-server-sdk/commit/1b472ff0cab78802bd57ba217fd31e7aae11f685))

## [2.4.1](https://github.com/launchdarkly/rust-server-sdk/compare/2.4.0...2.4.1) (2024-12-13)


### Bug Fixes

* Enable event-compression during docs.rs generation ([#104](https://github.com/launchdarkly/rust-server-sdk/issues/104)) ([c50a65d](https://github.com/launchdarkly/rust-server-sdk/commit/c50a65d15551984828b405f18902651ab25f4387))

## [2.4.0](https://github.com/launchdarkly/rust-server-sdk/compare/2.3.0...2.4.0) (2024-12-05)


### Features

* Add option to enable compression of event payloads ([#102](https://github.com/launchdarkly/rust-server-sdk/issues/102)) ([5cc7aac](https://github.com/launchdarkly/rust-server-sdk/commit/5cc7aacc3ce641f94faffa06f24ac5cbeff08ce6))

## [2.3.0](https://github.com/launchdarkly/rust-server-sdk/compare/2.2.1...2.3.0) (2024-10-24)


### Features

* support prerequisite relation data in all_flags_detail ([#99](https://github.com/launchdarkly/rust-server-sdk/issues/99)) ([d0ad003](https://github.com/launchdarkly/rust-server-sdk/commit/d0ad00309b83d0157c729b534a873e230d71a4c8))

## [2.2.1](https://github.com/launchdarkly/rust-server-sdk/compare/2.2.0...2.2.1) (2024-08-08)


### Bug Fixes

* Allow event retries even if initial request fails to connect ([#93](https://github.com/launchdarkly/rust-server-sdk/issues/93)) ([a9c0150](https://github.com/launchdarkly/rust-server-sdk/commit/a9c01501296be0f12ad23c0c8ee9441c5b531d73))
* Suppress error log on `es::Error::Eof` ([#96](https://github.com/launchdarkly/rust-server-sdk/issues/96)) ([20d0891](https://github.com/launchdarkly/rust-server-sdk/commit/20d0891f410dac7b16d52e1be3aa97e47428114c))

## [2.2.0](https://github.com/launchdarkly/rust-server-sdk/compare/2.1.0...2.2.0) (2024-07-19)


### Features

* Add option to omit anonymous users from index and identify events ([#89](https://github.com/launchdarkly/rust-server-sdk/issues/89)) ([78c9668](https://github.com/launchdarkly/rust-server-sdk/commit/78c9668ed7999873b8ff045b61092363adbd6fc2))
* Add support for migrations ([#90](https://github.com/launchdarkly/rust-server-sdk/issues/90)) ([445ab74](https://github.com/launchdarkly/rust-server-sdk/commit/445ab74b9da88b8cf3904c50d74900f448ae02fe))
* Add wait_for_initialization with timeout parameter ([#76](https://github.com/launchdarkly/rust-server-sdk/issues/76)) ([45e3451](https://github.com/launchdarkly/rust-server-sdk/commit/45e3451b80e4f3104795410655e845cf9bfb7962))


### Bug Fixes

* Bump rustc to 1.74 ([#78](https://github.com/launchdarkly/rust-server-sdk/issues/78)) ([0c1c58d](https://github.com/launchdarkly/rust-server-sdk/commit/0c1c58d446b1f8c4cbaaf0813ee1855683cd319f))

## [2.1.0](https://github.com/launchdarkly/rust-server-sdk/compare/2.0.2...2.1.0) (2024-03-15)


### Features

* Inline contexts for all evaluation events ([#63](https://github.com/launchdarkly/rust-server-sdk/issues/63)) ([b31b5e7](https://github.com/launchdarkly/rust-server-sdk/commit/b31b5e77cc2a0edf7fdbed84974c76df7b3a02d4))
* Redact anonymous attributes within feature events ([#64](https://github.com/launchdarkly/rust-server-sdk/issues/64)) ([66e2e54](https://github.com/launchdarkly/rust-server-sdk/commit/66e2e54106cbed5c2f35806eaff0165f5351ccc6))

## [2.0.2](https://github.com/launchdarkly/rust-server-sdk/compare/2.0.1...2.0.2) (2023-12-21)


### Bug Fixes

* Bump MSRV to 1.70.0 ([#61](https://github.com/launchdarkly/rust-server-sdk/issues/61)) ([3a4d8e7](https://github.com/launchdarkly/rust-server-sdk/commit/3a4d8e734d25b0adea7e77d0b43c13f451059b6a))
* **deps:** Bump hyper to fix CVE-2022-31394 ([#59](https://github.com/launchdarkly/rust-server-sdk/issues/59)) ([fdd2c32](https://github.com/launchdarkly/rust-server-sdk/commit/fdd2c3285dacf8caf3a0730f2b760e69707086be))
* **deps:** Bump tokio to fix CVE-2021-45710 ([#60](https://github.com/launchdarkly/rust-server-sdk/issues/60)) ([64d6e7b](https://github.com/launchdarkly/rust-server-sdk/commit/64d6e7b0eef3183ea6de89eaa35708f838c2cbfb))

## [2.0.1](https://github.com/launchdarkly/rust-server-sdk/compare/2.0.0...2.0.1) (2023-11-22)


### Bug Fixes

* Export config::BuildError as ConfigBuildError ([#57](https://github.com/launchdarkly/rust-server-sdk/issues/57)) ([28e8d78](https://github.com/launchdarkly/rust-server-sdk/commit/28e8d78ad485f025a5d12d198fb5a67fd157d815))

## [2.0.0](https://github.com/launchdarkly/rust-server-sdk/compare/1.1.3...2.0.0) (2023-11-17)


### ⚠ BREAKING CHANGES

* Make rustls dependency optional ([#136](https://github.com/launchdarkly/rust-server-sdk/issues/136))
* Update to latest event source client ([#135](https://github.com/launchdarkly/rust-server-sdk/issues/135))

### Features

* Make rustls dependency optional ([#136](https://github.com/launchdarkly/rust-server-sdk/issues/136)) ([fac8df7](https://github.com/launchdarkly/rust-server-sdk/commit/fac8df750ed4ab233edb62fc6d77277ed18e5dba))
* Update to latest event source client ([#135](https://github.com/launchdarkly/rust-server-sdk/issues/135)) ([91f7297](https://github.com/launchdarkly/rust-server-sdk/commit/91f72970908c786c351958311e0ff76ffde715d9))

## [1.1.3] - 2023-08-10
### Fixed:
- Fixed an issue with evaluating user targets as part of a multi-kind context.

## [1.1.2] - 2023-06-26
### Removed:
- Removed reliance on `built` crate.

## [1.1.1] - 2023-05-10
### Fixed:
- The secure mode hashing algorithm has been updated to use the context's fully qualified key.

## [1.1.0] - 2023-05-03
### Changed:
- Updated MSRV from 1.60.0 to 1.64.0

### Fixed:
- Ensure an error is logged if a persistence store fails to initialize appropriately.
- Resolve issue with parsing partially read polling response bodies.

## [1.0.0] - 2022-12-07
The latest version of this SDK supports LaunchDarkly's new custom contexts feature. Contexts are an evolution of a previously-existing concept, "users." Contexts let you create targeting rules for feature flags based on a variety of different information, including attributes pertaining to users, organizations, devices, and more. You can even combine contexts to create "multi-contexts." 

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
