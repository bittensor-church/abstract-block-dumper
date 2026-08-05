# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This project uses [*towncrier*](https://towncrier.readthedocs.io/) and the changes for the
upcoming release can be found in [changelog.d](changelog.d).

<!-- towncrier release notes start -->

## [0.1.9](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.1.9) - 2026-08-05

### Changed

- Upgraded bittensor SDK requirement to >=11.0.1 and migrated from the removed `Subtensor.get_current_block()` method to the `Subtensor.block` property.


## [0.1.8](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.1.8) - 2026-07-01

### Fixed

- The `BITTENSOR_NETWORK` Django setting is now honored by the live block scheduler (`block_tasks_v1`); previously the scheduler always connected to `finney` regardless of the setting.


## [0.1.7](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.1.7) - 2026-07-01

### Added

- The live block scheduler now honors each task's `backfilling_lookback`, backfilling the trailing block window on every new head via the shared `WindowBackfiller` (skipping already-executed and in-flight blocks). Gated by the new `BLOCK_DUMPER_LOOKBACK_ENABLED` setting (default on).


## [0.1.6](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.1.6) - 2026-02-04

No significant changes.


## [0.1.5](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.1.5) - 2026-02-04

No significant changes.


## [0.1.3](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.1.3) - 2026-02-04

No significant changes.


## [0.1.2](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.1.2) - 2026-01-12

No significant changes.


## [0.1.1](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.1.1) - 2025-12-18

No significant changes.


## [0.1.0](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.1.0) - 2025-12-10

No significant changes.


## [0.0.9](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.0.9) - 2025-12-04

No significant changes.


## [0.0.8](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.0.8) - 2025-12-04

No significant changes.


## [0.0.7](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.0.7) - 2025-12-01

No significant changes.


## [0.0.6](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.0.6) - 2025-11-26

No significant changes.


## [0.0.4](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.0.4) - 2025-11-17

No significant changes.


## [0.0.5](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.0.5) - 2025-11-17

No significant changes.


## [0.0.2](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.0.2) - 2025-10-24

No significant changes.


## [0.0.1](https://github.com/bactensor/abstract-block-dumper/releases/tag/v0.0.1) - 2025-10-21

No significant changes.
