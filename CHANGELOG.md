# CHANGELOG

<!-- version list -->

## v0.5.0 (2026-10-14)

### Documentation

- Add versioning and deprecation policy ([#7](https://github.com/lawrnsfeng/mint/pull/7),
  [`a1b03dd`](https://github.com/lawrnsfeng/mint/commit/a1b03dde0c83adc32dd67bf2ba6d5ed527fd97f3))

### Features

- **db**: Add timeouts, pool-exhaustion error and scope retry
  ([#7](https://github.com/lawrnsfeng/mint/pull/7),
  [`a1b03dd`](https://github.com/lawrnsfeng/mint/commit/a1b03dde0c83adc32dd67bf2ba6d5ed527fd97f3))

- **db**: Make the session lifecycle importable without sqlmodel
  ([#7](https://github.com/lawrnsfeng/mint/pull/7),
  [`a1b03dd`](https://github.com/lawrnsfeng/mint/commit/a1b03dde0c83adc32dd67bf2ba6d5ed527fd97f3))


## v0.4.0 (2026-10-14)

### Build System

- Commit a refreshed lock and test against it ([#5](https://github.com/lawrnsfeng/mint/pull/5),
  [`bc1fdd6`](https://github.com/lawrnsfeng/mint/commit/bc1fdd6cd3fd53af0d4a232f3b57b6cd79047e72))

### Continuous Integration

- Run tests with a postgres matrix and coverage gate
  ([#5](https://github.com/lawrnsfeng/mint/pull/5),
  [`bc1fdd6`](https://github.com/lawrnsfeng/mint/commit/bc1fdd6cd3fd53af0d4a232f3b57b6cd79047e72))

### Features

- **db**: Scope a session to any function and commit once
  ([#6](https://github.com/lawrnsfeng/mint/pull/6),
  [`4522018`](https://github.com/lawrnsfeng/mint/commit/452201864b8797d22395cc951f51b29bf7ebcd5b))


## v0.3.0 (2026-09-05)

### Bug Fixes

- Pre-commit complaints ([#3](https://github.com/lawrnsfeng/mint/pull/3),
  [`b3f4134`](https://github.com/lawrnsfeng/mint/commit/b3f4134908716c43be055f4c9b410cceca839323))

### Documentation

- **fs**: Show every way to supply a storage client
  ([#3](https://github.com/lawrnsfeng/mint/pull/3),
  [`b3f4134`](https://github.com/lawrnsfeng/mint/commit/b3f4134908716c43be055f4c9b410cceca839323))

- **rules**: Forbid tool and assistant attribution in commit messages
  ([#3](https://github.com/lawrnsfeng/mint/pull/3),
  [`b3f4134`](https://github.com/lawrnsfeng/mint/commit/b3f4134908716c43be055f4c9b410cceca839323))

### Features

- Storage client cache ([#3](https://github.com/lawrnsfeng/mint/pull/3),
  [`b3f4134`](https://github.com/lawrnsfeng/mint/commit/b3f4134908716c43be055f4c9b410cceca839323))

- **fs**: Cache storage clients per configuration and event loop
  ([#3](https://github.com/lawrnsfeng/mint/pull/3),
  [`b3f4134`](https://github.com/lawrnsfeng/mint/commit/b3f4134908716c43be055f4c9b410cceca839323))


## v0.2.0 (2026-07-19)

### Features

- Add db and repo implementation ([#2](https://github.com/lawrnsfeng/mint/pull/2),
  [`f891ee8`](https://github.com/lawrnsfeng/mint/commit/f891ee88481bb31be8a0be0759062f06c5aaf814))


## v0.1.0 (2026-07-19)

- Initial Release
