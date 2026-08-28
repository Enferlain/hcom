# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Rules:

- Use proper sub titles "Added", "Changed", "Removed" and "Fixed"
- Keep proper track of days for where entries should go
- Be concise but mention all changes without necessarily detailing each one

## [2026-08-28]

### Added

- Added `hcom events --cursor` and `--result-from <agent>` for durable,
  generation-aware worker-result waits.

### Changed

- Recipient-free `hcom send` calls now require `--broadcast`; targeted and
  seeded-thread sends continue to work without it.

### Fixed

- Prevented result waits from accepting another worker's message, a result
  from the wrong workflow thread, or a stale result from a reused agent name.
- Result correlation now survives report-then-stop ordering and ignores failed
  launch placeholder stops.
- `--result-from` now fails clearly when combined with an events subcommand
  instead of being silently ignored.

### Removed
