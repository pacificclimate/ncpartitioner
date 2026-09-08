# Changelog

All notable changes to this project will be documented in this file.

## 0.2.0 - 2026-09-08

### Added

- Add asynchronous NetCDF slicing with queued background jobs.
- Add Dragonfly/Redis-backed FIFO job queue for slice requests.
- Add job status polling with queued, running, complete, and failed states.
- Add queue position and processing progress reporting.
- Add configurable timeout for abandoned queued jobs.
- Add support for `.dds` targets and ASCII dimension requests.
- Expand automated tests for request handling, validation, queueing, slicing, and worker behaviour.

### Changed

- Replace synchronous `ncks`-based slice generation with an asynchronous direct NetCDF writer.
- Rename internal and user-facing "partition" terminology to "slice" where appropriate.
- Update the container base image from Python 3.9 to Python 3.12.
- Update Poetry installation and dependency configuration.
- Improve filename, output-path, and input sanitization handling.
- Update CI configuration.
- Expand development and deployment documentation.

### Fixed

- Preserve the source NetCDF format when subsetting classic NetCDF files.
- Correct redirect URLs for generated downloads.
- Check subprocess return status so processing failures are surfaced correctly.

## 0.1.0 - 2025-12-05

### Added

- Initial NCPartitioner implementation.
- Add HTTP service for creating user-requested NetCDF subsets.
- Add NetCDF subsetting using `ncks`.
- Add THREDDS redirects for generated files and metadata requests.
- Add request validation and sanitization.
- Add Flask/Gunicorn application setup.
- Add Docker image configuration.
- Add Poetry packaging and dependency management.
- Add initial pytest test suite and CI configuration.