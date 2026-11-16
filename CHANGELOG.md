# Changelog

All notable changes to SGit are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Configurable resource limits, all read from the environment and disabled by
  setting them to `0` where noted:
  - `SGIT_MAX_REQUEST_SIZE_MB` (default `500`) caps push request bodies.
  - `SGIT_MAX_CONCURRENT_REQS` (default `20`, `0` disables) bounds concurrent git
    processes through a shared semaphore.
  - `SGIT_RATE_LIMIT_PER_IP` (default `30` requests/minute per IP, `0` disables)
    using a `tower-governor` token bucket.
  - `SGIT_READ_TIMEOUT` (default `15`s) for small metadata reads such as
    `info/refs`.
  - `SGIT_STREAM_TIMEOUT` (default `60`s, `0` disables, falls back to
    `SGIT_TIMEOUT`) as an idle timeout for streaming push/pull transfers.
- Per-repository disk quota via `SGIT_MAX_REPO_SIZE_MB` (unset means unlimited).
  The quota is checked before repository auto-creation and before git is
  spawned, counts the incoming request body, returns
  `507 Insufficient Storage` when exceeded, and is passed to `receive-pack` as
  `receive.maxInputSize` so a pack that expands beyond the allowance is also
  rejected. Repository size is re-checked after each push.
- `SIGHUP` configuration reload: `users.toml` is re-read into the shared
  `AppState` on Unix without a restart and without disconnecting active Git
  streams.
- `log_stderr!` macro that logs to stderr with a Unix-epoch timestamp prefix;
  used for server lifecycle messages, git stderr output, and quota warnings.
- Graceful shutdown on `SIGTERM` in addition to `SIGINT` for container
  environments.
- Request-body spooling: bodies larger than 8 MiB are written to temporary
  files (mode `0600` on Unix) instead of being held in memory, and the files are
  removed when the body is dropped.
- Async-safe helpers (`ensure_repo_exists_async`, `get_dir_size_async`,
  `update_head_if_invalid_async`) that move repository size walks and HEAD scans
  onto the blocking thread pool.
- `no-cache, max-age=0, must-revalidate` / `Pragma: no-cache` response headers on
  Git ref advertisements and RPC responses.
- Tests covering percent-encoded endpoints, Argon2id and legacy password
  verification, repository quotas, spooled request bodies (memory and disk
  paths), zero-value limit semantics, rate-limit configuration, read and idle
  timeouts, concurrency-permit lifetime, and the user-config reload path.

### Changed

- Git process concurrency is now enforced by a dedicated semaphore whose permit
  is held until the git process has actually exited and been reaped, instead of
  relying on the request-concurrency layer, which released its permit as soon as
  response headers were produced.
- The idle timeout now runs in its own task and resets on stdin or stdout
  progress, so a stalled git process is killed even when a slow client stops
  polling the response body. A value of `0` disables the timeout.
- `git-receive-pack` is no longer `SIGKILL`ed when the client disconnects:
  stdin is closed, the child gets a 60-second grace period to finish its ref
  transaction (then is killed if still running), and HEAD re-sync plus
  `git gc --auto` run after the real exit status is known.
- `info/refs` reads drain stdout and stderr concurrently so a large ref
  advertisement cannot deadlock the child on a full pipe; on timeout the child
  is killed and reaped and the pipe readers are aborted before the `504`
  response is returned.
- Zero values for `SGIT_MAX_CONCURRENT_REQS` and `SGIT_MAX_REQUEST_SIZE_MB` now
  consistently mean "disabled" instead of parking every request or rejecting
  every non-empty body.
- Git paths are stripped of exactly one endpoint suffix; requests whose
  repository portion still ends in an endpoint segment are rejected.
- Repository size and HEAD scans no longer block the async runtime.
- README documents the new configuration variables and Argon2id password-hash
  generation.
- `Cargo.lock` dependency updates, plus new `tower`, `tower-governor`,
  `argon2`, and `subtle` dependencies.

### Fixed

- Chunked push bodies (`Transfer-Encoding: chunked`, which carries no
  `Content-Length`) are now counted while reading and rejected with
  `413 Payload Too Large`; the previous byte-limited reader silently truncated
  the stream instead.
- Spooled request bodies are opened read-write so they can be rewound and fed to
  git instead of failing on the disk-spill path.
- Git child processes killed by the read or idle timeout are now reaped, so no
  zombies are left behind.
- Repository quota enforcement now happens before auto-creation and before the
  push is accepted, instead of only measuring an already-created repository and
  ignoring the incoming pack.
- Very large `SGIT_RATE_LIMIT_PER_IP` values no longer panic: the per-request
  interval is clamped to at least 1 ms and a rejected governor configuration is
  handled gracefully.
- `git` stderr is logged when a service exits unsuccessfully.
- Test-only: corrected bob's SHA-256 password hash and asserted a successful
  authenticated push, and added a negative case for a wrong password.

### Security

- Authorization now evaluates the percent-decoded path and `service` query
  value, closing an authentication bypass where encoded write endpoints such as
  `/repo/git%2Dreceive-pack` or `?service=git%2Dreceive-pack` reached handlers
  without credentials. Rejected requests no longer auto-create repositories.
  (B1)
- Passwords are hashed and verified with salted **Argon2id** (`$argon2id$...`
  PHC strings) and constant-time comparison; legacy unsalted SHA-256 hashes in
  existing `users.toml` files are still verified for compatibility. (B9)
- Git ref advertisements and RPC results are sent with no-cache headers so
  reverse proxies and clients cannot serve stale or cross-user results. (B13)

## [0.1.0] - 2026-08-09

First tagged release.

### Added

- Git Smart HTTP Protocol server built on Axum, Tokio, and `git2`, exposing
  `info/refs`, `git-upload-pack`, and `git-receive-pack` with bi-directional
  streaming and HTTP Basic authentication.
- Automatic bare-repository initialization on the first `git push`.
- Arbitrarily nested namespaces and subfolders
  (e.g. `http://localhost:3000/org/team/project.git`).
- Path-traversal protection rejecting `..`, leading/trailing slashes, repeated
  slashes, backslashes, and empty path segments.
- Read/write authorization split: clones and fetches can stay public while
  pushes require credentials from `users.toml` (`SGIT_USERS_FILE`); an absent
  users file enables public-write mode.
- SHA-256 password hashing for `users.toml` credentials (unsalted; superseded
  by Argon2id in the unreleased changes above).
- Self-healing `HEAD`: repositories whose `HEAD` is unborn or invalid are
  pointed at a valid branch (`main`/`master`) after a push.
- Asynchronous `git gc --auto` maintenance after successful pushes.
- Graceful shutdown on `SIGINT` and configurable request timeouts for stalled
  git operations.
- Environment configuration: `SGIT_HOST` (`0.0.0.0`), `SGIT_PORT` (`3000`),
  `SGIT_DATA_DIR` (`/var/lib/sgit`), `SGIT_USERS_FILE` (`users.toml`), and
  `SGIT_TIMEOUT` (`60`s).
- Multi-stage `Dockerfile` plus `Dockerfile-debian`, a size-optimized release
  profile, and a GitHub Actions workflow that publishes images to GHCR on
  `main`/`master` and `v*.*.*` tags.
- README with configuration, authentication, build, Docker, testing, and usage
  documentation.

### Changed

- Split the monolithic `src/main.rs` into `lib.rs`, `handlers.rs`, `types.rs`,
  and `utils.rs`, leaving `main.rs` responsible only for startup and shutdown.
- Added `pkt_line_encode` for safe Git pkt-line payload encoding and removed the
  unauthenticated Web UI and dumb-HTTP fallback endpoints to reduce the attack
  surface.
- User credentials are loaded once into `AppState` at startup instead of reading
  `users.toml` from disk on every request.
- Release builds use LTO, a single codegen unit, `panic = "abort"`, and symbol
  stripping; the Docker image was optimized for size.
- Environment-variable-mutating tests are serialized behind a global mutex to
  avoid races.

### Fixed

- Missing imports and mismatched types introduced during the module split.

[Unreleased]: https://github.com/codetease/sgit/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/codetease/sgit/releases/tag/v0.1.0
