# Changelog

## 2.0.0

### New features

- Added CoDel (Controlled Delay) queue management algorithm with configurable
  `queue_target` and `queue_interval` — adaptively sheds load under sustained
  overload by returning `ConnectionUnavailable`
- Added checkout deadlines — the pool forcibly closes connections held past
  their deadline, preventing runaway resource hoarding
- Added automatic reconnection with exponential backoff and jitter (500 ms –
  30 s) when a connection process exits unexpectedly
- Added `with_connection`, a convenience function that checks out a connection,
  runs a callback, and automatically checks it back in
- Added `on_idle` callback, invoked on every checkin and once per connection at
  pool startup
- Added `on_active` callback, invoked on every checkout
- Added re-entrant checkout — a process that already holds a connection
  receives the same one instead of deadlocking
- `checkout` and `with_connection` now return `ConnectionUnavailable` instead
  of blocking forever or panicking when the pool actor is unreachable

### Breaking changes

- Removed `on_interval` — use `on_idle` and `on_active` instead
- Removed the `interval` configuration function — the poll interval is now
  managed internally
- Changed the `checkout` signature to require a `deadline` parameter
  (milliseconds)
- Changed `start` to return `actor.Started(Subject(...))` — call `.data` on
  the result to obtain the pool subject
- Added `ConnectionUnavailable` variant to `PoolError`

### Bug fixes

- Fixed shutdown and exit to drain waiting callers with `ConnectionUnavailable`
  instead of leaving them blocked forever
- Fixed shutdown and exit to close active (checked-out) connections, cancel
  their deadline timers, and remove their monitors
- Fixed checkin to no longer crash on a mismatched connection — logs a warning
  to stderr instead

### Internal

- Replaced internal queue implementation with
  [`rasa/queue`](https://hexdocs.pm/rasa/)
- Upgraded queue counter from millisecond to microsecond resolution to prevent
  key collisions under contention
