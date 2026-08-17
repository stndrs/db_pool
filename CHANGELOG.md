# Changelog

## 3.0.0

### Bug fixes

- Fixed a crash under load where two callers enqueued within the same
  nanosecond tick could collide on the queue's timestamp key, causing
  `queue.push` to fail and taking down the entire pool actor (every active
  connection and every waiter). The internal queue now uses a strictly-unique
  key, and the enqueue path no longer asserts on `queue.push`. CoDel delay
  measurements use a real monotonic timestamp stored per waiter rather than the
  queue key.

### Breaking changes

- Removed the `caller` parameter from `checkout` and `checkin`. The caller is
  now always derived from `process.self()`. Update calls from
  `checkout(pool, caller, timeout, deadline)` to
  `checkout(pool, timeout, deadline)`, and from `checkin(pool, conn, caller)`
  to `checkin(pool, conn)`.
- Added `error_to_string`, a `Pool` builder function that maps errors to
  strings. This allows `db_pool` to log and return clearer error strings for
  the generic component of `PoolError` provided by callers.

### Internal

- Guard reconnect against `max_size` so pool capacity is always enforced.
- Treat a supervisor `shutdown` exit reason as a clean stop.
- Clamp pool size, interval, timeout and deadline to safe ranges.
- Read the real clock in the CoDel poll and fix interval boundary drift.
- Track checkout depth so nested checkins don't release a connection early.
- Route the mismatched-checkin warning through `logging` instead of stderr.
- Updated dependencies.

## 2.0.1

- Updated `rasa` and `gleam_stdlib`

## 2.0.0

### New features

- Added CoDel queue management algorithm with configurable
  `queue_target` and `queue_interval`. Sheds load under sustained
  overload by returning `ConnectionUnavailable`
- Added checkout deadlines. The pool closes connections held past
  their deadline
- Added `with_connection`, a convenience function that checks out a connection,
  runs a callback, and automatically checks it back in
- Added `on_idle` callback, invoked on every checkin and once per connection at
  pool startup
- Added `on_active` callback, invoked on every checkout
- Added re-entrant checkout. A process that already holds a connection
  receives the same one.
- Added `supervised`, a convenience function that creates a
  `ChildSpecification` for adding the pool to a supervision tree

### Breaking changes

- Removed `on_interval`. Use `on_idle` and `on_active` instead
- Removed the `interval` configuration function. The poll interval is now
  managed internally
- Changed the `checkout` signature to require a `deadline` parameter
- Changed `start` to return `actor.Started(Subject(...))`
- Added `ConnectionUnavailable` variant to `PoolError`

### Bug fixes

- Fixed shutdown and exit to drain waiting callers with `ConnectionUnavailable`
  instead of leaving them blocked forever
- Fixed shutdown and exit to close active (checked-out) connections, cancel
  their deadline timers, and remove their monitors
- Fixed checkin to no longer crash on a mismatched connection. Logs a warning
  to stderr instead

### Internal

- Replaced internal queue implementation with
  [`rasa/queue`](https://hexdocs.pm/rasa/)
- Upgraded queue counter from millisecond to microsecond resolution to prevent
  key collisions under contention
