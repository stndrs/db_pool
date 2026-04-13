# Changelog

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
