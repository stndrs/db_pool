# db_pool

[![Package Version](https://img.shields.io/hexpm/v/db_pool)](https://hex.pm/packages/db_pool)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/db_pool/)

A database connection pool for Gleam on Erlang/OTP.

This library eagerly opens connections at startup. Connections can be checked out from the pool, marking them as active. Active connections are associated with the `Pid` of the caller that checked it out. Checking in connections will remove the association with the caller. Because active connections are associated with the `Pid` of a calling process, subsequent calls to check out a connection from the same process will return the already checked out connection.

If all connections are checked out, new callers attempting to check out will be added to a FIFO queue. Callers waiting in the queue will be given connections as they become available.

Callers are monitored so if they crash their checked out connections are closed and replaced with new ones.

```gleam
import database
import db_pool
import gleam/erlang/process

pub fn main() -> Nil {
  let name = process.new_name("db_pool")

  let db_pool =
    db_pool.new()
    |> db_pool.size(5)
    |> db_pool.on_open(database.open)
    |> db_pool.on_close(database.close)
    |> db_pool.on_idle(database.idle)
    |> db_pool.on_active(database.active)

  let assert Ok(pool) = db_pool.start(db_pool, name, 1000)
  let pool = pool.data

  let assert Ok(users) =
    db_pool.with_connection(pool, 500, 30_000, fn(conn) {
      database.query("SELECT * FROM users", conn)
    })

  let assert Ok(_) = db_pool.shutdown(pool, 1000)
}
```

## Installation

```sh
gleam add db_pool
```

## Error handling

All checkout and shutdown operations return `Result(value, PoolError(err))`. The three error variants are:

- **`ConnectionError(err)`** — wraps an error returned by the `on_open` or `on_close` callback. The inner `err` type is determined by your callback.
- **`ConnectionTimeout`** — the checkout request timed out while waiting in the queue. No connection was available within the specified `timeout`.
- **`ConnectionUnavailable`** — the request was dropped by the CoDel algorithm due to sustained overload, the pool shut down while the caller was waiting, or the pool actor is unreachable.

## Checkout and checkin

`with_connection` (shown above) is the recommended API — it automatically checks the connection back in when the callback returns. For cases where you need manual control over the connection lifecycle, use `checkout` and `checkin` directly:

```gleam
let caller = process.self()

// timeout: 500ms queue wait, deadline: 30s max hold time
let assert Ok(conn) = db_pool.checkout(pool, caller, 500, 30_000)

// Use the connection...
let users = database.query("SELECT * FROM users", conn)

// Return it to the pool when done.
db_pool.checkin(pool, conn, caller)
```

The `deadline` parameter sets the maximum time in milliseconds that a connection may be held. If the caller has not checked in by then, the pool forcibly closes the connection and opens a replacement. The caller is left holding a now-closed connection.

Re-entrant: calling `checkout` again from the same process returns the already checked-out connection. The original deadline is preserved — a second checkout cannot extend it.

## CoDel queue management

The pool uses the [CoDel](https://en.wikipedia.org/wiki/CoDel) (Controlled Delay) algorithm to shed load under sustained overload. When queue delay exceeds the target for a full measurement interval, the pool begins dropping requests with `ConnectionUnavailable` instead of letting them wait indefinitely.

Two parameters control CoDel behavior:

- **`queue_target`** — the maximum acceptable queue delay in milliseconds (default: 50ms).
- **`queue_interval`** — the length of each measurement interval in milliseconds (default: 1000ms).

```gleam
let db_pool =
  db_pool.new()
  |> db_pool.size(10)
  |> db_pool.queue_target(100)
  |> db_pool.queue_interval(2000)
  |> db_pool.on_open(database.open)
  |> db_pool.on_close(database.close)
```

Under normal load, CoDel has no effect — requests are served immediately or within the target delay. It only activates when the pool is sustainedly overloaded, preventing unbounded queue growth.

## Supervision

Use `supervised` to add the pool to a supervision tree:

```gleam
import gleam/otp/static_supervisor

pub fn main() {
  let db_pool =
    db_pool.new()
    |> db_pool.size(5)
    |> db_pool.on_open(database.open)
    |> db_pool.on_close(database.close)

  let name = process.new_name("db_pool")

  static_supervisor.new(static_supervisor.OneForOne)
  |> static_supervisor.add(db_pool.supervised(db_pool, name, 5000))
  |> static_supervisor.start_link
}
```

The pool is started with a `Transient` restart strategy — it is restarted only if it terminates abnormally.

## Reconnection

When a connection is lost (caller crash, deadline expiry, or unexpected exit), the pool closes the old connection and immediately attempts to open a replacement. If the replacement fails, the pool retries with exponential backoff and jitter — initial delay of 500 ms–1 s, capped at 30 s. No user configuration is needed; reconnection is automatic.

## Development

```sh
gleam test  # Run the tests
```

## Acknowledgements

Inspired in part by [`bath`](https://github.com/Pevensie/bath).
