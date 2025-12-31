# db_pool

[![Package Version](https://img.shields.io/hexpm/v/db_pool)](https://hex.pm/packages/db_pool)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/db_pool/)

A database connection pool.

This library eagerly opens connections at startup. Connections can be checked out from the pool, marking them as active. Active connections are associated with the `Pid` of the caller that checked it out. Checking in connections will remove the association with the caller. Because active connections are associated with the `Pid` of a calling process, subsequent calls to check out a connection from the same process will return the already checked out connection.

If all connections are checked out, new callers attempting to check out will be added to a FIFO queue. Callers waiting in the queue will be given connections as they become available.

Callers are monitored so if they crash their checked out connections can be added back to the pool.

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
    |> db_pool.on_interval(database.ping)

  let assert Ok(started) = db_pool.start(db_pool, name, 1000)

  let pool = started.data

  let self = process.self()

  let assert Ok(conn) = db_pool.checkout(pool, self, 500)

  let assert Ok(users) = database.query("SELECT * FROM users", conn)

  db_pool.checkin(pool, conn, self)

  let assert Ok(_) = db_pool.shutdown(pool, 1000)
}
```

## Installation

```sh
gleam add db_pool
```

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
```

## Acknowledgements

Inspired in part by [`bath`](https://github.com/Pevensie/bath).
