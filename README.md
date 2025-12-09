# db_pool

[![Package Version](https://img.shields.io/hexpm/v/db_pool)](https://hex.pm/packages/db_pool)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/db_pool/)

```gleam
import database
import db/pool
import gleam/erlang/process

pub fn main() -> Nil {
  let db_pool =
    pool.new()
    |> pool.size(5)
    |> pool.on_open(database.open)
    |> pool.on_close(database.close)
    |> pool.on_ping(database.ping)

  let assert Ok(_) = pool.start(db_pool, 1000)

  let self = process.self()

  let assert Ok(conn) = pool.checkout(db_pool, self, 500)

  let assert Ok(users) = database.query("SELECT * FROM users", conn)

  pool.checkin(db_pool, conn, self)

  let assert Ok(_) = pool.shutdown(new_pool, 1000)
}
```

## Installation

TODO

Further documentation can be found at <https://hexdocs.pm/db_pool>.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
```
