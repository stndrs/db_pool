import db/pool
import exception
import gleam/erlang/process
import gleam/otp/static_supervisor
import global_value

pub fn start_test() {
  let new_pool =
    pool.new()
    |> pool.size(2)
    |> pool.on_open(fn() { Ok(Nil) })
    |> pool.on_close(fn(_) { Ok(Nil) })
    |> pool.on_ping(fn(_) { Ok(Nil) })

  let name = process.new_name("db_pool_test")
  let assert Ok(pool) = pool.start(new_pool, name, 200)

  let assert Ok(_) = pool.shutdown(pool.data, 200)
}

pub fn supervised_test() {
  let name = process.new_name("db_pool_test")

  let new_pool =
    pool.new()
    |> pool.size(2)
    |> pool.on_open(fn() { Ok(Nil) })
    |> pool.on_close(fn(_) { Ok(Nil) })
    |> pool.on_ping(fn(_) { Ok(Nil) })

  let pool_spec = pool.supervised(new_pool, name, 200)

  let assert Ok(_) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(pool_spec)
    |> static_supervisor.start
}

pub fn checkout_checkin_test() {
  let pool = db_pool()

  let self = process.self()

  let assert Ok(Nil) = pool.checkout(pool, self, 200)

  pool.checkin(pool, Nil, self)
}

pub fn checkout_waiting_test() {
  let name = process.new_name("db_pool_test")

  let db_pool =
    pool.new()
    |> pool.size(1)
    |> pool.on_open(fn() { Ok(Nil) })
    |> pool.on_close(fn(_) { Ok(Nil) })
    |> pool.on_ping(fn(_) { Ok(Nil) })

  let assert Ok(pool) = pool.start(db_pool, name, 200)

  process.spawn_unlinked(fn() {
    let self = process.self()

    let assert Ok(Nil) = pool.checkout(pool.data, self, 50)

    process.sleep(100)

    pool.checkin(pool.data, Nil, self)
  })

  process.spawn_unlinked(fn() {
    let self = process.self()

    let assert Ok(Nil) = pool.checkout(pool.data, self, 200)

    process.sleep(100)

    pool.checkin(pool.data, Nil, self)
  })

  process.sleep(300)

  let assert Ok(_) = pool.shutdown(pool.data, 200)
}

pub fn checkout_exhaustion_test() {
  let pool = db_pool()

  process.spawn_unlinked(fn() {
    let self = process.self()
    let assert Ok(Nil) = pool.checkout(pool, self, 50)

    process.sleep(200)

    pool.checkin(pool, Nil, self)
  })

  process.spawn_unlinked(fn() {
    let self = process.self()
    let assert Ok(Nil) = pool.checkout(pool, self, 50)

    process.sleep(200)

    pool.checkin(pool, Nil, self)
  })

  process.sleep(100)

  let assert Error(_) =
    exception.rescue(fn() { pool.checkout(pool, process.self(), 50) })
}

pub fn caller_down_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    pool.new()
    |> pool.size(1)
    |> pool.on_open(fn() { Ok(Nil) })
    |> pool.on_close(fn(_) { Ok(Nil) })
    |> pool.on_ping(fn(_) { Ok(Nil) })

  let assert Ok(pool) = pool.start(pool, name, 200)

  process.spawn_unlinked(fn() {
    let self = process.self()

    let assert Ok(Nil) = pool.checkout(pool.data, self, 200)

    panic as "Crash!"
  })

  process.sleep(200)

  let self = process.self()

  let assert Ok(Nil) = pool.checkout(pool.data, self, 200)

  let assert Ok(_) = pool.shutdown(pool.data, 200)
}

fn db_pool() -> process.Subject(pool.Msg(Nil, err)) {
  global_value.create_with_unique_name("db_pool_test", fn() {
    let name = process.new_name("db_pool_test")

    let db_pool =
      pool.new()
      |> pool.size(2)
      |> pool.on_open(fn() { Ok(Nil) })
      |> pool.on_close(fn(_) { Ok(Nil) })
      |> pool.on_ping(fn(_) { Ok(Nil) })

    let assert Ok(pool) = pool.start(db_pool, name, 200)

    pool.data
  })
}
