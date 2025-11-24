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
    |> pool.on_close(fn(_) { Nil })
    |> pool.on_ping(fn(_) { Nil })

  let assert Ok(_) = pool.start(new_pool, 200)

  let assert Ok(_) = pool.shutdown(new_pool, 200)
}

pub fn ping_test() {
  let new_pool =
    pool.new()
    |> pool.size(2)
    |> pool.on_open(fn() { Ok(Nil) })
    |> pool.on_close(fn(_) { Nil })
    |> pool.on_ping(fn(_) {
      let assert Ok(_) = ping()

      Nil
    })

  let assert Ok(_) = pool.start(new_pool, 200)

  let assert Ok(_) = pool.shutdown(new_pool, 200)
}

pub fn ping_error_test() {
  let new_pool =
    pool.new()
    |> pool.size(2)
    |> pool.on_open(fn() { Ok(Nil) })
    |> pool.on_close(fn(_) { Nil })
    |> pool.on_ping(fn(_) {
      let assert Error(_) =
        exception.rescue(fn() {
          let assert Error(_) = ping()
        })

      Nil
    })

  let assert Ok(_) = pool.start(new_pool, 200)

  let assert Ok(_) = pool.shutdown(new_pool, 200)
}

fn ping() {
  Ok(Nil)
}

pub fn supervised_test() {
  let new_pool =
    pool.new()
    |> pool.size(2)
    |> pool.on_open(fn() { Ok(Nil) })
    |> pool.on_close(fn(_) { Nil })
    |> pool.on_ping(fn(_) { Nil })

  let pool_spec = pool.supervised(new_pool, 200)

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
  let pool =
    pool.new()
    |> pool.size(1)
    |> pool.on_open(fn() { Ok(Nil) })
    |> pool.on_close(fn(_) { Nil })
    |> pool.on_ping(fn(_) { Nil })

  let assert Ok(_) = pool.start(pool, 50)

  let _ =
    exception.rescue(fn() {
      process.spawn_unlinked(fn() {
        let self = process.self()

        let assert Ok(Nil) = pool.checkout(pool, self, 50)

        panic as "Crash!"
      })
    })

  // process.sleep(200)

  let assert Ok(Nil) = pool.checkout(pool, process.self(), 100)

  process.sleep(200)

  let assert Ok(_) = pool.shutdown(pool, 100)
}

fn db_pool() -> pool.Pool(Nil) {
  global_value.create_with_unique_name("db_pool_test", fn() {
    let db_pool =
      pool.new()
      |> pool.size(2)
      |> pool.on_open(fn() { Ok(Nil) })
      |> pool.on_close(fn(_) { Nil })
      |> pool.on_ping(fn(_) { Nil })

    let assert Ok(_) = pool.start(db_pool, 200)

    db_pool
  })
}
