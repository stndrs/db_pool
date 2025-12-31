import db/pool
import gleam/erlang/process
import gleam/int
import gleam/otp/actor
import gleam/otp/static_supervisor
import global_value

pub fn new_error_test() {
  let db_pool = pool.new()

  let name = process.new_name("db_pool_test")

  let assert Error(actor.InitFailed(_)) = pool.start(db_pool, name, 200)
}

pub fn start_test() {
  let new_pool =
    pool.new()
    |> pool.size(2)
    |> pool.on_open(fn() { Ok(Nil) })
    |> pool.on_close(fn(_) { Ok(Nil) })
    |> pool.on_interval(fn(_) { Nil })

  let name = process.new_name("db_pool_test")
  let assert Ok(pool) = pool.start(new_pool, name, 200)

  let assert Ok(_) = pool.shutdown(pool.data, 200)
}

pub fn start_error_test() {
  let new_pool =
    pool.new()
    |> pool.size(2)
    |> pool.on_open(fn() { Error("oops") })
    |> pool.on_close(fn(_) { Ok(Nil) })
    |> pool.on_interval(fn(_) { Nil })

  let name = process.new_name("db_pool_test")

  let assert Error(actor.InitFailed("(db_pool) Failed to open connections")) =
    pool.start(new_pool, name, 200)
}

pub fn supervised_test() {
  let name = process.new_name("db_pool_test")

  let new_pool =
    pool.new()
    |> pool.size(2)
    |> pool.on_open(fn() { Ok(Nil) })
    |> pool.on_close(fn(_) { Ok(Nil) })
    |> pool.on_interval(fn(_) { Nil })

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

pub fn checkout_exhaustion_test() {
  let pool = db_pool()

  process.spawn(fn() {
    let self = process.self()
    let assert Ok(Nil) = pool.checkout(pool, self, 50)

    process.sleep(200)

    pool.checkin(pool, Nil, self)
  })

  process.spawn(fn() {
    let self = process.self()
    let assert Ok(Nil) = pool.checkout(pool, self, 50)

    process.sleep(200)

    pool.checkin(pool, Nil, self)
  })

  process.sleep(100)

  let assert Error(pool.ConnectionTimeout) =
    pool.checkout(pool, process.self(), 50)
}

pub fn caller_down_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    pool.new()
    |> pool.size(1)
    |> pool.on_open(fn() { Ok(int.random(10)) })
    |> pool.on_close(fn(_) { Ok(Nil) })
    |> pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = pool.start(pool, name, 200)

  let caller =
    process.spawn_unlinked(fn() {
      let self = process.self()

      let assert Ok(_conn) = pool.checkout(pool.data, self, 100)

      process.sleep_forever()
    })

  process.sleep(200)

  process.kill(caller)

  let self = process.self()

  let assert Ok(_conn) = pool.checkout(pool.data, self, 200)

  let assert Ok(_) = pool.shutdown(pool.data, 200)
}

pub fn waiting_caller_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    pool.new()
    |> pool.size(1)
    |> pool.on_open(fn() { Ok(Nil) })
    |> pool.on_close(fn(_) { Ok(Nil) })
    |> pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = pool.start(pool, name, 200)

  process.spawn(fn() {
    let self = process.self()

    let assert Ok(Nil) = pool.checkout(pool.data, self, 50)

    process.sleep(100)

    pool.checkin(pool.data, Nil, self)
  })

  process.spawn(fn() {
    let self = process.self()

    let assert Ok(Nil) = pool.checkout(pool.data, self, 200)

    pool.checkin(pool.data, Nil, self)
  })

  process.sleep(250)

  let assert Ok(_) = pool.shutdown(pool.data, 100)
}

pub fn waiting_caller_timeout_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    pool.new()
    |> pool.size(1)
    |> pool.on_open(fn() { Ok(Nil) })
    |> pool.on_close(fn(_) { Ok(Nil) })
    |> pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = pool.start(pool, name, 100)

  process.spawn(fn() {
    let self = process.self()

    let assert Ok(Nil) = pool.checkout(pool.data, self, 100)

    process.sleep(100)

    pool.checkin(pool.data, Nil, self)
  })

  process.spawn(fn() {
    let self = process.self()

    let assert Error(pool.ConnectionTimeout) =
      pool.checkout(pool.data, self, 100)

    pool.checkin(pool.data, Nil, self)
  })

  process.sleep(150)

  let assert Ok(_) = pool.shutdown(pool.data, 100)
}

pub fn pool_exit_test() {
  let name = process.new_name("db_pool_test")

  let db_pool =
    pool.new()
    |> pool.size(2)
    |> pool.on_open(fn() { Ok(Nil) })
    |> pool.on_close(fn(_) { Ok(Nil) })
    |> pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = pool.start(db_pool, name, 200)

  let assert Ok(pid) = process.subject_owner(pool.data)

  // Doesn't crash
  process.send_exit(pid)
}

fn db_pool() -> process.Subject(pool.Message(Nil, err)) {
  global_value.create_with_unique_name("db_pool_test", fn() {
    let name = process.new_name("db_pool_test")

    let db_pool =
      pool.new()
      |> pool.size(2)
      |> pool.on_open(fn() { Ok(Nil) })
      |> pool.on_close(fn(_) { Ok(Nil) })
      |> pool.on_interval(fn(_) { Nil })

    let assert Ok(pool) = pool.start(db_pool, name, 200)

    pool.data
  })
}
