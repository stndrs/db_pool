import db_pool
import gleam/erlang/process
import gleam/erlang/reference
import gleam/int
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleeunit
import global_value

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn new_error_test() {
  let db_pool = db_pool.new()

  let name = process.new_name("db_pool_test")

  let assert Error(actor.InitFailed(_)) = db_pool.start(db_pool, name, 200)
}

pub fn start_test() {
  let new_pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let name = process.new_name("db_pool_test")

  let assert Ok(pool) = db_pool.start(new_pool, name, 200)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

pub fn start_error_test() {
  let new_pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Error("oops") })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let name = process.new_name("db_pool_test")

  let assert Error(actor.InitFailed("(db_pool) Failed to open connections")) =
    db_pool.start(new_pool, name, 200)
}

pub fn supervised_test() {
  let name = process.new_name("db_pool_test")

  let new_pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let pool_spec = db_pool.supervised(new_pool, name, 200)

  let assert Ok(_) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(pool_spec)
    |> static_supervisor.start
}

pub fn checkout_current_connection_test() {
  let name = process.new_name("db_pool_test")

  let new_pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Ok(reference.new()) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(new_pool, name, 200)

  let self = process.self()

  let assert Ok(conn1) = db_pool.checkout(pool, self, 200, 30_000)

  let assert Ok(conn2) = db_pool.checkout(pool, self, 200, 30_000)

  assert conn1 == conn2

  process.spawn(fn() {
    let self = process.self()

    let assert Ok(conn3) = db_pool.checkout(pool, self, 200, 30_000)

    assert conn1 != conn3
  })
}

pub fn checkout_checkin_test() {
  let pool = db_pool()

  let self = process.self()

  let assert Ok(Nil) = db_pool.checkout(pool, self, 200, 30_000)

  db_pool.checkin(pool, Nil, self)
}

pub fn checkout_exhaustion_test() {
  let pool = db_pool()

  process.spawn(fn() {
    let self = process.self()
    let assert Ok(Nil) = db_pool.checkout(pool, self, 50, 30_000)

    process.sleep(100)

    db_pool.checkin(pool, Nil, self)
  })

  process.spawn(fn() {
    let self = process.self()
    let assert Ok(Nil) = db_pool.checkout(pool, self, 50, 30_000)

    process.sleep(100)

    db_pool.checkin(pool, Nil, self)
  })

  process.spawn(fn() {
    process.sleep(50)

    let self = process.self()

    let assert Error(db_pool.ConnectionTimeout) =
      db_pool.checkout(pool, self, 20, 30_000)
  })
}

pub fn caller_down_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(int.random(10)) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200)

  let caller =
    process.spawn_unlinked(fn() {
      let self = process.self()

      let assert Ok(_conn) = db_pool.checkout(pool, self, 100, 30_000)

      process.sleep_forever()
    })

  process.sleep(200)

  process.kill(caller)

  let self = process.self()

  let assert Ok(_conn) = db_pool.checkout(pool, self, 200, 30_000)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

pub fn waiting_caller_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200)

  process.spawn(fn() {
    let self = process.self()

    let assert Ok(Nil) = db_pool.checkout(pool, self, 50, 30_000)

    process.sleep(100)

    db_pool.checkin(pool, Nil, self)
  })

  process.spawn(fn() {
    let self = process.self()

    let assert Ok(Nil) = db_pool.checkout(pool, self, 200, 30_000)

    db_pool.checkin(pool, Nil, self)
  })

  process.sleep(250)

  let assert Ok(_) = db_pool.shutdown(pool, 100)
}

pub fn waiting_caller_timeout_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 100)

  process.spawn(fn() {
    let self = process.self()

    let assert Ok(Nil) = db_pool.checkout(pool, self, 100, 30_000)

    process.sleep(100)

    db_pool.checkin(pool, Nil, self)
  })

  process.spawn(fn() {
    let self = process.self()

    let assert Error(db_pool.ConnectionTimeout) =
      db_pool.checkout(pool, self, 100, 30_000)

    db_pool.checkin(pool, Nil, self)
  })

  process.sleep(150)

  let assert Ok(_) = db_pool.shutdown(pool, 100)
}

pub fn pool_exit_test() {
  let name = process.new_name("db_pool_test")

  let db_pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(db_pool, name, 200)

  let assert Ok(pid) = process.subject_owner(pool.subject)

  // Doesn't crash
  process.send_exit(pid)
}

/// When a caller holds a connection past its deadline, the pool forcefully
/// replaces the connection and subsequent checkouts succeed.
pub fn deadline_expires_and_pool_recovers_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(int.random(10_000)) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200)

  // First caller checks out with a 50ms deadline, then holds it forever
  process.spawn_unlinked(fn() {
    let self = process.self()
    let assert Ok(_conn) = db_pool.checkout(pool, self, 200, 50)
    // Hold the connection indefinitely (deadline should fire after 50ms)
    process.sleep_forever()
  })

  // Wait for the deadline to fire and the replacement connection to be opened
  process.sleep(200)

  // Second caller should be able to check out successfully because the pool
  // replaced the deadline-expired connection
  let self = process.self()
  let assert Ok(_conn) = db_pool.checkout(pool, self, 200, 30_000)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

/// When a caller checks in before the deadline, the deadline timer is
/// cancelled and the pool operates normally.
pub fn deadline_cancelled_by_checkin_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200)

  let self = process.self()

  // Checkout with a 100ms deadline
  let assert Ok(conn) = db_pool.checkout(pool, self, 200, 100)

  // Return the connection well before the deadline
  db_pool.checkin(pool, conn, self)

  // Sleep past the deadline period
  process.sleep(200)

  // Pool should still be fully operational -- checkout again
  let assert Ok(_conn2) = db_pool.checkout(pool, self, 200, 30_000)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

/// When a deadline fires and a waiting caller exists, the replacement
/// connection is given to the waiter.
pub fn deadline_expires_serves_waiting_caller_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200)

  // First caller takes the only connection with a 50ms deadline, holds forever
  process.spawn_unlinked(fn() {
    let self = process.self()
    let assert Ok(_conn) = db_pool.checkout(pool, self, 200, 50)
    process.sleep_forever()
  })

  // Give time for the first checkout to complete
  process.sleep(10)

  // Second caller tries to checkout -- will wait because pool is exhausted.
  // When the deadline fires after 50ms, the replacement should serve this waiter.
  process.spawn(fn() {
    let self = process.self()
    let assert Ok(Nil) = db_pool.checkout(pool, self, 300, 30_000)
    db_pool.checkin(pool, Nil, self)
  })

  process.sleep(300)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

// When a waiting caller dies before a connection becomes available,
// the pool skips the dead waiter and serves the next live waiter.
pub fn dead_waiter_skipped_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200)

  // Caller A takes the only connection
  let self = process.self()
  let assert Ok(Nil) = db_pool.checkout(pool, self, 200, 30_000)

  // Caller B enqueues as a waiter, then dies
  let waiter_b =
    process.spawn_unlinked(fn() {
      let self = process.self()
      let _result = db_pool.checkout(pool, self, 5000, 30_000)
      Nil
    })

  process.sleep(50)

  process.kill(waiter_b)
  process.sleep(50)

  // Caller C enqueues as a waiter
  let result_subject = process.new_subject()
  process.spawn(fn() {
    let self = process.self()
    let result = db_pool.checkout(pool, self, 5000, 30_000)
    process.send(result_subject, result)
  })

  // Give time for C to enqueue
  process.sleep(50)

  // A returns the connection -- should skip dead B and serve C
  db_pool.checkin(pool, Nil, self)

  // C should receive the connection
  let assert Ok(Ok(Nil)) = process.receive(result_subject, 500)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

/// When all waiting callers are dead, the connection returns to idle.
pub fn all_dead_waiters_connection_returns_to_idle_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200)

  // Caller A takes the only connection
  let self = process.self()
  let assert Ok(Nil) = db_pool.checkout(pool, self, 200, 30_000)

  // Caller B enqueues as a waiter, then dies
  let waiter_b =
    process.spawn_unlinked(fn() {
      let self = process.self()
      let _result = db_pool.checkout(pool, self, 5000, 30_000)
      Nil
    })

  process.sleep(50)

  process.kill(waiter_b)
  process.sleep(50)

  // A returns the connection -- should skip dead B and return conn to idle
  db_pool.checkin(pool, Nil, self)

  process.sleep(50)

  // A new caller should be able to checkout immediately (conn is idle)
  let assert Ok(Nil) = db_pool.checkout(pool, self, 200, 30_000)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

fn db_pool() -> db_pool.PoolHandle(Nil, err) {
  global_value.create_with_unique_name("db_pool_test", fn() {
    let name = process.new_name("db_pool_test")

    let db_pool =
      db_pool.new()
      |> db_pool.size(2)
      |> db_pool.on_open(fn() { Ok(Nil) })
      |> db_pool.on_close(fn(_) { Ok(Nil) })
      |> db_pool.on_interval(fn(_) { Nil })

    let assert Ok(pool) = db_pool.start(db_pool, name, 200)

    pool
  })
}
