import db_pool
import gleam/bool
import gleam/erlang/atom
import gleam/erlang/process
import gleam/erlang/reference
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleeunit
import global_value
import rasa/atomic
import rasa/table

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn new_error_test() {
  let db_pool = db_pool.new()

  let name = process.new_name("db_pool_test")

  let assert Error(actor.InitFailed(_)) =
    db_pool.start(db_pool, name, 200, None)
}

pub fn start_test() {
  let new_pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let name = process.new_name("db_pool_test")

  let assert Ok(pool) = db_pool.start(new_pool, name, 200, None)
  let pool = pool.data

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

pub fn start_error_test() {
  let new_pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Error("oops") })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let name = process.new_name("db_pool_test")

  let assert Error(actor.InitFailed("(db_pool) ConnectionError")) =
    db_pool.start(new_pool, name, 200, None)
}

pub fn supervised_test() {
  let name = process.new_name("db_pool_test")

  let new_pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let pool_spec = db_pool.supervised(new_pool, name, 200, None)

  let assert Ok(_) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(pool_spec)
    |> static_supervisor.start

  // Verify the pool is functional by checking out and checking in
  let pool = process.named_subject(name)

  let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)
  db_pool.checkin(pool, Nil)
}

pub opaque type Err {
  Oops
}

pub fn error_to_string_test() {
  let new_pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Error(Oops) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let name = process.new_name("db_pool_test")

  let assert Error(actor.InitFailed("(db_pool) ConnectionError: Oops")) =
    db_pool.start(new_pool, name, 200, Some(fn(_err) { "Oops" }))
}

pub fn with_connection_test() {
  let name = process.new_name("db_pool_test")

  let new_pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let pool_spec = db_pool.supervised(new_pool, name, 200, None)

  let assert Ok(_) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(pool_spec)
    |> static_supervisor.start

  let pool = process.named_subject(name)

  let assert Ok("Success") =
    db_pool.with_connection(pool, 200, 30_000, fn(_conn) { "Success" })
}

pub fn with_connection_current_connection_test() {
  let name = process.new_name("db_pool_test")

  let new_pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let pool_spec = db_pool.supervised(new_pool, name, 200, None)

  let assert Ok(_) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(pool_spec)
    |> static_supervisor.start

  let pool = process.named_subject(name)

  let assert Ok("Success") =
    db_pool.with_connection(pool, 200, 30_000, fn(conn) {
      let assert Ok(value) =
        db_pool.with_connection(pool, 200, 30_000, fn(conn1) {
          assert conn == conn1

          "Success"
        })

      value
    })
}

pub fn with_connection_exhaustion_test() {
  let pool = db_pool()

  // Two callers hold both connections for 200ms
  let holder1 = process.new_subject()
  process.spawn(fn() {
    use conn <- db_pool.with_connection(pool, 200, 30_000)
    process.send(holder1, conn)
    process.sleep(200)
  })

  let holder2 = process.new_subject()
  process.spawn(fn() {
    use conn <- db_pool.with_connection(pool, 200, 30_000)
    process.send(holder2, conn)
    process.sleep(200)
  })

  // Verify both acquired connections
  let assert Ok(Nil) = process.receive(holder1, 500)
  let assert Ok(Nil) = process.receive(holder2, 500)

  let assert Error(db_pool.ConnectionTimeout) =
    db_pool.with_connection(pool, 50, 30_000, fn(_conn) { "Nope" })
}

pub fn checkout_current_connection_test() {
  let name = process.new_name("db_pool_test")

  let new_pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Ok(reference.new()) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(new_pool, name, 200, None)
  let pool = pool.data

  let assert Ok(conn1) = db_pool.checkout(pool, 200, 30_000)

  let assert Ok(conn2) = db_pool.checkout(pool, 200, 30_000)

  assert conn1 == conn2

  // A different process gets a different connection
  let result_subject = process.new_subject()
  process.spawn(fn() {
    let result = db_pool.checkout(pool, 200, 30_000)
    process.send(result_subject, result)
  })

  let assert Ok(Ok(conn3)) = process.receive(result_subject, 500)
  assert conn1 != conn3
}

pub fn checkout_depth_test() {
  let name = process.new_name("db_pool_test")

  let new_pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(reference.new()) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(new_pool, name, 200, None)
  let pool = pool.data

  // Two nested checkouts of the only connection.
  let assert Ok(conn) = db_pool.checkout(pool, 200, 30_000)
  let assert Ok(_) = db_pool.checkout(pool, 200, 30_000)

  // First checkin only decrements depth: the connection is still held,
  // so another process cannot acquire it yet.
  db_pool.checkin(pool, conn)

  let blocked = process.new_subject()
  process.spawn(fn() {
    process.send(blocked, db_pool.checkout(pool, 50, 30_000))
  })
  let assert Ok(Error(db_pool.ConnectionTimeout)) =
    process.receive(blocked, 500)

  // Second checkin releases the connection for real.
  db_pool.checkin(pool, conn)

  let served = process.new_subject()
  process.spawn(fn() {
    process.send(served, db_pool.checkout(pool, 200, 30_000))
  })
  let assert Ok(Ok(_)) = process.receive(served, 500)
}

pub fn clamp_negative_timeout_deadline_test() {
  let name = process.new_name("db_pool_test")

  let new_pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(new_pool, name, 200, None)
  let pool = pool.data

  // Negative timeout/deadline must not crash the pool actor.
  let _ = db_pool.checkout(pool, -5, -5)
  db_pool.checkin(pool, Nil)

  // Pool is still alive and serving.
  let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)
  db_pool.checkin(pool, Nil)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

pub fn clamp_size_and_interval_test() {
  let name = process.new_name("db_pool_test")

  let new_pool =
    db_pool.new()
    |> db_pool.size(0)
    |> db_pool.queue_interval(0)
    |> db_pool.queue_target(-1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(new_pool, name, 200, None)
  let pool = pool.data

  let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)
  db_pool.checkin(pool, Nil)

  process.sleep(20)
  let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)
  db_pool.checkin(pool, Nil)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

/// With `max_idle_connections` below `size`, the pool opens only that many
/// connections eagerly at startup instead of a full `size`.
pub fn max_idle_connections_limits_startup_opens_test() {
  let opens = atomic.new()

  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(5)
    |> db_pool.max_idle_connections(2)
    |> db_pool.on_open(fn() {
      atomic.add(opens, 1)
      Ok(Nil)
    })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  assert atomic.get(opens) == 2

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

/// With `size` above `max_idle_connections`, the pool starts small and opens
/// new connections on demand when concurrent checkouts exceed the idle set,
/// up to `size`.
pub fn pool_grows_on_demand_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(3)
    |> db_pool.max_idle_connections(1)
    |> db_pool.on_open(fn() { Ok(reference.new()) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Three callers check out concurrently. Only one connection exists at
  // startup, so the pool must grow by two to serve them all. The holders
  // report success then hold (without checking in) so all three are held
  // at once; shutdown reclaims the active connections.
  let collector = process.new_subject()
  list.repeat(Nil, 3)
  |> list.each(fn(_) {
    process.spawn(fn() {
      let result = db_pool.checkout(pool, 1000, 30_000)
      process.send(collector, result)
      process.sleep(300)
    })
  })

  let assert Ok(Ok(_)) = process.receive(collector, 2000)
  let assert Ok(Ok(_)) = process.receive(collector, 2000)
  let assert Ok(Ok(_)) = process.receive(collector, 2000)

  let assert Ok(_) = db_pool.shutdown(pool, 500)
}

/// Once a burst subsides, checkins beyond `max_idle_connections` close their
/// connection instead of returning it to the idle set, shrinking the pool.
pub fn checkin_beyond_idle_cap_closes_connection_test() {
  let closes = atomic.new()

  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(3)
    |> db_pool.max_idle_connections(1)
    |> db_pool.on_open(fn() { Ok(reference.new()) })
    |> db_pool.on_close(fn(_) {
      atomic.add(closes, 1)
      Ok(Nil)
    })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Grow to 3 by holding all three connections concurrently, then check them
  // all back in.
  let collector = process.new_subject()
  list.repeat(Nil, 3)
  |> list.each(fn(_) {
    process.spawn(fn() {
      let assert Ok(conn) = db_pool.checkout(pool, 1000, 30_000)
      process.send(collector, Nil)
      process.sleep(150)
      db_pool.checkin(pool, conn)
    })
  })

  let assert Ok(Nil) = process.receive(collector, 2000)
  let assert Ok(Nil) = process.receive(collector, 2000)
  let assert Ok(Nil) = process.receive(collector, 2000)

  // Wait for the three checkins to be processed. With a cap of 1, two of the
  // three returned connections are closed.
  process.sleep(300)
  assert atomic.get(closes) == 2

  let assert Ok(_) = db_pool.shutdown(pool, 500)
}

/// With `max_idle_time` set, connections that sit idle past the limit are
/// closed by the periodic reap sweep and not replaced. The pool regrows on
/// demand afterwards.
pub fn idle_connections_reaped_after_max_idle_time_test() {
  let closes = atomic.new()

  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.max_idle_time(100)
    |> db_pool.on_open(fn() { Ok(reference.new()) })
    |> db_pool.on_close(fn(_) {
      atomic.add(closes, 1)
      Ok(Nil)
    })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Both connections start idle. The reap interval is max(100/2, 1000) = 1000ms;
  // after it fires, both connections (idle far longer than 100ms) are closed.
  process.sleep(1300)
  assert atomic.get(closes) == 2

  // The pool regrows on demand after reaping to zero.
  let assert Ok(conn) = db_pool.checkout(pool, 1000, 30_000)
  db_pool.checkin(pool, conn)

  let assert Ok(_) = db_pool.shutdown(pool, 500)
}

/// A failed open with a waiter still queued is retried on backoff until it
/// succeeds and serves the waiter.
pub fn failed_open_retries_while_waiter_queued_test() {
  let flag =
    table.new()
    |> table.with_access(table.Public)
    |> table.build()

  let assert Ok(Nil) = table.insert(flag, "open", True)

  let name = process.new_name("db_pool_test")
  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() {
      case table.lookup(flag, "open") {
        Ok(True) -> Ok(Nil)
        _ -> Error(Nil)
      }
    })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Caller A holds the only connection.
  let caller_a =
    process.spawn_unlinked(fn() {
      let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)
      process.sleep(30_000)
    })
  process.sleep(20)

  // Caller B waits (pool is full).
  let result = process.new_subject()
  process.spawn(fn() {
    process.send(result, db_pool.checkout(pool, 5000, 30_000))
  })
  process.sleep(20)

  // Break opens, then free A's connection. The demand-driven replacement open
  // fails; because B is still queued the pool schedules a retry.
  let assert Ok(Nil) = table.insert(flag, "open", False)
  process.kill(caller_a)
  process.sleep(50)

  // Repair opens; the retry (first backoff ~0.5-1s) now succeeds and serves B.
  let assert Ok(Nil) = table.insert(flag, "open", True)

  let assert Ok(Ok(Nil)) = process.receive(result, 3000)

  let assert Ok(Nil) = table.drop(flag)
  let assert Ok(_) = db_pool.shutdown(pool, 500)
}

/// A `handle_open` that panics is treated as a failed open: the in-flight
/// count does not leak, so the pool can still open once opens recover and
/// serve the queued waiter.
pub fn opener_panic_does_not_leak_capacity_test() {
  let flag =
    table.new()
    |> table.with_access(table.Public)
    |> table.build()

  let assert Ok(Nil) = table.insert(flag, "mode", "ok")

  let name = process.new_name("db_pool_test")
  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() {
      case table.lookup(flag, "mode") {
        Ok("ok") -> Ok(Nil)
        _ -> panic as "boom"
      }
    })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Caller A holds the only connection.
  let caller_a =
    process.spawn_unlinked(fn() {
      let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)
      process.sleep(30_000)
    })
  process.sleep(20)

  // Caller B waits (pool is full).
  let result = process.new_subject()
  process.spawn(fn() {
    process.send(result, db_pool.checkout(pool, 5000, 30_000))
  })
  process.sleep(20)

  // Make opens panic, then free A's connection: the replacement opener panics
  // without delivering a result. If the pool leaked its in-flight slot it
  // could never open again; instead it retries and, once opens recover,
  // serves B.
  let assert Ok(Nil) = table.insert(flag, "mode", "panic")
  process.kill(caller_a)
  process.sleep(50)

  let assert Ok(Nil) = table.insert(flag, "mode", "ok")

  let assert Ok(Ok(Nil)) = process.receive(result, 3000)

  let assert Ok(Nil) = table.drop(flag)
  let assert Ok(_) = db_pool.shutdown(pool, 500)
}

/// A burst of waiters never drives the live connection count above `size`,
/// even as the pool grows on demand and sheds beyond the idle cap.
pub fn capacity_invariant_under_burst_test() {
  let opens = atomic.new()
  let closes = atomic.new()

  let name = process.new_name("db_pool_test")
  let pool =
    db_pool.new()
    |> db_pool.size(3)
    |> db_pool.max_idle_connections(1)
    |> db_pool.on_open(fn() {
      atomic.add(opens, 1)
      Ok(reference.new())
    })
    |> db_pool.on_close(fn(_) {
      atomic.add(closes, 1)
      Ok(Nil)
    })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  let collector = process.new_subject()
  let count = 20

  list.repeat(Nil, count)
  |> list.each(fn(_) {
    process.spawn(fn() {
      case db_pool.checkout(pool, 2000, 30_000) {
        Ok(conn) -> {
          process.sleep(30)
          db_pool.checkin(pool, conn)
        }
        Error(_) -> Nil
      }
      process.send(collector, Nil)
    })
  })

  // Sample the live count (opened minus closed) repeatedly while the burst is
  // in flight. It must never exceed `size`.
  assert_live_within(opens, closes, 3, 40)

  drain_dones(collector, count)

  let assert Ok(_) = db_pool.shutdown(pool, 500)
}

fn assert_live_within(
  opens: atomic.Atomic,
  closes: atomic.Atomic,
  max: Int,
  samples: Int,
) -> Nil {
  case samples <= 0 {
    True -> Nil
    False -> {
      assert atomic.get(opens) - atomic.get(closes) <= max
      process.sleep(5)
      assert_live_within(opens, closes, max, samples - 1)
    }
  }
}

fn drain_dones(collector: process.Subject(Nil), remaining: Int) -> Nil {
  case remaining <= 0 {
    True -> Nil
    False ->
      case process.receive(collector, 3000) {
        Ok(Nil) -> drain_dones(collector, remaining - 1)
        Error(Nil) -> Nil
      }
  }
}

/// Shutting down while an open is in flight drains it: the connection that
/// arrives after shutdown begins is closed rather than leaked, and the
/// shutdown caller still receives `Ok(Nil)`.
pub fn shutdown_with_pending_open_drains_connection_test() {
  let opens = atomic.new()
  let closes = atomic.new()

  let flag =
    table.new()
    |> table.with_access(table.Public)
    |> table.build()

  let assert Ok(Nil) = table.insert(flag, "slow", False)

  let name = process.new_name("db_pool_test")
  let pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.max_idle_connections(1)
    |> db_pool.on_open(fn() {
      atomic.add(opens, 1)
      case table.lookup(flag, "slow") {
        Ok(True) -> process.sleep(300)
        _ -> Nil
      }
      Ok(reference.new())
    })
    |> db_pool.on_close(fn(_) {
      atomic.add(closes, 1)
      Ok(Nil)
    })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // From now on new opens are slow.
  let assert Ok(Nil) = table.insert(flag, "slow", True)

  // Caller A takes the one idle connection and holds it.
  let ack = process.new_subject()
  process.spawn(fn() {
    let assert Ok(_) = db_pool.checkout(pool, 1000, 30_000)
    process.send(ack, Nil)
    process.sleep(5000)
  })
  let assert Ok(Nil) = process.receive(ack, 1000)

  // Caller B checks out: pool is not at capacity, so an opener is spawned with
  // a slow open still in flight.
  process.spawn(fn() {
    let _ = db_pool.checkout(pool, 2000, 30_000)
    Nil
  })
  process.sleep(50)

  // Shut down while the open is in flight. The pool drains: it stays alive to
  // close the connection the opener delivers, then replies Ok.
  let assert Ok(Nil) = db_pool.shutdown(pool, 1000)

  // Every opened connection has been closed; nothing leaked.
  assert atomic.get(opens) == atomic.get(closes)
  assert atomic.get(opens) == 2

  let assert Ok(Nil) = table.drop(flag)
}

pub fn shutdown_reason_treated_as_normal_test() {
  let name = process.new_name("db_pool_test")

  let new_pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(started) = db_pool.start(new_pool, name, 200, None)
  let pid = started.pid

  process.send_abnormal_exit(pid, atom.create("shutdown"))

  // The pool should terminate.
  wait_for_exit(pid, 500)

  assert !process.is_alive(pid)
}

fn wait_for_exit(pid: process.Pid, remaining: Int) -> Nil {
  use <- bool.guard(when: !process.is_alive(pid) || remaining <= 0, return: Nil)
  process.sleep(10)
  wait_for_exit(pid, remaining - 10)
}

pub fn checkout_checkin_test() {
  let pool = db_pool()

  let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)

  db_pool.checkin(pool, Nil)
}

pub fn checkout_exhaustion_test() {
  let pool = db_pool()

  // Two callers hold both connections for 200ms
  let holder1 = process.new_subject()
  process.spawn(fn() {
    let result = db_pool.checkout(pool, 200, 30_000)
    process.send(holder1, result)
    process.sleep(200)
    db_pool.checkin(pool, Nil)
  })

  let holder2 = process.new_subject()
  process.spawn(fn() {
    let result = db_pool.checkout(pool, 200, 30_000)
    process.send(holder2, result)
    process.sleep(200)
    db_pool.checkin(pool, Nil)
  })

  // Verify both acquired connections
  let assert Ok(Ok(Nil)) = process.receive(holder1, 500)
  let assert Ok(Ok(Nil)) = process.receive(holder2, 500)

  // Third caller should time out -- pool exhausted for another ~150ms
  let result_subject = process.new_subject()
  process.spawn(fn() {
    let result = db_pool.checkout(pool, 50, 30_000)
    process.send(result_subject, result)
  })

  let assert Ok(Error(db_pool.ConnectionTimeout)) =
    process.receive(result_subject, 500)
}

pub fn caller_down_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(int.random(10)) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  let caller =
    process.spawn_unlinked(fn() {
      let assert Ok(_conn) = db_pool.checkout(pool, 100, 30_000)

      process.sleep_forever()
    })

  process.sleep(200)

  process.kill(caller)

  let assert Ok(_conn) = db_pool.checkout(pool, 200, 30_000)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

pub fn waiting_caller_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // First caller holds the connection for 200ms
  process.spawn(fn() {
    let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)
    process.sleep(200)
    db_pool.checkin(pool, Nil)
  })

  // Give time for first caller to acquire
  process.sleep(50)

  // Second caller waits -- should receive the connection after first returns
  let result_subject = process.new_subject()
  process.spawn(fn() {
    let result = db_pool.checkout(pool, 500, 30_000)
    process.send(result_subject, result)
  })

  let assert Ok(Ok(Nil)) = process.receive(result_subject, 1000)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

pub fn waiting_caller_timeout_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 100, None)
  let pool = pool.data

  // First caller holds the connection for 200ms
  process.spawn(fn() {
    let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)

    process.sleep(200)

    db_pool.checkin(pool, Nil)
  })

  // Give time for first caller to acquire
  process.sleep(20)

  // Second caller times out after 50ms — connection won't be back for ~180ms
  let result_subject = process.new_subject()
  process.spawn(fn() {
    let result = db_pool.checkout(pool, 50, 30_000)
    process.send(result_subject, result)
  })

  let assert Ok(Error(db_pool.ConnectionTimeout)) =
    process.receive(result_subject, 500)

  process.sleep(250)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

pub fn pool_exit_test() {
  let name = process.new_name("db_pool_test")

  let db_pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(db_pool, name, 200, None)
  let pool = pool.data

  let assert Ok(pid) = process.subject_owner(pool)

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
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // First caller checks out with a 50ms deadline, then holds it forever
  process.spawn_unlinked(fn() {
    let assert Ok(_conn) = db_pool.checkout(pool, 200, 50)
    // Hold the connection indefinitely (deadline should fire after 50ms)
    process.sleep_forever()
  })

  // Wait for the deadline to fire and the replacement connection to be opened
  process.sleep(200)

  // Second caller should be able to check out successfully because the pool
  // replaced the deadline-expired connection
  let assert Ok(_conn) = db_pool.checkout(pool, 200, 30_000)

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
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Checkout with a 100ms deadline
  let assert Ok(conn) = db_pool.checkout(pool, 200, 100)

  // Return the connection well before the deadline
  db_pool.checkin(pool, conn)

  // Sleep past the deadline period
  process.sleep(200)

  // Pool should still be fully operational -- checkout again
  let assert Ok(_conn2) = db_pool.checkout(pool, 200, 30_000)

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
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // First caller takes the only connection with a 100ms deadline, holds forever
  process.spawn_unlinked(fn() {
    let assert Ok(_conn) = db_pool.checkout(pool, 200, 100)
    process.sleep_forever()
  })

  // Give time for the first checkout to complete
  process.sleep(50)

  // Second caller tries to checkout -- will wait because pool is exhausted.
  // When the deadline fires after 100ms, the replacement should serve this waiter.
  let result_subject = process.new_subject()
  process.spawn(fn() {
    let result = db_pool.checkout(pool, 500, 30_000)
    process.send(result_subject, result)
  })

  let assert Ok(Ok(Nil)) = process.receive(result_subject, 1000)

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
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Caller A takes the only connection
  let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)

  // Caller B enqueues as a waiter, then dies
  let waiter_b =
    process.spawn_unlinked(fn() {
      let _result = db_pool.checkout(pool, 5000, 30_000)
      Nil
    })

  process.sleep(50)

  process.kill(waiter_b)
  process.sleep(50)

  // Caller C enqueues as a waiter
  let result_subject = process.new_subject()
  process.spawn(fn() {
    let result = db_pool.checkout(pool, 5000, 30_000)
    process.send(result_subject, result)
  })

  // Give time for C to enqueue
  process.sleep(50)

  // A returns the connection -- should skip dead B and serve C
  db_pool.checkin(pool, Nil)

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
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Caller A takes the only connection
  let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)

  // Caller B enqueues as a waiter, then dies
  let waiter_b =
    process.spawn_unlinked(fn() {
      let _result = db_pool.checkout(pool, 5000, 30_000)
      Nil
    })

  process.sleep(50)

  process.kill(waiter_b)
  process.sleep(50)

  // A returns the connection -- should skip dead B and return conn to idle
  db_pool.checkin(pool, Nil)

  process.sleep(50)

  // A new caller should be able to checkout immediately (conn is idle)
  let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

/// When the pool is overloaded and CoDel enters slow mode, waiters that
/// have been in the queue longer than queue_target * 2 are dropped with
/// ConnectionUnavailable.
pub fn codel_drops_slow_waiters_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.queue_target(1)
    |> db_pool.queue_interval(50)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Exhaust the only connection
  let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)

  // Spawn 5 waiters that will be queued
  let collector = process.new_subject()
  list.repeat(Nil, 5)
  |> list.each(fn(_) {
    process.spawn(fn() {
      let result = db_pool.checkout(pool, 5000, 30_000)
      process.send(collector, result)
    })
  })

  // Wait for CoDel to detect overload:
  // - queue_interval=50ms: first poll fires at 50ms, sees delay > 1ms (target),
  //   enters slow mode and drops waiters older than 2ms (target * 2)
  process.sleep(150)

  // Return the connection — may serve one surviving waiter via codel_dequeue
  db_pool.checkin(pool, Nil)

  // Give time for the served waiter to complete
  process.sleep(100)

  // Collect all results
  let results = collect_results(collector, [])

  // At least one waiter should have been dropped with ConnectionUnavailable
  let dropped =
    list.filter(results, fn(r) { r == Error(db_pool.ConnectionUnavailable) })
  assert list.length(dropped) >= 1

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

/// The CoDel poll loop alone (no checkin happening) drops slow waiters once
/// the queue delay stays above target for a measurement interval. This
/// exercises the poll path specifically. The connection is never returned,
/// so the only mechanism that can drop waiters is the periodic Poll.
pub fn codel_poll_drops_slow_waiters_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.queue_target(1)
    |> db_pool.queue_interval(50)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Exhaust the only connection and never return it.
  let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)

  // Spawn 5 waiters that will queue and never be checked in.
  let collector = process.new_subject()

  list.repeat(Nil, 5)
  |> list.each(fn(_) {
    process.spawn(fn() {
      let result = db_pool.checkout(pool, 5000, 30_000)
      process.send(collector, result)
    })
  })

  // Let several poll intervals elapse so CoDel enters slow mode and the poll
  // loop drops the stale waiters. All without any checkin occurring.
  process.sleep(250)

  let results = collect_results(collector, [])

  let dropped =
    list.filter(results, fn(r) { r == Error(db_pool.ConnectionUnavailable) })

  assert list.length(dropped) == 5

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

/// In fast mode (delay < queue_target), waiters are served immediately
/// without being dropped.
pub fn codel_fast_mode_serves_immediately_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.queue_target(5000)
    |> db_pool.queue_interval(5000)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Exhaust the connection briefly
  let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)

  // Spawn a waiter
  let collector = process.new_subject()
  process.spawn(fn() {
    let result = db_pool.checkout(pool, 2000, 30_000)
    process.send(collector, result)
  })

  // Let the waiter enqueue
  process.sleep(20)

  // Return the connection quickly — delay will be well under queue_target
  db_pool.checkin(pool, Nil)

  // Waiter should be served, not dropped
  let assert Ok(Ok(Nil)) = process.receive(collector, 500)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

pub fn reconnect_after_failed_replacement_test() {
  // Shared flag: when True, handle_open succeeds; when False, it fails.
  let flag =
    table.new()
    |> table.with_access(table.Public)
    |> table.build()

  let assert Ok(Nil) = table.insert(flag, "open", True)

  let name = process.new_name("db_pool_test")
  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() {
      case table.lookup(flag, "open") {
        Ok(True) -> Ok(Nil)
        _ -> Error(Nil)
      }
    })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Check out the only connection
  let caller =
    process.spawn_unlinked(fn() {
      let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)
      // Hold the connection until killed
      process.sleep(30_000)
    })

  // Wait for checkout to complete
  process.sleep(20)

  // Disable handle_open so replacement fails, then kill the caller
  let assert Ok(Nil) = table.insert(flag, "open", False)
  process.kill(caller)

  // Wait for the pool to attempt replacement (and fail)
  process.sleep(50)

  // Pool has 0 usable connections now. Re-enable handle_open so
  // the reconnect backoff timer succeeds on the next attempt.
  let assert Ok(Nil) = table.insert(flag, "open", True)

  // The reconnect timer fires at ~500-1000ms (first backoff).
  // Wait for it, then verify checkout works again.
  let assert Ok(Nil) = db_pool.checkout(pool, 2000, 30_000)
  db_pool.checkin(pool, Nil)

  let assert Ok(Nil) = table.drop(flag)
  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

/// The pool never opens more connections than `max_size`, even across
/// repeated reconnects from caller crashes. We track live connections
/// and assert it stays within capacity.
pub fn reconnect_respects_max_size_test() {
  let opens = atomic.new()
  let closes = atomic.new()

  let name = process.new_name("db_pool_test")
  let pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() {
      atomic.add(opens, 1)
      Ok(Nil)
    })
    |> db_pool.on_close(fn(_) {
      atomic.add(closes, 1)
      Ok(Nil)
    })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Repeatedly check out and crash the holder so the pool reconnects.
  list.repeat(Nil, 6)
  |> list.each(fn(_) {
    let caller =
      process.spawn_unlinked(fn() {
        let assert Ok(Nil) = db_pool.checkout(pool, 500, 30_000)
        process.sleep(30_000)
      })
    process.sleep(20)
    process.kill(caller)
    process.sleep(20)

    // Live connections must never exceed the pool's max_size.
    let live = atomic.get(opens) - atomic.get(closes)
    assert live <= 2
  })

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

/// When the pool shuts down while callers are waiting in the queue,
/// those callers receive ConnectionUnavailable instead of blocking forever.
pub fn shutdown_drains_waiters_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  // Take the only connection so subsequent checkouts must wait
  let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)

  // Spawn a waiter that will be queued
  let result_subject = process.new_subject()
  process.spawn(fn() {
    let result = db_pool.checkout(pool, 5000, 30_000)
    process.send(result_subject, result)
  })

  // Give time for the waiter to enqueue
  process.sleep(50)

  let assert Ok(_) = db_pool.shutdown(pool, 200)

  // The waiting caller should have received ConnectionUnavailable
  let assert Ok(Error(db_pool.ConnectionUnavailable)) =
    process.receive(result_subject, 500)
}

pub fn on_close_called_on_shutdown_test() {
  let close_count = atomic.new()

  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(3)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) {
      atomic.add(close_count, 1)
      Ok(Nil)
    })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  let assert Ok(_) = db_pool.shutdown(pool, 200)

  assert atomic.get(close_count) == 3
}

pub fn shutdown_closes_active_connections_test() {
  let close_count = atomic.new()

  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) {
      atomic.add(close_count, 1)
      Ok(Nil)
    })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  let assert Ok(Nil) = db_pool.checkout(pool, 200, 30_000)

  let assert Ok(_) = db_pool.shutdown(pool, 200)

  assert atomic.get(close_count) == 2
}

pub fn on_idle_and_on_active_called_at_checkin_and_checkout_test() {
  let idle_count = atomic.new()
  let active_count = atomic.new()

  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { atomic.add(idle_count, 1) })
    |> db_pool.on_active(fn(_) { atomic.add(active_count, 1) })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  let assert Ok(_conn) = db_pool.checkout(pool, 200, 30_000)

  db_pool.checkin(pool, Nil)

  // must sleep to give time for checkin to be processed
  process.sleep(150)

  // idle count is pool size + 1 from calling handle_idle on initial
  // creation, and on checkin after checkout
  assert 3 == atomic.get(idle_count)
  // active count is only the number of times checkout was called
  assert 1 == atomic.get(active_count)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

pub fn checkin_by_non_active_caller_ignored_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(reference.new()) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  let assert Ok(conn) = db_pool.checkout(pool, 200, 30_000)

  let done = process.new_subject()
  process.spawn(fn() {
    db_pool.checkin(pool, conn)
    process.send(done, Nil)
  })
  let assert Ok(Nil) = process.receive(done, 500)

  process.sleep(50)

  let assert Ok(conn2) = db_pool.checkout(pool, 200, 30_000)
  assert conn == conn2

  db_pool.checkin(pool, conn)
  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

pub fn pool_exit_abnormal_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(2)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200, None)
  let pool = pool.data

  let assert Ok(pid) = process.subject_owner(pool)

  process.send_abnormal_exit(pid, "test crash")

  process.sleep(50)

  assert process.is_alive(pid) == False
}

// Many callers enqueued in rapid succession used to be able to collide on the
// queue's nanosecond timestamp key, making `queue.push` return Error and
// crashing the whole pool actor via a `let assert`.
// The queue now uses a strictly-unique key, so the pool must survive a burst
// of waiters and eventually serve every one of them.
pub fn many_waiters_enqueued_without_collision_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_idle(fn(_) { Nil })
    |> db_pool.on_active(fn(_) { Nil })

  let assert Ok(started) = db_pool.start(pool, name, 200, None)
  let pool = started.data
  let pool_pid = started.pid

  // Hold the single connection so every other caller must enqueue, then
  // release it after a short delay so the queued waiters can be served.
  process.spawn(fn() {
    let assert Ok(Nil) = db_pool.checkout(pool, 1000, 30_000)
    process.sleep(150)
    db_pool.checkin(pool, Nil)
  })

  process.sleep(50)

  // Flood the pool with waiters with no sleeps between spawns, so their
  // enqueue messages are processed back-to-back by the actor.
  let results = process.new_subject()
  let count = 50

  list.repeat(Nil, count)
  |> list.each(fn(_) {
    process.spawn(fn() {
      let res = db_pool.checkout(pool, 2000, 30_000)
      case res {
        Ok(Nil) -> db_pool.checkin(pool, Nil)
        Error(_) -> Nil
      }
      process.send(results, res)
    })
  })

  // Let all waiters enqueue while the holder still owns the connection.
  process.sleep(50)
  assert process.is_alive(pool_pid) == True

  // Every waiter should eventually get served without the pool crashing.
  let collected = collect_all(results, [], count)

  assert list.length(collected) == count
  assert list.all(collected, fn(r) { r == Ok(Nil) })
  assert process.is_alive(pool_pid) == True

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}

fn collect_all(
  collector: process.Subject(Result(Nil, db_pool.PoolError(err))),
  acc: List(Result(Nil, db_pool.PoolError(err))),
  remaining: Int,
) -> List(Result(Nil, db_pool.PoolError(err))) {
  case remaining {
    0 -> acc
    _ ->
      case process.receive(collector, 2000) {
        Ok(result) -> collect_all(collector, [result, ..acc], remaining - 1)
        Error(Nil) -> acc
      }
  }
}

fn collect_results(
  collector: process.Subject(Result(Nil, db_pool.PoolError(err))),
  acc: List(Result(Nil, db_pool.PoolError(err))),
) -> List(Result(Nil, db_pool.PoolError(err))) {
  case process.receive(collector, 0) {
    Ok(result) -> collect_results(collector, [result, ..acc])
    Error(Nil) -> acc
  }
}

fn db_pool() -> process.Subject(db_pool.Message(Nil, err)) {
  global_value.create_with_unique_name("db_pool_test", fn() {
    let name = process.new_name("db_pool_test")

    let db_pool =
      db_pool.new()
      |> db_pool.size(2)
      |> db_pool.on_open(fn() { Ok(Nil) })
      |> db_pool.on_close(fn(_) { Ok(Nil) })
      |> db_pool.on_idle(fn(_) { Nil })
      |> db_pool.on_active(fn(_) { Nil })

    let assert Ok(pool) = db_pool.start(db_pool, name, 200, None)

    pool.data
  })
}
