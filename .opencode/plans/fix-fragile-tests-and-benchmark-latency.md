# Fix fragile tests (#21) and benchmark latency (#22)

## #21a: checkout_current_connection_test (line 77)

**Problem:** Spawned process asserts `conn1 != conn3` silently at line 102 -- failure is invisible to test runner.

**Fix:** Send checkout result back via `result_subject`, assert in test process:

```gleam
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

  // Re-entrant checkout returns the same connection
  let assert Ok(conn2) = db_pool.checkout(pool, self, 200, 30_000)

  assert conn1 == conn2

  // A different process gets a different connection
  let result_subject = process.new_subject()
  process.spawn(fn() {
    let self = process.self()
    let result = db_pool.checkout(pool, self, 200, 30_000)
    process.send(result_subject, result)
  })

  let assert Ok(Ok(conn3)) = process.receive(result_subject, 500)
  assert conn1 != conn3
}
```

## #21b: checkout_exhaustion_test (line 116)

**Problem:** All three spawned processes assert silently. Third process uses `sleep(50)` + 20ms timeout race. On slow CI, scheduling jitter could make this flaky.

**Fix:** Increase timing gaps. Use result_subject for the critical assertion (third caller timeout). First two callers hold for 200ms; third caller waits 50ms then times out after 50ms.

```gleam
pub fn checkout_exhaustion_test() {
  let pool = db_pool()

  // Two callers hold both connections for 200ms
  process.spawn(fn() {
    let self = process.self()
    let assert Ok(Nil) = db_pool.checkout(pool, self, 200, 30_000)
    process.sleep(200)
    db_pool.checkin(pool, Nil, self)
  })

  process.spawn(fn() {
    let self = process.self()
    let assert Ok(Nil) = db_pool.checkout(pool, self, 200, 30_000)
    process.sleep(200)
    db_pool.checkin(pool, Nil, self)
  })

  // Give time for both to acquire
  process.sleep(50)

  // Third caller should time out -- pool exhausted for another ~150ms
  let result_subject = process.new_subject()
  process.spawn(fn() {
    let self = process.self()
    let result = db_pool.checkout(pool, self, 50, 30_000)
    process.send(result_subject, result)
  })

  let assert Ok(Error(db_pool.ConnectionTimeout)) =
    process.receive(result_subject, 500)
}
```

## #21c: waiting_caller_test (line 179)

**Problem:** Both spawned processes assert silently. First caller checkout at 50ms timeout is tight. Second caller's success is never verified by test process.

**Fix:** Increase hold time. Verify second caller's result via subject.

```gleam
pub fn waiting_caller_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200)

  // First caller holds the connection for 200ms
  process.spawn(fn() {
    let self = process.self()
    let assert Ok(Nil) = db_pool.checkout(pool, self, 200, 30_000)
    process.sleep(200)
    db_pool.checkin(pool, Nil, self)
  })

  // Give time for first caller to acquire
  process.sleep(50)

  // Second caller waits -- should receive the connection after first returns
  let result_subject = process.new_subject()
  process.spawn(fn() {
    let self = process.self()
    let result = db_pool.checkout(pool, self, 500, 30_000)
    process.send(result_subject, result)
  })

  let assert Ok(Ok(Nil)) = process.receive(result_subject, 1000)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}
```

## #21d: deadline_expires_serves_waiting_caller_test (line 341)

**Problem:** `sleep(10)` for sequencing is fragile. Second caller asserts inside spawned process.

**Fix:** Increase sequencing sleep to 50ms. Verify second caller's result via subject.

```gleam
pub fn deadline_expires_serves_waiting_caller_test() {
  let name = process.new_name("db_pool_test")

  let pool =
    db_pool.new()
    |> db_pool.size(1)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 200)

  // First caller takes the only connection with a 100ms deadline, holds forever
  process.spawn_unlinked(fn() {
    let self = process.self()
    let assert Ok(_conn) = db_pool.checkout(pool, self, 200, 100)
    process.sleep_forever()
  })

  // Give time for the first checkout to complete
  process.sleep(50)

  // Second caller tries to checkout -- will wait because pool is exhausted.
  // When the deadline fires after 100ms, the replacement should serve this waiter.
  let result_subject = process.new_subject()
  process.spawn(fn() {
    let self = process.self()
    let result = db_pool.checkout(pool, self, 500, 30_000)
    process.send(result_subject, result)
  })

  let assert Ok(Ok(Nil)) = process.receive(result_subject, 1000)

  let assert Ok(_) = db_pool.shutdown(pool, 200)
}
```

## #22: Benchmark latency measures only checkout wait time

**Problem:** `t1 - now` at line 204 includes checkout wait + hold_ms sleep + checkin. Reported latency conflates pool overhead with simulated work.

**Fix:** Take timestamp right after checkout succeeds:

```gleam
case result {
  Ok(conn) -> {
    let checkout_end = counter.next(timer)

    // Simulate work
    case scenario.hold_ms > 0 {
      True -> process.sleep(scenario.hold_ms)
      False -> Nil
    }
    db_pool.checkin(pool, conn, self)

    process.send(collector, Sample(latency_us: checkout_end - now, ok: True))
  }
  Error(_) -> {
    let t1 = counter.next(timer)
    process.send(collector, Sample(latency_us: t1 - now, ok: False))
  }
}
```

Error case unchanged (measures just the failed checkout wait).
