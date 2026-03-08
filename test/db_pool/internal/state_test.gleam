import db_pool/internal/state
import gleam/erlang/process
import gleam/erlang/reference
import gleam/option.{None, Some}
import gleam/result
import rasa/counter

pub fn current_connection_error_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  let self = process.self()

  let assert Error(Nil) = state.current_connection(state, self)
}

pub fn enqueue_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  let self = process.self()
  let subject = process.new_subject()

  assert 0 == state.queue_size(state)

  let state =
    state
    |> state.enqueue(self, subject, 100, fn(_, _) { "Timeout!" }, fn(_) { "" })

  use selector <- state.with_selector(state)

  let assert Ok("Timeout!") = process.selector_receive(selector, 150)

  assert 1 == state.queue_size(state)
}

pub fn checkout_test() {
  let assert Ok(state) =
    state.new()
    |> state.max_size(1)
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  assert 0 == state.active_size(state)

  let self = process.self()

  let assert Error(Nil) = state.current_connection(state, self)

  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, fn(_conn) { Nil })

  let assert Ok(_conn) = state.current_connection(state, self)

  assert 1 == state.active_size(state)
}

pub fn dequeue_test() {
  let self = process.self()
  let conn = reference.new()

  // Initialize new state
  let assert Ok(state) =
    state.new()
    |> state.max_size(1)
    |> state.on_open(fn() { Ok(conn) })
    |> state.build(process.new_selector())

  assert 1 == state.current_size(state)

  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, fn(_) { Nil })

  assert 0 == state.queue_size(state)

  let subject = process.new_subject()

  // Enqueue a waiting process
  let state =
    state
    |> state.enqueue(self, subject, 1000, fn(_, _) { Nil }, fn(_) { Nil })

  assert 1 == state.queue_size(state)

  let state =
    state
    |> state.dequeue(
      Some(conn),
      self,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
    )

  // Ensure previously queued process removed from queue
  assert 0 == state.queue_size(state)
}

pub fn dequeue_without_connection_test() {
  let self = process.self()

  // Initialize new state
  let assert Ok(state) =
    state.new()
    |> state.max_size(1)
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, fn(_) { Nil })

  assert 0 == state.queue_size(state)

  let subject = process.new_subject()

  // Enqueue a waiting process
  let state =
    state
    |> state.enqueue(self, subject, 5000, fn(_, _) { Nil }, fn(_) { Nil })

  assert 1 == state.queue_size(state)

  let state =
    state
    |> state.dequeue(
      None,
      self,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
    )

  // Ensure queued process has been removed from the queue
  assert 0 == state.queue_size(state)
}

pub fn expire_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  let assert Ok(now_in_ms) =
    counter.Millisecond
    |> counter.monotonic
    |> counter.next

  assert 0 == state.queue_size(state)

  let state =
    state
    |> state.enqueue(
      process.self(),
      process.new_subject(),
      5000,
      fn(_, _) { Nil },
      fn(_) { Nil },
    )

  // Enqueue waiting process
  assert 1 == state.queue_size(state)

  let extend = fn(_, _) { panic as "should not be called" }
  let on_expiry = fn(_) { Nil }

  state
  |> state.expire(now_in_ms, -100, on_expiry:, or_else: extend)

  // Ensure queued process has been removed from the queue
  assert 0 == state.queue_size(state)
}

pub fn expire_retry_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  let assert Ok(now_in_ms) =
    counter.Millisecond
    |> counter.monotonic
    |> counter.next

  assert 0 == state.queue_size(state)

  let state =
    state
    |> state.enqueue(
      process.self(),
      process.new_subject(),
      5000,
      fn(_, _) { "" },
      fn(_) { "" },
    )

  assert 1 == state.queue_size(state)

  let extend = fn(_, _) { "Extend!" }
  let on_expiry = fn(_) { panic as "Should not be called" }

  let state =
    state
    |> state.expire(now_in_ms, 100, on_expiry:, or_else: extend)

  // Ensure queued process is still in the queue
  assert 1 == state.queue_size(state)

  use selector <- state.with_selector(state)

  let assert Ok("Extend!") = process.selector_receive(selector, 150)
}

pub fn ping_test() {
  let selector = process.new_selector()

  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.interval(10)
    |> state.build(selector)
    |> result.map(state.ping(_, "Ping!"))

  use selector <- state.with_selector(state)

  let assert Ok("Ping!") = process.selector_receive(selector, 100)
}

pub fn shutdown_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  assert Nil == state.shutdown(state)
}

pub fn close_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  assert Nil == state.close(state)
}

// When dequeue runs in fast mode (slow=False, mid-interval), the first
// waiter should be served immediately.
pub fn dequeue_fast_serves_waiter_test() {
  let self = process.self()
  let conn = reference.new()

  // Use a large queue_interval so we never cross an interval boundary
  let assert Ok(state) =
    state.new()
    |> state.max_size(1)
    |> state.on_open(fn() { Ok(conn) })
    |> state.queue_target(50)
    |> state.queue_interval(999_999)
    |> state.build(process.new_selector())

  // Pool starts with slow=False
  assert False == state.is_slow(state)

  // Check out the only connection
  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, fn(_) { Nil })

  // Enqueue a waiter
  let waiter_subject = process.new_subject()
  let state =
    state
    |> state.enqueue(self, waiter_subject, 5000, fn(_, _) { Nil }, fn(_) { Nil })

  assert 1 == state.queue_size(state)

  // Dequeue (checkin), should serve the waiter in fast mode
  let state =
    state
    |> state.dequeue(
      Some(conn),
      self,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { panic as "Should not drop in fast mode" },
    )

  assert 0 == state.queue_size(state)
  assert False == state.is_slow(state)
}

/// When dequeue hits an interval boundary and the previous interval's
/// delay > target, the pool enters slow mode on the next interval boundary.
/// CoDel requires observing one full interval of high delay before acting.
pub fn dequeue_first_enters_slow_mode_test() {
  let self = process.self()
  let conn = reference.new()

  // tiny target (1ms) and tiny interval (1ms) to enter slow mode quickly
  let assert Ok(state) =
    state.new()
    |> state.max_size(1)
    |> state.on_open(fn() { Ok(conn) })
    |> state.queue_target(1)
    |> state.queue_interval(1)
    |> state.build(process.new_selector())

  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, fn(_) { Nil })

  // Enqueue waiter 1
  let waiter1 = process.new_subject()
  let state =
    state
    |> state.enqueue(self, waiter1, 5000, fn(_, _) { Nil }, fn(_) { Nil })

  assert 1 == state.queue_size(state)
  assert False == state.is_slow(state)

  // Sleep to cross the interval boundary and accumulate delay > target
  process.sleep(10)

  // First dequeue. Crosses interval boundary, measures delay > target,
  // but slow is set based on the previous interval's delay (which was 0).
  // So slow stays False, but delay is now recorded as high.
  let state =
    state
    |> state.dequeue(
      Some(conn),
      self,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
    )

  assert 0 == state.queue_size(state)
  assert False == state.is_slow(state)

  assert state.codel_delay(state) > 1

  // Second checkout
  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, fn(_) { Nil })

  let waiter2 = process.new_subject()
  let state =
    state
    |> state.enqueue(self, waiter2, 5000, fn(_, _) { Nil }, fn(_) { Nil })

  // Sleep past the next interval boundary
  process.sleep(10)

  // Second dequeue. state.delay (from first cycle) > target,
  // so dequeue_first sets slow=True
  let state =
    state
    |> state.dequeue(
      Some(conn),
      self,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
    )

  assert 0 == state.queue_size(state)
  assert True == state.is_slow(state)
}

/// When dequeue hits an interval boundary and delay <= target,
/// the pool should exit slow mode (or stay in fast mode).
pub fn dequeue_first_stays_fast_when_delay_ok_test() {
  let self = process.self()
  let conn = reference.new()

  // Use a tiny queue_interval (1ms) but a very large target (999999ms)
  // so delay will never exceed target
  let assert Ok(state) =
    state.new()
    |> state.max_size(1)
    |> state.on_open(fn() { Ok(conn) })
    |> state.queue_target(999_999)
    |> state.queue_interval(1)
    |> state.build(process.new_selector())

  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, fn(_) { Nil })

  let waiter = process.new_subject()
  let state =
    state
    |> state.enqueue(self, waiter, 5000, fn(_, _) { Nil }, fn(_) { Nil })

  // Sleep to cross the interval boundary
  process.sleep(5)

  let state =
    state
    |> state.dequeue(
      Some(conn),
      self,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
    )

  assert 0 == state.queue_size(state)
  // delay <= target so pool stays fast
  assert False == state.is_slow(state)
}

/// In slow mode, waiters that have been waiting longer than target * 2
/// should be dropped, and the on_drop callback should be called.
pub fn dequeue_slow_drops_stale_waiters_test() {
  let self = process.self()
  let conn = reference.new()

  // target=1ms so target*2=2ms drop threshold.
  // interval=10ms so we can control when we cross boundaries.
  let assert Ok(state) =
    state.new()
    |> state.max_size(1)
    |> state.on_open(fn() { Ok(conn) })
    |> state.queue_target(1)
    |> state.queue_interval(10)
    |> state.build(process.new_selector())

  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, fn(_) { Nil })

  let waiter1 = process.new_subject()
  let state =
    state
    |> state.enqueue(self, waiter1, 5000, fn(_, _) { Nil }, fn(_) { Nil })

  // Sleep past the first interval boundary (10ms) and accumulate delay
  process.sleep(20)

  // crosses interval boundary, records high delay, stays fast
  let state =
    state
    |> state.dequeue(
      Some(conn),
      self,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
    )

  assert False == state.is_slow(state)
  assert state.codel_delay(state) > 1

  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, fn(_) { Nil })

  let waiter2 = process.new_subject()
  let state =
    state
    |> state.enqueue(self, waiter2, 5000, fn(_, _) { Nil }, fn(_) { Nil })

  // Sleep past the second interval boundary
  process.sleep(20)

  let state =
    state
    |> state.dequeue(
      Some(conn),
      self,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
    )

  assert True == state.is_slow(state)

  // now in slow mode, do not cross interval boundary
  // We need to stay within the current interval so dequeue_slow runs.
  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, fn(_) { Nil })

  // Enqueue a waiter that will become stale
  let stale = process.new_subject()
  let state =
    state
    |> state.enqueue(self, stale, 5000, fn(_, _) { Nil }, fn(_) { Nil })

  // Sleep just enough for the stale waiter to exceed target*2=2ms
  // but not enough to cross the 10ms interval boundary
  process.sleep(5)

  // Enqueue a fresh waiter
  let fresh = process.new_subject()
  let state =
    state
    |> state.enqueue(self, fresh, 5000, fn(_, _) { Nil }, fn(_) { Nil })

  assert 2 == state.queue_size(state)

  let drop_count = process.new_subject()

  // Dequeue in slow mode, mid-interval. Should drop the stale waiter
  // and serve the fresh one
  let state =
    state
    |> state.dequeue(
      Some(conn),
      self,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { process.send(drop_count, 1) },
    )

  // Stale waiter was dropped, fresh waiter was served
  assert 0 == state.queue_size(state)

  // Verify the drop callback was called
  let assert Ok(1) = process.receive(drop_count, 0)
}

// The poll function should schedule the next poll timer and return
// updated state.
pub fn poll_schedules_next_poll_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.queue_interval(50)
    |> state.build(process.new_selector())

  let assert Ok(now) =
    counter.Millisecond
    |> counter.monotonic
    |> counter.next

  // Poll with no queue entries should just schedule next poll
  let state =
    state.poll(
      state,
      now,
      now,
      on_poll: fn(time, last_sent) { #("Poll", time, last_sent) },
      on_drop: fn(_) { Nil },
    )

  use selector <- state.with_selector(state)

  // The poll timer should fire within queue_interval + buffer
  let assert Ok(#("Poll", _, _)) = process.selector_receive(selector, 150)
}

/// Poll should trigger CoDel slow mode when the queue has stalled
/// and delay exceeds the target.
pub fn poll_enters_slow_on_stall_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.queue_target(1)
    |> state.queue_interval(1)
    |> state.build(process.new_selector())

  assert False == state.is_slow(state)

  // Enqueue a waiter
  let state =
    state
    |> state.enqueue(
      process.self(),
      process.new_subject(),
      5000,
      fn(_, _) { Nil },
      fn(_) { Nil },
    )

  assert 1 == state.queue_size(state)

  // Sleep past the interval boundary + enough for delay > target
  process.sleep(10)

  let assert Ok(now) =
    counter.Millisecond
    |> counter.monotonic
    |> counter.next

  let state =
    state.poll(state, now, 0, on_poll: fn(_, _) { Nil }, on_drop: fn(_) { Nil })

  // We need to trigger a second interval boundary.

  process.sleep(5)

  let assert Ok(now2) =
    counter.Millisecond
    |> counter.monotonic
    |> counter.next

  let state =
    state.poll(state, now2, 0, on_poll: fn(_, _) { Nil }, on_drop: fn(_) { Nil })

  assert True == state.is_slow(state)
}

/// When dequeue returns a connection to idle, the connection
/// should be available for the next checkout.
pub fn dequeue_returns_to_idle_when_no_waiters_test() {
  let self = process.self()
  let conn = reference.new()

  let assert Ok(state) =
    state.new()
    |> state.max_size(1)
    |> state.on_open(fn() { Ok(conn) })
    |> state.queue_target(50)
    |> state.queue_interval(999_999)
    |> state.build(process.new_selector())

  // Checkout
  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, fn(_) { Nil })
  assert 1 == state.active_size(state)

  // Dequeue with no waiters -- should return conn to idle
  let state =
    state
    |> state.dequeue(
      Some(conn),
      self,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
    )

  assert 0 == state.active_size(state)
  assert 0 == state.queue_size(state)

  // Connection should be available again
  let assert Ok(_state) =
    state.checkout(state, self, fn(_) { Nil }, fn(_) { Nil })
}
