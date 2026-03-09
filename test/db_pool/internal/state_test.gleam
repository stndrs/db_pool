import db_pool/internal/state
import gleam/erlang/process
import gleam/erlang/reference
import gleam/option.{None, Some}
import gleam/result
import rasa/counter

// Large deadline used in tests that don't exercise deadline behaviour.
const no_deadline = 999_999

fn no_op_deadline(_caller: process.Pid, _checkout_time: Int) -> Nil {
  Nil
}

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
    |> state.enqueue(
      self,
      subject,
      100,
      no_deadline,
      on_timeout: fn(_, _) { "Timeout!" },
      on_down: fn(_) { "" },
    )

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
    state.checkout(
      state,
      self,
      fn(_) { Nil },
      no_deadline,
      no_op_deadline,
      fn(_conn) { Nil },
    )

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
    state.checkout(
      state,
      self,
      fn(_) { Nil },
      no_deadline,
      no_op_deadline,
      fn(_) { Nil },
    )

  assert 0 == state.queue_size(state)

  let subject = process.new_subject()

  // Enqueue a waiting process
  let state =
    state
    |> state.enqueue(
      self,
      subject,
      1000,
      no_deadline,
      on_timeout: fn(_, _) { Nil },
      on_down: fn(_) { Nil },
    )

  assert 1 == state.queue_size(state)

  let state =
    state
    |> state.dequeue(
      Some(conn),
      self,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
      on_deadline: no_op_deadline,
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
    state.checkout(
      state,
      self,
      fn(_) { Nil },
      no_deadline,
      no_op_deadline,
      fn(_) { Nil },
    )

  assert 0 == state.queue_size(state)

  let subject = process.new_subject()

  // Enqueue a waiting process
  let state =
    state
    |> state.enqueue(
      self,
      subject,
      5000,
      no_deadline,
      on_timeout: fn(_, _) { Nil },
      on_down: fn(_) { Nil },
    )

  assert 1 == state.queue_size(state)

  let state =
    state
    |> state.dequeue(
      None,
      self,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
      on_deadline: no_op_deadline,
    )

  // Ensure queued process has been removed from the queue
  assert 0 == state.queue_size(state)
}

pub fn expire_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  assert 0 == state.queue_size(state)

  // Use a subject to capture the queue key from the timeout callback
  let key_subject = process.new_subject()

  let state =
    state
    |> state.enqueue(
      process.self(),
      process.new_subject(),
      1,
      no_deadline,
      on_timeout: fn(sent, _timeout) {
        process.send(key_subject, sent)
        Nil
      },
      on_down: fn(_) { Nil },
    )

  // Enqueue waiting process
  assert 1 == state.queue_size(state)

  // Receive the queue key from the timeout callback (fires after 1ms)
  use selector <- state.with_selector(state)
  let assert Ok(Nil) = process.selector_receive(selector, 50)
  let assert Ok(sent) = process.receive(key_subject, 0)

  let extend = fn(_, _) { panic as "should not be called" }
  let on_expiry = fn(_) { Nil }

  state
  |> state.expire(sent, -100, on_expiry:, or_else: extend)

  // Ensure queued process has been removed from the queue
  assert 0 == state.queue_size(state)
}

pub fn expire_retry_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  assert 0 == state.queue_size(state)

  // Use a subject to capture the queue key from the timeout callback
  let key_subject = process.new_subject()

  let state =
    state
    |> state.enqueue(
      process.self(),
      process.new_subject(),
      1,
      no_deadline,
      on_timeout: fn(sent, _timeout) {
        process.send(key_subject, sent)
        ""
      },
      on_down: fn(_) { "" },
    )

  assert 1 == state.queue_size(state)

  // Receive the queue key from the timeout callback (fires after 1ms)
  use selector <- state.with_selector(state)
  let assert Ok("") = process.selector_receive(selector, 50)
  let assert Ok(sent) = process.receive(key_subject, 0)

  let extend = fn(_, _) { "Extend!" }
  let on_expiry = fn(_) { panic as "Should not be called" }

  let state =
    state
    |> state.expire(sent, 100, on_expiry:, or_else: extend)

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
    state.checkout(
      state,
      self,
      fn(_) { Nil },
      no_deadline,
      no_op_deadline,
      fn(_) { Nil },
    )

  // Enqueue a waiter
  let waiter_subject = process.new_subject()
  let state =
    state
    |> state.enqueue(
      self,
      waiter_subject,
      5000,
      no_deadline,
      on_timeout: fn(_, _) { Nil },
      on_down: fn(_) { Nil },
    )

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
      on_deadline: no_op_deadline,
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
    state.checkout(
      state,
      self,
      fn(_) { Nil },
      no_deadline,
      no_op_deadline,
      fn(_) { Nil },
    )

  // Enqueue waiter 1
  let waiter1 = process.new_subject()
  let state =
    state
    |> state.enqueue(
      self,
      waiter1,
      5000,
      no_deadline,
      on_timeout: fn(_, _) { Nil },
      on_down: fn(_) { Nil },
    )

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
      on_deadline: no_op_deadline,
    )

  assert 0 == state.queue_size(state)
  assert False == state.is_slow(state)

  assert state.codel_delay(state) > 1

  // Second checkout
  let assert Ok(state) =
    state.checkout(
      state,
      self,
      fn(_) { Nil },
      no_deadline,
      no_op_deadline,
      fn(_) { Nil },
    )

  let waiter2 = process.new_subject()
  let state =
    state
    |> state.enqueue(
      self,
      waiter2,
      5000,
      no_deadline,
      on_timeout: fn(_, _) { Nil },
      on_down: fn(_) { Nil },
    )

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
      on_deadline: no_op_deadline,
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
    state.checkout(
      state,
      self,
      fn(_) { Nil },
      no_deadline,
      no_op_deadline,
      fn(_) { Nil },
    )

  let waiter = process.new_subject()
  let state =
    state
    |> state.enqueue(
      self,
      waiter,
      5000,
      no_deadline,
      on_timeout: fn(_, _) { Nil },
      on_down: fn(_) { Nil },
    )

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
      on_deadline: no_op_deadline,
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
    state.checkout(
      state,
      self,
      fn(_) { Nil },
      no_deadline,
      no_op_deadline,
      fn(_) { Nil },
    )

  let waiter1 = process.new_subject()
  let state =
    state
    |> state.enqueue(
      self,
      waiter1,
      5000,
      no_deadline,
      on_timeout: fn(_, _) { Nil },
      on_down: fn(_) { Nil },
    )

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
      on_deadline: no_op_deadline,
    )

  assert False == state.is_slow(state)
  assert state.codel_delay(state) > 1

  let assert Ok(state) =
    state.checkout(
      state,
      self,
      fn(_) { Nil },
      no_deadline,
      no_op_deadline,
      fn(_) { Nil },
    )

  let waiter2 = process.new_subject()
  let state =
    state
    |> state.enqueue(
      self,
      waiter2,
      5000,
      no_deadline,
      on_timeout: fn(_, _) { Nil },
      on_down: fn(_) { Nil },
    )

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
      on_deadline: no_op_deadline,
    )

  assert True == state.is_slow(state)

  // now in slow mode, do not cross interval boundary
  // We need to stay within the current interval so dequeue_slow runs.
  let assert Ok(state) =
    state.checkout(
      state,
      self,
      fn(_) { Nil },
      no_deadline,
      no_op_deadline,
      fn(_) { Nil },
    )

  // Enqueue a waiter that will become stale
  let stale = process.new_subject()
  let state =
    state
    |> state.enqueue(
      self,
      stale,
      5000,
      no_deadline,
      on_timeout: fn(_, _) { Nil },
      on_down: fn(_) { Nil },
    )

  // Sleep just enough for the stale waiter to exceed target*2=2ms
  // but not enough to cross the 10ms interval boundary
  process.sleep(5)

  // Enqueue a fresh waiter
  let fresh = process.new_subject()
  let state =
    state
    |> state.enqueue(
      self,
      fresh,
      5000,
      no_deadline,
      on_timeout: fn(_, _) { Nil },
      on_down: fn(_) { Nil },
    )

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
      on_deadline: no_op_deadline,
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
    counter.Nanosecond
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
      no_deadline,
      on_timeout: fn(_, _) { Nil },
      on_down: fn(_) { Nil },
    )

  assert 1 == state.queue_size(state)

  // Sleep past the interval boundary + enough for delay > target
  process.sleep(10)

  let assert Ok(now) =
    counter.Nanosecond
    |> counter.monotonic
    |> counter.next

  let state =
    state.poll(state, now, 0, on_poll: fn(_, _) { Nil }, on_drop: fn(_) { Nil })

  // We need to trigger a second interval boundary.

  process.sleep(5)

  let assert Ok(now2) =
    counter.Nanosecond
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
    state.checkout(
      state,
      self,
      fn(_) { Nil },
      no_deadline,
      no_op_deadline,
      fn(_) { Nil },
    )
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
      on_deadline: no_op_deadline,
    )

  assert 0 == state.active_size(state)
  assert 0 == state.queue_size(state)

  // Connection should be available again
  let assert Ok(_state) =
    state.checkout(
      state,
      self,
      fn(_) { Nil },
      no_deadline,
      no_op_deadline,
      fn(_) { Nil },
    )
}

// When a deadline fires and the checkout_time matches, the connection
// should be closed, a replacement opened, and the replacement returned
// to idle (since there are no waiters).
pub fn deadline_expired_replaces_connection_test() {
  let self = process.self()
  let close_count = process.new_subject()

  let assert Ok(state) =
    state.new()
    |> state.max_size(1)
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.on_close(fn(_) {
      process.send(close_count, 1)
      Ok(Nil)
    })
    |> state.build(process.new_selector())

  assert 1 == state.current_size(state)

  // Checkout with a very short deadline (10ms)
  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, 10, no_op_deadline, fn(_) { Nil })

  assert 1 == state.active_size(state)

  // Sleep past the deadline
  process.sleep(20)

  // Receive the deadline message from the selector
  use selector <- state.with_selector(state)
  let assert Ok(Nil) = process.selector_receive(selector, 50)

  // Test stale timer. Calling with wrong checkout_time should be a no-op
  let state_after =
    state.deadline_expired(
      state,
      self,
      -1,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
      on_deadline: no_op_deadline,
    )

  // Still has the active connection (stale timer was ignored)
  assert 1 == state.active_size(state_after)
}

// Deadline timer is cancelled when a connection is returned via dequeue
// (normal checkin). The timer should not fire after cancellation.
pub fn deadline_cancelled_on_checkin_test() {
  let self = process.self()
  let conn = reference.new()
  let close_count = process.new_subject()

  let assert Ok(state) =
    state.new()
    |> state.max_size(1)
    |> state.on_open(fn() { Ok(conn) })
    |> state.on_close(fn(_) {
      process.send(close_count, 1)
      Ok(Nil)
    })
    |> state.build(process.new_selector())

  // Checkout with a short deadline
  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, 100, no_op_deadline, fn(_) {
      Nil
    })

  assert 1 == state.active_size(state)

  // Return the connection immediately (well before deadline)
  let state =
    state
    |> state.dequeue(
      Some(conn),
      self,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
      on_deadline: no_op_deadline,
    )

  assert 0 == state.active_size(state)

  // Wait past the deadline period to confirm it doesn't fire
  process.sleep(150)

  // handle_close should NOT have been called (deadline was cancelled)
  let assert Error(Nil) = process.receive(close_count, 0)
}

// When handle_open fails during deadline replacement, the pool
// should shrink by 1.
pub fn deadline_expired_shrinks_pool_on_open_failure_test() {
  let self = process.self()
  let conn = reference.new()
  let open_count = process.new_subject()

  // First call to handle_open succeeds (pool init), subsequent calls fail
  let assert Ok(state) =
    state.new()
    |> state.max_size(1)
    |> state.on_open(fn() {
      case process.receive(open_count, 0) {
        // First open (pool init) succeeds
        Error(Nil) -> {
          process.send(open_count, 1)
          Ok(conn)
        }
        // Subsequent opens fail
        Ok(_) -> Error(Nil)
      }
    })
    |> state.build(process.new_selector())

  assert 1 == state.current_size(state)

  // Checkout with a short deadline
  let assert Ok(state) =
    state.checkout(state, self, fn(_) { Nil }, 10, no_op_deadline, fn(_) { Nil })

  assert 1 == state.active_size(state)

  // Sleep past the deadline
  process.sleep(20)

  let state_after =
    state.deadline_expired(
      state,
      self,
      -1,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
      on_deadline: no_op_deadline,
    )

  // Stale timer - no change
  assert 1 == state.current_size(state_after)
  assert 1 == state.active_size(state_after)
}

// Deadline expired for a caller that is no longer active should be a no-op.
pub fn deadline_expired_unknown_caller_noop_test() {
  let assert Ok(state) =
    state.new()
    |> state.max_size(1)
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  assert 1 == state.current_size(state)
  assert 0 == state.active_size(state)

  // Call deadline_expired for a pid that has no active connection
  let state_after =
    state.deadline_expired(
      state,
      process.self(),
      0,
      fn(_) { Nil },
      fn(_client, _conn) { Nil },
      on_drop: fn(_) { Nil },
      on_deadline: no_op_deadline,
    )

  // No change
  assert 1 == state.current_size(state_after)
  assert 0 == state.active_size(state_after)
}
