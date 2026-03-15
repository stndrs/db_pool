import db_pool/internal/state
import gleam/erlang/process
import gleam/erlang/reference
import gleam/result
import rasa/counter
import rasa/monotonic

// Large deadline used in tests that don't exercise deadline behaviour.
const no_deadline = 999_999

pub fn current_connection_error_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_subject())

  let self = process.self()

  let assert Error(Nil) = state.current_connection(state, self)
}

pub fn enqueue_test() {
  let self_subject = process.new_subject()
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(self_subject)

  let self = process.self()
  let subject = process.new_subject()

  assert 0 == state.queue_size(state)

  let state =
    state
    |> state.enqueue(self, subject, 100, no_deadline, on_timeout: fn(_, _) {
      "Timeout!"
    })

  let assert Ok("Timeout!") = process.receive(self_subject, 150)

  assert 1 == state.queue_size(state)
}

pub fn expire_test() {
  let self_subject = process.new_subject()
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(self_subject)

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
    )

  // Enqueue waiting process
  assert 1 == state.queue_size(state)

  // Receive the queue key from the timeout callback (fires after 1ms)
  let assert Ok(Nil) = process.receive(self_subject, 50)
  let assert Ok(sent) = process.receive(key_subject, 0)

  let extend = fn(_, _) { panic as "should not be called" }
  let on_expiry = fn(_) { Nil }

  state
  |> state.expire(sent, -100, on_expiry:, or_else: extend)

  // Ensure queued process has been removed from the queue
  assert 0 == state.queue_size(state)
}

pub fn expire_retry_test() {
  let self_subject = process.new_subject()
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(self_subject)

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
    )

  assert 1 == state.queue_size(state)

  // Receive the queue key from the timeout callback (fires after 1ms)
  let assert Ok("") = process.receive(self_subject, 50)
  let assert Ok(sent) = process.receive(key_subject, 0)

  let extend = fn(_, _) { "Extend!" }
  let on_expiry = fn(_) { panic as "Should not be called" }

  let state =
    state
    |> state.expire(sent, 100, on_expiry:, or_else: extend)

  // Ensure queued process is still in the queue
  assert 1 == state.queue_size(state)

  let assert Ok("Extend!") = process.receive(self_subject, 150)
}

pub fn ping_test() {
  let self_subject = process.new_subject()

  let assert Ok(_state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.interval(10)
    |> state.build(self_subject)
    |> result.map(state.ping(_, "Ping!"))

  let assert Ok("Ping!") = process.receive(self_subject, 100)
}

pub fn shutdown_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_subject())

  assert Nil == state.shutdown(state)
}

pub fn close_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_subject())

  assert Nil == state.close(state)
}

// The poll function should schedule the next poll timer and return
// updated state.
pub fn poll_schedules_next_poll_test() {
  let self_subject = process.new_subject()
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.queue_interval(50)
    |> state.build(self_subject)

  let now =
    monotonic.Nanosecond
    |> counter.monotonic
    |> counter.next

  // Poll with no queue entries should just schedule next poll
  let _state =
    state.poll(
      state,
      now,
      now,
      on_poll: fn(time, last_sent) { #("Poll", time, last_sent) },
      on_drop: fn(_) { Nil },
    )

  // The poll timer should fire within queue_interval + buffer
  let assert Ok(#("Poll", _, _)) = process.receive(self_subject, 150)
}

/// Poll should trigger CoDel slow mode when the queue has stalled
/// and delay exceeds the target.
pub fn poll_enters_slow_on_stall_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.queue_target(1)
    |> state.queue_interval(1)
    |> state.build(process.new_subject())

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
    )

  assert 1 == state.queue_size(state)

  // Sleep past the interval boundary + enough for delay > target
  process.sleep(10)

  let now =
    monotonic.Nanosecond
    |> counter.monotonic
    |> counter.next

  let state =
    state.poll(state, now, 0, on_poll: fn(_, _) { Nil }, on_drop: fn(_) { Nil })

  // We need to trigger a second interval boundary.

  process.sleep(5)

  let now2 =
    monotonic.Nanosecond
    |> counter.monotonic
    |> counter.next

  let state =
    state.poll(state, now2, 0, on_poll: fn(_, _) { Nil }, on_drop: fn(_) { Nil })

  assert True == state.is_slow(state)
}

// --- HolderRef tests ---

pub fn new_holder_creates_holder_test() {
  let ref = state.new_holder("test_conn")
  let assert Ok("test_conn") = state.get_conn(ref)
  let assert Ok(Nil) = state.destroy_holder(ref)
}

pub fn holder_store_and_get_conn_test() {
  let ref = state.new_holder(42)
  let assert Ok(42) = state.get_conn(ref)
  let assert Ok(Nil) = state.destroy_holder(ref)
}

pub fn destroy_holder_makes_lookups_fail_test() {
  let ref = state.new_holder("conn")
  let assert Ok(Nil) = state.destroy_holder(ref)
  let assert Error(Nil) = state.get_conn(ref)
}

pub fn holder_overwrite_conn_test() {
  let ref = state.new_holder("first")
  let assert Ok(Nil) = state.store_conn(ref, "second")
  let assert Ok("second") = state.get_conn(ref)
  let assert Ok(Nil) = state.destroy_holder(ref)
}

// --- PoolCounter tests ---

pub fn new_pool_counter_initializes_to_value_test() {
  let ctr = state.new_pool_counter(5)
  let assert 5 = state.counter_get(ctr)
}

pub fn counter_checkout_decrements_test() {
  let ctr = state.new_pool_counter(3)
  let assert Ok(2) = state.counter_checkout(ctr)
  let assert 2 = state.counter_get(ctr)
}

pub fn counter_checkout_multiple_test() {
  let ctr = state.new_pool_counter(3)
  let assert Ok(2) = state.counter_checkout(ctr)
  let assert Ok(1) = state.counter_checkout(ctr)
  let assert Ok(0) = state.counter_checkout(ctr)
  let assert 0 = state.counter_get(ctr)
}

pub fn counter_checkout_when_empty_returns_error_test() {
  let ctr = state.new_pool_counter(1)
  let assert Ok(0) = state.counter_checkout(ctr)
  let assert Error(Nil) = state.counter_checkout(ctr)
  let assert 0 = state.counter_get(ctr)
}

pub fn counter_checkout_undo_is_clean_test() {
  let ctr = state.new_pool_counter(0)
  let assert Error(Nil) = state.counter_checkout(ctr)
  let assert 0 = state.counter_get(ctr)
}

pub fn counter_checkin_increments_test() {
  let ctr = state.new_pool_counter(2)
  let assert Ok(1) = state.counter_checkout(ctr)
  let assert 2 = state.counter_checkin(ctr)
  let assert 2 = state.counter_get(ctr)
}

pub fn counter_checkout_then_checkin_cycle_test() {
  let ctr = state.new_pool_counter(3)
  let assert Ok(2) = state.counter_checkout(ctr)
  let assert Ok(1) = state.counter_checkout(ctr)
  let assert Ok(0) = state.counter_checkout(ctr)
  let assert 0 = state.counter_get(ctr)

  let assert Error(Nil) = state.counter_checkout(ctr)

  let assert 1 = state.counter_checkin(ctr)

  let assert Ok(0) = state.counter_checkout(ctr)
  let assert 0 = state.counter_get(ctr)
}

pub fn new_pool_counter_with_zero_test() {
  let ctr = state.new_pool_counter(0)
  let assert 0 = state.counter_get(ctr)
  let assert Error(Nil) = state.counter_checkout(ctr)
}
