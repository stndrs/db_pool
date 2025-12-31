import db_pool/internal
import db_pool/internal/state
import gleam/erlang/process
import gleam/erlang/reference
import gleam/option.{None, Some}
import gleam/result

pub fn current_connection_none_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  let self = process.self()

  assert None == state.current_connection(state, self)
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

pub fn claim_test() {
  let assert Ok(state) =
    state.new()
    |> state.max_size(1)
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  assert 0 == state.active_size(state)

  let self = process.self()

  assert None == state.current_connection(state, self)

  let state =
    state.next_connection(state, fn(conn) {
      let assert Some(Ok(conn)) = conn

      state.claim(state, self, conn, fn(_) { Nil })
    })

  let assert Some(_) = state.current_connection(state, self)

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

  // Claim a connection
  let state =
    state
    |> state.next_connection(fn(conn) {
      let assert Some(Ok(conn)) = conn

      state
      |> state.claim(self, conn, fn(_) { Nil })
    })

  assert 0 == state.queue_size(state)

  let subject = process.new_subject()

  // Enqueue a waiting process
  state
  |> state.enqueue(self, subject, 1000, fn(_, _) { Nil }, fn(_) { Nil })

  assert 1 == state.queue_size(state)

  state
  |> state.dequeue(Some(conn), self, fn(_) { Nil }, fn(_client, _conn) { Nil })

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

  // Claim a connection
  let state =
    state
    |> state.next_connection(fn(conn) {
      let assert Some(Ok(conn)) = conn

      state
      |> state.claim(self, conn, fn(_) { Nil })
    })

  assert 0 == state.queue_size(state)

  let subject = process.new_subject()

  // Enqueue a waiting process
  state
  |> state.enqueue(self, subject, 5000, fn(_, _) { Nil }, fn(_) { Nil })

  assert 1 == state.queue_size(state)

  state
  |> state.dequeue(None, self, fn(_) { Nil }, fn(_client, _conn) { Nil })

  // Ensure queued process has been removed from the queue
  assert 0 == state.queue_size(state)
}

pub fn expire_test() {
  let assert Ok(state) =
    state.new()
    |> state.on_open(fn() { Ok(reference.new()) })
    |> state.build(process.new_selector())

  let now_in_ms = internal.now_in_ms()

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

  let now_in_ms = internal.now_in_ms()

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
