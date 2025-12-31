import db/pool/internal
import db/pool/internal/state
import gleam/erlang/process
import gleam/option.{None, Some}

pub fn enqueue_test() {
  let state =
    process.new_selector()
    |> state.new

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

pub type Connection {
  Connection(Int)
}

pub fn claim_test() {
  let state =
    process.new_selector()
    |> state.new
    |> state.idle([Connection(1)])

  assert 0 == state.active_size(state)

  let state =
    state.with_connection(state, fn(conn) {
      let assert Some(Ok(conn)) = conn

      state.claim(state, process.self(), conn, fn(_) { Nil })
    })

  assert 1 == state.active_size(state)
}

pub fn dequeue_test() {
  let self = process.self()
  let connection = Connection(1)

  // Initialize new state
  let state =
    process.new_selector()
    |> state.new
    |> state.idle([connection])

  // Claim a connection
  let state =
    state
    |> state.with_connection(fn(conn) {
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
  |> state.dequeue(Some(connection), self, fn(_) { Nil }, fn(_client, _conn) {
    Nil
  })

  // Ensure previously queued process removed from queue
  assert 0 == state.queue_size(state)
}

pub fn dequeue_without_connection_test() {
  let self = process.self()

  // Initialize new state
  let state =
    process.new_selector()
    |> state.new
    |> state.idle([Connection(1)])

  // Claim a connection
  let state =
    state
    |> state.with_connection(fn(conn) {
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
  let state =
    process.new_selector()
    |> state.new

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
  let state =
    process.new_selector()
    |> state.new

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

  let state =
    selector
    |> state.new
    |> state.idle_interval(10)
    |> state.ping("Ping!")

  use selector <- state.with_selector(state)

  let assert Ok("Ping!") = process.selector_receive(selector, 100)
}

pub fn shutdown_test() {
  assert Nil
    == process.new_selector()
    |> state.new
    |> state.shutdown
}

pub fn close_test() {
  assert Nil
    == process.new_selector()
    |> state.new
    |> state.close
}
