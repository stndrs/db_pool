import db_pool/internal/state.{type State}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result

pub type PoolError(err) {
  ConnectionError(err)
  ConnectionTimeout
}

/// A `Pool` configuration. Holds the size of the pool and functions
/// for opening and closing connections. Can also be provided
/// a function to be run every `interval` milliseconds.
///
/// Example:
///
/// ```gleam
///   let db_pool = db_pool.new()
///     |> db_pool.size(5)
///     |> db_pool.interval(1000)
///     |> db_pool.on_open(database.open)
///     |> db_pool.on_close(database.close)
///     |> db_pool.on_interval(database.ping)
/// ```
///
pub opaque type Pool(conn, err) {
  Pool(
    size: Int,
    interval: Int,
    handle_open: fn() -> Result(conn, PoolError(err)),
    handle_close: fn(conn) -> Result(Nil, PoolError(err)),
    handle_interval: fn(conn) -> Nil,
  )
}

/// Returns a `Pool` that needs to be configured.
pub fn new() -> Pool(conn, err) {
  let handle_open = fn() { Error(ConnectionTimeout) }
  let handle_close = fn(_) { Ok(Nil) }
  let handle_interval = fn(_) { Nil }

  Pool(size: 5, interval: 1000, handle_open:, handle_close:, handle_interval:)
}

/// Sets the size of the pool. At startup the pool will create `size`
/// number of connections.
pub fn size(pool: Pool(conn, err), size: Int) -> Pool(conn, err) {
  Pool(..pool, size:)
}

/// Sets the `Pool`'s `interval` value. The pool will call the
/// configured `on_interval` function every `interval` milliseconds.
pub fn interval(pool: Pool(conn, err), interval: Int) -> Pool(conn, err) {
  Pool(..pool, interval:)
}

/// Sets the `Pool`'s `on_open` function. The provided function will be
/// called at startup to create connections.
pub fn on_open(
  pool: Pool(conn, err),
  handle_open: fn() -> Result(conn, err),
) -> Pool(conn, err) {
  let handle_open = fn() { handle_open() |> result.map_error(ConnectionError) }

  Pool(..pool, handle_open:)
}

/// Sets the `Pool`'s `on_close` function. The provided function will be
/// called on each idle connection when the pool is shut down or exits.
pub fn on_close(
  pool: Pool(conn, err),
  handle_close: fn(conn) -> Result(Nil, err),
) -> Pool(conn, err) {
  let handle_close = fn(conn) {
    handle_close(conn) |> result.map_error(ConnectionError)
  }

  Pool(..pool, handle_close:)
}

/// Sets the `Pool`'s `on_interval` function. The provided function
/// will be called every `interval` milliseconds.
pub fn on_interval(
  pool: Pool(conn, err),
  handle_interval: fn(conn) -> Nil,
) -> Pool(conn, err) {
  Pool(..pool, handle_interval:)
}

/// Starts a connection pool.
pub fn start(
  pool: Pool(conn, err),
  name: process.Name(Message(conn, err)),
  timeout: Int,
) -> actor.StartResult(Subject(Message(conn, err))) {
  actor.new_with_initialiser(timeout, initialise_pool(_, pool))
  |> actor.on_message(handle_message)
  |> actor.named(name)
  |> actor.start
  |> result.map(fn(started) {
    let _timer = process.send_after(started.data, pool.interval, Interval)

    started
  })
}

/// Creates a `supervision.ChildSpecification` so the pool can be
/// added to an application's supervision tree.
pub fn supervised(
  pool: Pool(conn, err),
  name: process.Name(Message(conn, err)),
  timeout: Int,
) -> supervision.ChildSpecification(Subject(Message(conn, err))) {
  supervision.worker(fn() { start(pool, name, timeout) })
  |> supervision.timeout(timeout)
  |> supervision.restart(supervision.Transient)
}

fn initialise_pool(
  self: Subject(Message(conn, err)),
  pool: Pool(conn, err),
) -> Result(
  actor.Initialised(
    State(conn, Message(conn, err), PoolError(err)),
    Message(conn, err),
    Subject(Message(conn, err)),
  ),
  String,
) {
  let resources = {
    list.repeat("", pool.size)
    |> list.try_map(fn(_) { pool.handle_open() })
    |> result.map_error(fn(_) { "(db_pool) Failed to open connections" })
  }

  use resources <- result.map(resources)

  process.trap_exits(True)

  let selector =
    process.new_selector()
    |> process.select(self)
    |> process.select_trapped_exits(PoolExit)

  state.new(selector)
  |> state.max_size(pool.size)
  |> state.current_size(pool.size)
  |> state.on_open(pool.handle_open)
  |> state.on_close(pool.handle_close)
  |> state.on_interval(pool.handle_interval)
  |> state.idle(resources)
  |> state.interval(pool.interval)
  |> actor.initialised
  |> actor.selecting(selector)
  |> actor.returning(self)
}

pub opaque type Message(conn, err) {
  Interval
  CheckIn(conn: conn, caller: Pid)
  CheckOut(
    client: Subject(Result(conn, PoolError(err))),
    caller: Pid,
    timeout: Int,
  )
  Timeout(time_sent: Int, timeout: Int)
  PoolExit(process.ExitMessage)
  CallerDown(process.Down)
  Shutdown(client: process.Subject(Result(Nil, PoolError(err))))
}

/// Attempts to check out a connection from the pool.
pub fn checkout(
  pool: Subject(Message(conn, err)),
  caller: Pid,
  timeout: Int,
) -> Result(conn, PoolError(err)) {
  let checkout_timeout = timeout - 50

  let #(process_timeout, timeout) = case checkout_timeout < 0 {
    True -> #(timeout, timeout)
    False -> #(timeout + 50, checkout_timeout)
  }

  process.call(pool, process_timeout, CheckOut(_, caller:, timeout:))
}

/// Returns a connection back to the pool.
pub fn checkin(
  pool: Subject(Message(conn, err)),
  conn: conn,
  caller: Pid,
) -> Nil {
  process.send(pool, CheckIn(conn:, caller:))
}

/// Shuts down the pool and any idle connections.
pub fn shutdown(
  pool: Subject(Message(conn, err)),
  timeout: Int,
) -> Result(Nil, PoolError(err)) {
  process.call(pool, timeout, Shutdown)
}

fn handle_message(
  state: State(conn, Message(conn, err), PoolError(err)),
  msg: Message(conn, err),
) -> actor.Next(
  State(conn, Message(conn, err), PoolError(err)),
  Message(conn, err),
) {
  case msg {
    Interval -> {
      let state = state.ping(state, Interval)

      use selector <- state.with_selector(state)

      actor.continue(state)
      |> actor.with_selector(selector)
    }
    CheckIn(conn:, caller:) -> {
      let handler = fn(client, conn) { actor.send(client, Ok(conn)) }
      let state = state.dequeue(state, Some(conn), caller, CallerDown, handler)

      use selector <- state.with_selector(state)

      actor.continue(state)
      |> actor.with_selector(selector)
    }
    CheckOut(client:, caller:, timeout:) -> {
      let state = case state.current_connection(state, caller) {
        Some(conn) -> {
          actor.send(client, Ok(conn))

          state
        }
        None -> {
          use next <- state.next_connection(state)

          next
          |> option.map(fn(conn) {
            actor.send(client, conn)

            conn
            |> result.map(state.claim(state, caller, _, CallerDown))
            |> result.unwrap(state)
          })
          |> option.lazy_unwrap(fn() {
            state.enqueue(
              state,
              caller,
              client,
              timeout,
              on_timeout: Timeout,
              on_down: CallerDown,
            )
          })
        }
      }

      use selector <- state.with_selector(state)

      actor.continue(state)
      |> actor.with_selector(selector)
    }
    Timeout(time_sent:, timeout:) -> {
      let state = {
        let on_expiry = actor.send(_, Error(ConnectionTimeout))

        state.expire(state, time_sent, timeout, on_expiry:, or_else: Timeout)
      }

      use selector <- state.with_selector(state)

      actor.continue(state)
      |> actor.with_selector(selector)
    }
    CallerDown(down) -> {
      let assert process.ProcessDown(pid:, ..) = down

      let handler = fn(client, conn) { actor.send(client, Ok(conn)) }
      let state = state.dequeue(state, None, pid, CallerDown, handler)

      use selector <- state.with_selector(state)

      actor.continue(state)
      |> actor.with_selector(selector)
    }
    PoolExit(exit) -> {
      state.close(state)

      case exit.reason {
        process.Normal -> actor.stop()
        process.Killed -> actor.stop_abnormal("pool killed")
        process.Abnormal(_reason) ->
          actor.stop_abnormal("pool stopped abnormally")
      }
    }
    Shutdown(client:) -> {
      state.shutdown(state)

      actor.send(client, Ok(Nil))
      actor.stop()
    }
  }
}
