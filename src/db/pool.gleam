import db/pool/internal/state.{type State}
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

/// A `Pool` configuration that informs how many connections will
/// be opened, and how they will be opened when the pool is started.
/// Also requires configured handlers for closing connections and
/// for pinging connections every `idle_interval` milliseconds.
pub opaque type Pool(conn, err) {
  Pool(
    size: Int,
    idle_interval: Int,
    handle_open: fn() -> Result(conn, PoolError(err)),
    handle_close: fn(conn) -> Result(Nil, PoolError(err)),
    handle_ping: fn(conn) -> Nil,
  )
}

/// Returns a `Pool` that needs to be configured.
pub fn new() -> Pool(conn, err) {
  let handle_open = fn() { Error(ConnectionTimeout) }
  let handle_close = fn(_) { Ok(Nil) }
  let handle_ping = fn(_) { Nil }

  Pool(size: 5, idle_interval: 1000, handle_open:, handle_close:, handle_ping:)
}

/// Sets the size of the pool. At startup the pool will create `size`
/// number of connections.
pub fn size(pool: Pool(conn, err), size: Int) -> Pool(conn, err) {
  Pool(..pool, size:)
}

/// Sets the `Pool`'s `idle_interval` value. The pool will call the
/// configured `on_ping` function every `idle_interval` milliseconds.
pub fn idle_interval(
  pool: Pool(conn, err),
  idle_interval: Int,
) -> Pool(conn, err) {
  Pool(..pool, idle_interval:)
}

/// Sets the `Pool`'s `on_open` function. The provided function
/// will be called at startup to create connections.
pub fn on_open(
  pool: Pool(conn, err),
  handle_open: fn() -> Result(conn, err),
) -> Pool(conn, err) {
  let handle_open = fn() { handle_open() |> result.map_error(ConnectionError) }

  Pool(..pool, handle_open:)
}

/// Sets the `Pool`'s `on_close` function. The provided function
/// will be called when the pool is shut down or exits.
pub fn on_close(
  pool: Pool(conn, err),
  handle_close: fn(conn) -> Result(Nil, err),
) -> Pool(conn, err) {
  let handle_close = fn(conn) {
    handle_close(conn) |> result.map_error(ConnectionError)
  }

  Pool(..pool, handle_close:)
}

/// Sets the `Pool`'s `on_close` function. The provided function
/// will be called every `idle_interval` milliseconds.
pub fn on_ping(
  pool: Pool(conn, err),
  handle_ping: fn(conn) -> Nil,
) -> Pool(conn, err) {
  Pool(..pool, handle_ping:)
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
    let _timer = process.send_after(started.data, pool.idle_interval, Ping)

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
  |> state.on_ping(pool.handle_ping)
  |> state.idle(resources)
  |> state.idle_interval(pool.idle_interval)
  |> actor.initialised
  |> actor.selecting(selector)
  |> actor.returning(self)
}

pub opaque type Message(conn, err) {
  Ping
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

/// Attempts to check out a connection from the pool. If successful
/// the connection will be associated with the caller's `Pid`. If
/// no connections are available, the caller will be added to a
/// queue. Calls to this function will time out if no connections
/// become available before the specified timeout.
///
/// The provided timeout will have 50 milliseconds added onto it
/// internally. So if you want to wait at most 1000 milliseconds,
/// you should provided a timeout of 950.
///
/// The 50 millisecond padding added here allows callers to wait in
/// the queue. The queue will respond with a connection or an error
/// value before the provided `timeout` is reached.
///
/// Without padding the timeout the process will always panic before
/// callers added to the queue can be sent a reply.
pub fn checkout(
  pool: Subject(Message(conn, err)),
  caller: Pid,
  timeout: Int,
) -> Result(conn, PoolError(err)) {
  process.call(pool, timeout + 50, CheckOut(_, caller:, timeout:))
}

/// Returns a connection back to the pool. If there are other callers
/// waiting in the queue, the provided connection will be given to them.
/// Otherwise the connection will be returned back to the list of idle
/// connections.
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
    Ping -> handle_ping(state)
    CheckIn(conn:, caller:) -> handle_checkin(state, conn, caller)
    CheckOut(client:, caller:, timeout:) ->
      handle_checkout(state, client, caller, timeout)
    Timeout(time_sent:, timeout:) -> handle_timeout(state, time_sent, timeout)
    PoolExit(exit) -> handle_pool_exit(state, exit)
    CallerDown(down) -> handle_caller_down(state, down)
    Shutdown(client:) -> handle_shutdown(state, client)
  }
}

fn handle_ping(
  state: State(conn, Message(conn, err), PoolError(err)),
) -> actor.Next(
  State(conn, Message(conn, err), PoolError(err)),
  Message(conn, err),
) {
  let state = state.ping(state, Ping)

  use selector <- state.with_selector(state)

  actor.continue(state)
  |> actor.with_selector(selector)
}

fn handle_checkin(
  state: State(conn, Message(conn, err), PoolError(err)),
  conn: conn,
  caller: Pid,
) -> actor.Next(
  State(conn, Message(conn, err), PoolError(err)),
  Message(conn, err),
) {
  let handler = fn(client, conn) { actor.send(client, Ok(conn)) }
  let state = state.dequeue(state, Some(conn), caller, CallerDown, handler)

  use selector <- state.with_selector(state)

  actor.continue(state)
  |> actor.with_selector(selector)
}

fn handle_checkout(
  state: State(conn, Message(conn, err), PoolError(err)),
  client: process.Subject(Result(conn, PoolError(err))),
  caller: Pid,
  timeout: Int,
) -> actor.Next(
  State(conn, Message(conn, err), PoolError(err)),
  Message(conn, err),
) {
  let state = {
    use conn <- state.with_connection(state)

    case conn {
      Some(Ok(conn)) -> {
        actor.send(client, Ok(conn))

        state.claim(state, caller, conn, CallerDown)
      }
      Some(Error(err)) -> {
        actor.send(client, Error(err))

        state
      }
      None -> state.enqueue(state, caller, client, timeout, Timeout, CallerDown)
    }
  }

  use selector <- state.with_selector(state)

  actor.continue(state)
  |> actor.with_selector(selector)
}

fn handle_timeout(
  state: State(conn, Message(conn, err), PoolError(err)),
  sent: Int,
  timeout: Int,
) -> actor.Next(
  State(conn, Message(conn, err), PoolError(err)),
  Message(conn, err),
) {
  let state = {
    let on_expiry = actor.send(_, Error(ConnectionTimeout))

    state.expire(state, sent, timeout, on_expiry:, or_else: Timeout)
  }

  use selector <- state.with_selector(state)

  actor.continue(state)
  |> actor.with_selector(selector)
}

fn handle_pool_exit(
  state: State(conn, Message(conn, err), PoolError(err)),
  exit: process.ExitMessage,
) -> actor.Next(
  State(conn, Message(conn, err), PoolError(err)),
  Message(conn, err),
) {
  state.close(state)

  case exit.reason {
    process.Normal -> actor.stop()
    process.Killed -> actor.stop_abnormal("pool killed")
    process.Abnormal(_reason) -> actor.stop_abnormal("pool stopped abnormally")
  }
}

fn handle_caller_down(
  state: State(conn, Message(conn, err), PoolError(err)),
  down: process.Down,
) -> actor.Next(
  State(conn, Message(conn, err), PoolError(err)),
  Message(conn, err),
) {
  let assert process.ProcessDown(pid:, ..) = down

  let handler = fn(client, conn) { actor.send(client, Ok(conn)) }
  let state = state.dequeue(state, None, pid, CallerDown, handler)

  use selector <- state.with_selector(state)

  actor.continue(state)
  |> actor.with_selector(selector)
}

fn handle_shutdown(
  state: State(conn, Message(conn, err), PoolError(err)),
  client: process.Subject(Result(Nil, PoolError(err))),
) -> actor.Next(
  State(conn, Message(conn, err), PoolError(err)),
  Message(conn, err),
) {
  state.shutdown(state)

  actor.send(client, Ok(Nil))
  actor.stop()
}
