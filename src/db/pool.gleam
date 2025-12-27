import db/pool/internal/queue
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result

/// A `Pool` configuration that informs how many connections will
/// be opened, and how they will be opened when the pool is started.
/// Also requires configured handlers for closing connections and
/// for pinging connections every `idle_interval` milliseconds.
pub opaque type Pool(conn, err) {
  Pool(
    size: Int,
    idle_interval: Int,
    handle_open: fn() -> Result(conn, err),
    handle_close: fn(conn) -> Result(Nil, err),
    handle_ping: fn(conn) -> Result(Nil, err),
  )
}

/// Returns a `Pool` that needs to be configured.
pub fn new() -> Pool(conn, err) {
  let handle_open = fn() { panic as "Pool not configured" }
  let handle_close = fn(_) { Ok(Nil) }
  let handle_ping = fn(_) { Ok(Nil) }

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
  Pool(..pool, handle_open:)
}

/// Sets the `Pool`'s `on_close` function. The provided function
/// will be called when the pool is shut down or exits.
pub fn on_close(
  pool: Pool(conn, err),
  handle_close: fn(conn) -> Result(Nil, err),
) -> Pool(conn, err) {
  Pool(..pool, handle_close:)
}

/// Sets the `Pool`'s `on_close` function. The provided function
/// will be called every `idle_interval` milliseconds.
pub fn on_ping(
  pool: Pool(conn, err),
  handle_ping: fn(conn) -> Result(Nil, err),
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
    State(conn, err),
    Message(conn, err),
    Subject(Message(conn, err)),
  ),
  String,
) {
  let resources = {
    list.repeat("", pool.size)
    |> list.try_map(fn(_) { pool.handle_open() })
    |> result.map_error(fn(_) { "Pgl pool failed to initialise" })
  }

  use resources <- result.map(resources)

  process.trap_exits(True)

  let selector =
    process.new_selector()
    |> process.select(self)
    |> process.select_trapped_exits(PoolExit)

  process.send(self, Ping(self, pool.idle_interval))

  State(
    selector:,
    max_size: pool.size,
    current_size: pool.size,
    handle_open: pool.handle_open,
    handle_close: pool.handle_close,
    handle_ping: pool.handle_ping,
    idle: resources,
    live: dict.new(),
    queue: queue.new("db_pool_queue"),
  )
  |> actor.initialised
  |> actor.selecting(selector)
  |> actor.returning(self)
}

type State(conn, err) {
  State(
    selector: process.Selector(Message(conn, err)),
    // pool capacity
    max_size: Int,
    // current number of connections
    current_size: Int,
    // create connection
    handle_open: fn() -> Result(conn, err),
    // close connection
    handle_close: fn(conn) -> Result(Nil, err),
    // called on each connection every `idle_interval`
    handle_ping: fn(conn) -> Result(Nil, err),
    // idle connections
    idle: List(conn),
    // connections in use
    live: Dict(Pid, Live(conn)),
    // processes waiting for a connection
    queue: queue.Queue(Int, #(Int, Waiting(conn, err))),
  )
}

type Live(conn) {
  Live(conn: conn, monitor: process.Monitor)
}

type Waiting(conn, err) {
  Waiting(
    caller: Pid,
    monitor: process.Monitor,
    client: Subject(Result(conn, err)),
  )
}

pub opaque type Message(conn, err) {
  Ping(subject: Subject(Message(conn, err)), timeout: Int)
  CheckIn(conn: conn, caller: Pid)
  CheckOut(client: Subject(Result(conn, err)), caller: Pid)
  PoolExit(process.ExitMessage)
  CallerDown(process.Down)
  Shutdown(client: process.Subject(Result(Nil, err)))
}

/// Attempts to check out a connection from the pool. If successful
/// the connection will be associated with the caller's `Pid`. If
/// no connections are available, the caller will be added to a
/// queue. Calls to this function will time out if no connections
/// become available before the specified timeout.
pub fn checkout(
  pool: Subject(Message(conn, err)),
  caller: Pid,
  timeout: Int,
) -> Result(conn, err) {
  process.call(pool, timeout, CheckOut(_, caller:))
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
) -> Result(Nil, err) {
  process.call(pool, timeout, Shutdown)
}

fn handle_message(
  state: State(conn, err),
  msg: Message(conn, err),
) -> actor.Next(State(conn, err), Message(conn, err)) {
  case msg {
    Ping(subject:, timeout:) -> handle_ping(state, subject, timeout)
    CheckIn(conn:, caller:) -> handle_checkin(state, conn, caller)
    CheckOut(client:, caller:) -> handle_checkout(state, client, caller)
    PoolExit(exit) -> handle_pool_exit(state, exit)
    CallerDown(down) -> handle_caller_down(state, down)
    Shutdown(client:) -> handle_shutdown(state, client)
  }
}

fn handle_ping(
  state: State(conn, err),
  subject: process.Subject(Message(conn, err)),
  timeout: Int,
) -> actor.Next(State(conn, err), Message(conn, err)) {
  state.idle
  |> list.each(fn(conn) {
    let _ = state.handle_ping(conn)

    process.send_after(subject, timeout, Ping(subject, timeout))
  })

  actor.continue(state)
}

fn handle_checkin(
  state: State(conn, err),
  checked_out: conn,
  caller: Pid,
) -> actor.Next(State(conn, err), Message(conn, err)) {
  let state = case dict.get(state.live, caller) {
    Error(_) -> state
    Ok(Live(conn:, monitor:)) -> {
      assert checked_out == conn

      process.demonitor_process(monitor)

      let selector = process.deselect_specific_monitor(state.selector, monitor)

      let live = dict.delete(state.live, caller)

      case queue.first_lookup(state.queue) {
        Some(#(sent, #(_int, Waiting(caller:, monitor:, client:)))) -> {
          let assert Ok(Nil) = queue.delete_key(state.queue, sent)

          actor.send(client, Ok(conn))

          let live = dict.insert(live, caller, Live(conn:, monitor:))

          State(..state, selector:, live:)
        }
        None -> {
          let idle = list.prepend(state.idle, conn)

          State(..state, selector:, idle:, live:)
        }
      }
    }
  }

  actor.continue(state)
  |> actor.with_selector(state.selector)
}

fn handle_checkout(
  state: State(conn, err),
  client: process.Subject(Result(conn, err)),
  caller: Pid,
) -> actor.Next(State(conn, err), Message(conn, err)) {
  let monitor = process.monitor(caller)
  let selector =
    process.select_specific_monitor(state.selector, monitor, CallerDown)

  let state = case state.idle {
    [] if state.current_size < state.max_size -> {
      state.handle_open()
      |> result.map(fn(conn) {
        actor.send(client, Ok(conn))

        let live = dict.insert(state.live, caller, Live(conn:, monitor:))

        State(..state, selector:, live:, current_size: state.current_size + 1)
      })
      |> result.unwrap(state)
    }
    [] -> {
      let key = monotonic_time()
      let value = #(unique_int(), Waiting(caller:, monitor:, client:))

      // If `queue.insert` fails, the client waiting for a connection will
      // timeout.
      let _ = queue.insert(state.queue, key, value)

      State(..state, selector:)
    }
    [conn, ..idle] -> {
      actor.send(client, Ok(conn))

      let live = dict.insert(state.live, caller, Live(conn:, monitor:))

      State(..state, selector:, live:, idle:)
    }
  }

  actor.continue(state)
  |> actor.with_selector(state.selector)
}

fn handle_pool_exit(
  state: State(conn, err),
  exit: process.ExitMessage,
) -> actor.Next(State(conn, err), Message(conn, err)) {
  state.idle
  |> list.each(state.handle_close)

  case exit.reason {
    process.Normal -> actor.stop()
    process.Killed -> actor.stop_abnormal("pool killed")
    process.Abnormal(_reason) -> actor.stop_abnormal("pool stopped abnormally")
  }
}

fn handle_caller_down(
  state: State(conn, err),
  down: process.Down,
) -> actor.Next(State(conn, err), Message(conn, err)) {
  let assert process.ProcessDown(pid:, ..) = down

  let state = case dict.get(state.live, pid) {
    Error(_) -> state
    Ok(Live(conn:, monitor:)) -> {
      process.demonitor_process(monitor)

      let selector = process.deselect_specific_monitor(state.selector, monitor)

      let live = dict.delete(state.live, pid)

      case queue.first_lookup(state.queue) {
        Some(#(sent, #(_int, Waiting(caller:, monitor:, client:)))) -> {
          let assert Ok(Nil) = queue.delete_key(state.queue, sent)

          actor.send(client, Ok(conn))

          let live = dict.insert(live, caller, Live(conn:, monitor:))

          State(..state, selector:, live:)
        }
        None -> {
          let idle = list.prepend(state.idle, conn)

          State(..state, selector:, idle:, live:)
        }
      }
    }
  }

  actor.continue(state)
  |> actor.with_selector(state.selector)
}

fn handle_shutdown(
  state: State(conn, err),
  client: process.Subject(Result(Nil, err)),
) -> actor.Next(State(conn, err), Message(conn, err)) {
  case dict.size(state.live) {
    0 -> {
      state.idle
      |> list.each(state.handle_close)

      actor.send(client, Ok(Nil))
      actor.stop()
    }
    _ -> {
      actor.send(client, Ok(Nil))
      actor.stop()
    }
  }
}

@external(erlang, "db_pool_ffi", "unique_int")
fn unique_int() -> Int

@external(erlang, "erlang", "monotonic_time")
fn monotonic_time() -> Int
