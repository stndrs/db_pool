import gleam/bool
import gleam/dict.{type Dict}
import gleam/erlang/atom.{type Atom}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result

pub opaque type Pool(conn) {
  Pool(
    size: Int,
    handle_open: fn() -> Result(conn, String),
    handle_close: fn(conn) -> Nil,
    handle_ping: fn(conn) -> Nil,
  )
}

pub fn new() -> Pool(conn) {
  let handle_open = fn() { Error("open not configured") }
  let handle_close = fn(_) { Nil }
  let handle_ping = fn(_) { Nil }

  Pool(size: 5, handle_open:, handle_close:, handle_ping:)
}

pub fn size(pool: Pool(conn), size: Int) -> Pool(conn) {
  Pool(..pool, size:)
}

pub fn on_open(
  pool: Pool(conn),
  handle_open: fn() -> Result(conn, String),
) -> Pool(conn) {
  Pool(..pool, handle_open:)
}

pub fn on_close(pool: Pool(conn), handle_close: fn(conn) -> Nil) -> Pool(conn) {
  Pool(..pool, handle_close:)
}

pub fn on_ping(pool: Pool(conn), handle_ping: fn(conn) -> Nil) -> Pool(conn) {
  Pool(..pool, handle_ping:)
}

pub fn start(
  pool: Pool(conn),
  timeout: Int,
) -> Result(Subject(Msg(conn)), actor.StartError) {
  actor.new_with_initialiser(timeout, initialise_pool(_, pool))
  |> actor.on_message(handle_message)
  |> actor.start
  |> result.map(fn(started) {
    let subj = started.data

    process.send(subj, Ping(subj, 1000))

    subj
  })
}

pub fn supervised(
  pool: Pool(conn),
  name: process.Name(Msg(conn)),
  timeout: Int,
) -> supervision.ChildSpecification(Subject(Msg(conn))) {
  supervision.worker(fn() {
    actor.new_with_initialiser(timeout, initialise_pool(_, pool))
    |> actor.on_message(handle_message)
    |> actor.named(name)
    |> actor.start
  })
  |> supervision.timeout(timeout)
  |> supervision.restart(supervision.Transient)
}

fn initialise_pool(
  self: Subject(Msg(conn)),
  pool: Pool(conn),
) -> Result(
  actor.Initialised(State(conn), Msg(conn), Subject(Msg(conn))),
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

  State(
    selector:,
    max_size: pool.size,
    current_size: pool.size,
    handle_open: pool.handle_open,
    handle_close: pool.handle_close,
    handle_ping: pool.handle_ping,
    idle: resources,
    live: dict.new(),
    queue: new_queue(),
  )
  |> actor.initialised
  |> actor.selecting(selector)
  |> actor.returning(self)
}

type State(conn) {
  State(
    selector: process.Selector(Msg(conn)),
    max_size: Int,
    current_size: Int,
    handle_open: fn() -> Result(conn, String),
    handle_close: fn(conn) -> Nil,
    handle_ping: fn(conn) -> Nil,
    idle: List(conn),
    live: Dict(Pid, Live(conn)),
    queue: Queue(Int, #(Int, Waiting(conn))),
  )
}

fn with_idle(
  state: State(conn),
  next: fn(conn) -> State(conn),
) -> Result(State(conn), Nil) {
  case state.idle {
    [] -> create_connection(state, next)
    [conn, ..idle] -> {
      let state1 = next(conn)

      Ok(State(..state1, idle:))
    }
  }
}

fn create_connection(
  state: State(conn),
  next: fn(conn) -> State(conn),
) -> Result(State(conn), Nil) {
  use <- bool.guard(state.current_size >= state.max_size, Error(Nil))

  state.handle_open()
  |> result.replace_error(Nil)
  |> result.map(fn(conn) {
    let state1 = next(conn)

    State(..state1, current_size: state1.current_size + 1)
  })
}

type Live(conn) {
  Live(conn: conn, monitor: process.Monitor)
}

type Waiting(conn) {
  Waiting(
    caller: Pid,
    monitor: process.Monitor,
    client: Subject(Result(conn, String)),
  )
}

pub opaque type Msg(conn) {
  Ping(subject: Subject(Msg(conn)), timeout: Int)
  CheckIn(conn: conn, caller: Pid)
  CheckOut(client: Subject(Result(conn, String)), caller: Pid)
  PoolExit(process.ExitMessage)
  CallerDown(process.Down)
  Shutdown(client: process.Subject(Result(Nil, String)))
}

pub fn checkout(
  pool: Subject(Msg(conn)),
  caller: Pid,
  timeout: Int,
) -> Result(conn, String) {
  process.call(pool, timeout, CheckOut(_, caller:))
}

pub fn checkin(pool: Subject(Msg(conn)), conn: conn, caller: Pid) -> Nil {
  process.send(pool, CheckIn(conn:, caller:))
}

pub fn shutdown(pool: Subject(Msg(conn)), timeout: Int) -> Result(Nil, String) {
  process.call(pool, timeout, Shutdown)
}

fn handle_message(
  state: State(conn),
  msg: Msg(conn),
) -> actor.Next(State(conn), Msg(conn)) {
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
  state: State(conn),
  subject: process.Subject(Msg(conn)),
  timeout: Int,
) -> actor.Next(State(conn), Msg(conn)) {
  state.idle
  |> list.each(fn(conn) {
    state.handle_ping(conn)

    process.send_after(subject, timeout, Ping(subject, timeout))
  })

  actor.continue(state)
}

// TODO: finish implementing queueing algorithm
// https://github.com/elixir-ecto/db_connection/blob/1b9783dd88b693dbcbde8a74a1a351b41e5ef5ef/lib/db_connection/connection_pool.ex
// https://queue.acm.org/appendices/codel.html
fn handle_checkin(
  state: State(conn),
  conn: conn,
  caller: Pid,
) -> actor.Next(State(conn), Msg(conn)) {
  let state =
    state
    |> handle_live_connection(Some(conn), caller)
    |> result.unwrap(state)

  actor.continue(state)
  |> actor.with_selector(state.selector)
}

// Validates `conn` against the `live` value.
// When there is `Some(conn)` value, it is compared against the
// relevant `live` value.
// After passing validation, deselect the monitor of the current `live`
// value.
// The next caller waiting for a connection is removed from the queue
// and given the connection.
// If the queue is empty the connection is returned to `idle`.
fn handle_live_connection(
  state: State(conn),
  conn: Option(conn),
  caller: Pid,
) -> Result(State(conn), Nil) {
  case dict.get(state.live, caller) {
    Error(_) -> Ok(state)
    Ok(live) -> {
      use current <- result.map(validate_conn(live, conn))

      state.queue
      |> dequeue(
        with: fn(waiting) {
          let conn = current.conn

          actor.send(waiting.client, Ok(conn))

          let live =
            state.live
            |> dict.delete(caller)
            |> dict.insert(
              waiting.caller,
              Live(conn:, monitor: waiting.monitor),
            )

          State(..state, live:)
        },
        or_else: fn() {
          let selector =
            process.deselect_specific_monitor(state.selector, current.monitor)

          let live =
            state.live
            |> dict.delete(caller)

          let idle = list.prepend(state.idle, current.conn)

          State(..state, selector:, idle:, live:)
        },
      )
    }
  }
}

fn validate_conn(
  live: Live(conn),
  conn: Option(conn),
) -> Result(Live(conn), Nil) {
  case conn {
    Some(conn) -> {
      use <- bool.guard({ conn == live.conn }, Ok(live))

      Error(Nil)
    }
    None -> Ok(live)
  }
}

fn handle_checkout(
  state: State(conn),
  client: process.Subject(Result(conn, String)),
  caller: Pid,
) -> actor.Next(State(conn), Msg(conn)) {
  let state =
    with_idle(state, handle_next_conn(_, state, client, caller))
    |> result.lazy_unwrap(fn() { handle_enqueue(state, client, caller) })

  actor.continue(state)
  |> actor.with_selector(state.selector)
}

fn handle_enqueue(
  state: State(conn),
  client: process.Subject(Result(conn, String)),
  caller: Pid,
) -> State(conn) {
  let monitor = process.monitor(caller)
  let selector =
    state.selector
    |> process.select_specific_monitor(monitor, CallerDown)

  let waiting = Waiting(caller:, monitor:, client:)

  enqueue(state.queue, waiting)

  State(..state, selector:)
}

fn handle_next_conn(
  conn: conn,
  state: State(conn),
  client: process.Subject(Result(conn, String)),
  caller: Pid,
) -> State(conn) {
  let monitor = process.monitor(caller)
  let selector =
    state.selector
    |> process.select_specific_monitor(monitor, CallerDown)

  actor.send(client, Ok(conn))

  let live =
    state.live
    |> dict.insert(caller, Live(conn:, monitor:))

  State(..state, live:, selector:)
}

fn handle_pool_exit(
  state: State(conn),
  exit: process.ExitMessage,
) -> actor.Next(State(conn), Msg(conn)) {
  state.idle
  |> list.each(state.handle_close)

  case exit.reason {
    process.Normal -> actor.stop()
    process.Killed -> actor.stop_abnormal("pool killed")
    process.Abnormal(_reason) -> actor.stop_abnormal("pool stopped abnormally")
  }
}

fn handle_caller_down(
  state: State(conn),
  down: process.Down,
) -> actor.Next(State(conn), Msg(conn)) {
  let assert process.ProcessDown(pid:, ..) = down

  let state =
    state
    |> handle_live_connection(None, pid)
    |> result.unwrap(state)

  actor.continue(state)
  |> actor.with_selector(state.selector)
}

fn handle_shutdown(
  state: State(conn),
  client: process.Subject(Result(Nil, String)),
) -> actor.Next(State(conn), Msg(conn)) {
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

// Queue

type Queue(a, b)

fn new_queue() -> Queue(Int, #(Int, Waiting(conn))) {
  let table_name = "pool_queue" <> int.to_string(unique_int())

  atom.create(table_name) |> ets_queue_
}

fn dequeue(
  queue: Queue(Int, #(Int, Waiting(conn))),
  with handler: fn(Waiting(conn)) -> t,
  or_else fallback: fn() -> t,
) -> t {
  case ets_first_(queue) {
    Some(key) -> {
      let #(_sent, #(_, waiting)) = key

      ets_delete_(queue, key)

      handler(waiting)
    }
    None -> fallback()
  }
}

fn enqueue(
  queue: Queue(Int, #(Int, Waiting(conn))),
  value: Waiting(conn),
) -> Queue(Int, #(Int, Waiting(conn))) {
  let _ = ets_insert_(queue, monotonic_time(), #(unique_int(), value))

  queue
}

@external(erlang, "db_pool_ffi", "ets_queue")
fn ets_queue_(name: Atom) -> Queue(a, b)

@external(erlang, "db_pool_ffi", "unique_int")
fn unique_int() -> Int

@external(erlang, "db_pool_ffi", "ets_queue_insert")
fn ets_insert_(queue: Queue(a, b), key: a, value: b) -> Result(Nil, Nil)

@external(erlang, "db_pool_ffi", "ets_first_lookup")
fn ets_first_(queue: Queue(a, b)) -> Option(#(a, b))

@external(erlang, "ets", "delete")
fn ets_delete_(queue: Queue(a, b), key: #(a, b)) -> Bool

@external(erlang, "erlang", "monotonic_time")
fn monotonic_time() -> Int
