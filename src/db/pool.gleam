import gleam/dict.{type Dict}
import gleam/erlang/atom.{type Atom}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result

pub opaque type Pool(conn, err) {
  Pool(
    size: Int,
    handle_open: fn() -> Result(conn, err),
    handle_close: fn(conn) -> Nil,
    handle_ping: fn(conn) -> Nil,
  )
}

pub fn new() -> Pool(conn, err) {
  let handle_open = fn() { panic as "Pool not configured" }
  let handle_close = fn(_) { Nil }
  let handle_ping = fn(_) { Nil }

  Pool(size: 5, handle_open:, handle_close:, handle_ping:)
}

pub fn size(pool: Pool(conn, err), size: Int) -> Pool(conn, err) {
  Pool(..pool, size:)
}

pub fn on_open(
  pool: Pool(conn, err),
  handle_open: fn() -> Result(conn, err),
) -> Pool(conn, err) {
  Pool(..pool, handle_open:)
}

pub fn on_close(
  pool: Pool(conn, err),
  handle_close: fn(conn) -> Nil,
) -> Pool(conn, err) {
  Pool(..pool, handle_close:)
}

pub fn on_ping(
  pool: Pool(conn, err),
  handle_ping: fn(conn) -> Nil,
) -> Pool(conn, err) {
  Pool(..pool, handle_ping:)
}

pub fn start(
  pool: Pool(conn, err),
  timeout: Int,
) -> Result(Subject(Msg(conn, err)), actor.StartError) {
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
  pool: Pool(conn, err),
  name: process.Name(Msg(conn, err)),
  timeout: Int,
) -> supervision.ChildSpecification(Subject(Msg(conn, err))) {
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
  self: Subject(Msg(conn, err)),
  pool: Pool(conn, err),
) -> Result(
  actor.Initialised(State(conn, err), Msg(conn, err), Subject(Msg(conn, err))),
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

type State(conn, err) {
  State(
    selector: process.Selector(Msg(conn, err)),
    max_size: Int,
    current_size: Int,
    handle_open: fn() -> Result(conn, err),
    handle_close: fn(conn) -> Nil,
    handle_ping: fn(conn) -> Nil,
    idle: List(conn),
    live: Dict(Pid, Live(conn)),
    queue: Queue(Int, #(Int, Waiting(conn, err))),
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

pub opaque type Msg(conn, err) {
  Ping(subject: Subject(Msg(conn, err)), timeout: Int)
  CheckIn(conn: conn, caller: Pid)
  CheckOut(client: Subject(Result(conn, err)), caller: Pid)
  PoolExit(process.ExitMessage)
  CallerDown(process.Down)
  Shutdown(client: process.Subject(Result(Nil, err)))
}

pub fn checkout(
  pool: Subject(Msg(conn, err)),
  caller: Pid,
  timeout: Int,
) -> Result(conn, err) {
  process.call(pool, timeout, CheckOut(_, caller:))
}

pub fn checkin(pool: Subject(Msg(conn, err)), conn: conn, caller: Pid) -> Nil {
  process.send(pool, CheckIn(conn:, caller:))
}

pub fn shutdown(pool: Subject(Msg(conn, err)), timeout: Int) -> Result(Nil, err) {
  process.call(pool, timeout, Shutdown)
}

fn handle_message(
  state: State(conn, err),
  msg: Msg(conn, err),
) -> actor.Next(State(conn, err), Msg(conn, err)) {
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
  subject: process.Subject(Msg(conn, err)),
  timeout: Int,
) -> actor.Next(State(conn, err), Msg(conn, err)) {
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
  state: State(conn, err),
  checked_out: conn,
  caller: Pid,
) -> actor.Next(State(conn, err), Msg(conn, err)) {
  let state = case dict.get(state.live, caller) {
    Error(_) -> state
    Ok(Live(conn:, monitor:)) -> {
      case checked_out == conn {
        True -> Nil
        False ->
          panic as "Checked out connection not associated with caller Pid"
      }

      process.demonitor_process(monitor)

      let selector = process.deselect_specific_monitor(state.selector, monitor)

      let live = dict.delete(state.live, caller)

      case dequeue(state.queue) {
        Some(Waiting(caller:, monitor:, client:)) -> {
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
) -> actor.Next(State(conn, err), Msg(conn, err)) {
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
      enqueue(state.queue, Waiting(caller:, monitor:, client:))

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
) -> actor.Next(State(conn, err), Msg(conn, err)) {
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
) -> actor.Next(State(conn, err), Msg(conn, err)) {
  let assert process.ProcessDown(pid:, ..) = down

  let state = case dict.get(state.live, pid) {
    Error(_) -> state
    Ok(Live(conn:, monitor:)) -> {
      process.demonitor_process(monitor)

      let selector = process.deselect_specific_monitor(state.selector, monitor)

      let live = dict.delete(state.live, pid)

      case dequeue(state.queue) {
        Some(Waiting(caller:, monitor:, client:)) -> {
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
) -> actor.Next(State(conn, err), Msg(conn, err)) {
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

fn new_queue() -> Queue(Int, #(Int, Waiting(conn, err))) {
  let table_name = "pool_queue" <> int.to_string(unique_int())

  atom.create(table_name) |> ets_queue_
}

fn dequeue(
  queue: Queue(Int, #(Int, Waiting(conn, err))),
) -> Option(Waiting(conn, err)) {
  case ets_first_(queue) {
    Some(key) -> {
      let #(_sent, #(_, waiting)) = key

      ets_delete_(queue, key)

      Some(waiting)
    }
    None -> None
  }
}

fn enqueue(
  queue: Queue(Int, #(Int, Waiting(conn, err))),
  value: Waiting(conn, err),
) -> Queue(Int, #(Int, Waiting(conn, err))) {
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
