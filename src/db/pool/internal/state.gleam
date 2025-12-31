import db/pool/internal
import db/pool/internal/queue.{type Queue}
import gleam/bool
import gleam/dict.{type Dict}
import gleam/erlang/process
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result

pub opaque type Waiting(conn, err) {
  Waiting(
    caller: process.Pid,
    monitor: process.Monitor,
    client: process.Subject(Result(conn, err)),
    timer: process.Timer,
  )
}

pub opaque type Active(conn) {
  Active(conn: conn, monitor: process.Monitor)
}

pub opaque type State(conn, msg, err) {
  State(
    selector: process.Selector(msg),
    // pool capacity
    max_size: Int,
    // current number of connections
    current_size: Int,
    // open connection
    handle_open: fn() -> Result(conn, err),
    // close connection
    handle_close: fn(conn) -> Result(Nil, err),
    // called on each connection every `idle_interval`
    handle_ping: fn(conn) -> Nil,
    // idle time between pings
    idle_interval: Int,
    // idle connections
    idle: List(conn),
    // connections in use
    active: Dict(process.Pid, Active(conn)),
    // processes waiting for a connection
    queue: Queue(Int, Waiting(conn, err)),
  )
}

pub fn new(selector: process.Selector(msg)) -> State(conn, msg, err) {
  State(
    selector:,
    max_size: 0,
    current_size: 0,
    handle_open: fn() { panic },
    handle_close: fn(_) { panic },
    handle_ping: fn(_) { Nil },
    idle_interval: 1000,
    idle: [],
    active: dict.new(),
    queue: queue.new("db_pool_queue"),
  )
}

pub fn with_selector(
  state: State(conn, msg, err),
  next: fn(process.Selector(msg)) -> t,
) -> t {
  next(state.selector)
}

pub fn selector(
  selector: process.Selector(msg),
  state: State(conn, msg, err),
) -> State(conn, msg, err) {
  State(..state, selector:)
}

pub fn max_size(
  state: State(conn, msg, err),
  max_size: Int,
) -> State(conn, msg, err) {
  State(..state, max_size:)
}

pub fn on_open(
  state: State(conn, msg, err),
  handle_open: fn() -> Result(conn, err),
) -> State(conn, msg, err) {
  State(..state, handle_open:)
}

pub fn on_close(
  state: State(conn, msg, err),
  handle_close: fn(conn) -> Result(Nil, err),
) -> State(conn, msg, err) {
  State(..state, handle_close:)
}

pub fn on_ping(
  state: State(conn, msg, err),
  handle_ping: fn(conn) -> Nil,
) -> State(conn, msg, err) {
  State(..state, handle_ping:)
}

pub fn current_size(
  state: State(conn, msg, err),
  current_size: Int,
) -> State(conn, msg, err) {
  State(..state, current_size:)
}

pub fn idle_interval(
  state: State(conn, msg, err),
  idle_interval: Int,
) -> State(conn, msg, err) {
  State(..state, idle_interval:)
}

pub fn idle(
  state: State(conn, msg, err),
  idle: List(conn),
) -> State(conn, msg, err) {
  State(..state, idle:)
}

pub fn active(
  state: State(conn, msg, err),
  active: Dict(process.Pid, Active(conn)),
) -> State(conn, msg, err) {
  State(..state, active:)
}

pub fn queue_size(state: State(conn, msg, err)) -> Int {
  queue.size(state.queue) |> result.unwrap(0)
}

pub fn active_size(state: State(conn, msg, err)) -> Int {
  dict.size(state.active)
}

fn with_active(
  state: State(conn, msg, err),
  caller: process.Pid,
  next: fn(Active(conn)) -> State(conn, msg, err),
) -> State(conn, msg, err) {
  dict.get(state.active, caller)
  |> result.map(next)
  |> result.unwrap(state)
}

pub fn dequeue(
  state: State(conn, msg, err),
  conn: Option(conn),
  caller: process.Pid,
  mapping: fn(process.Down) -> msg,
  handler: fn(process.Subject(Result(conn, err)), conn) -> Nil,
) -> State(conn, msg, err) {
  use prev <- with_active(state, caller)

  option.map(conn, fn(conn) {
    assert prev.conn == conn
  })

  {
    use #(time_sent, waiting) <- result.try(queue.first_lookup(state.queue))
    use _ <- result.map(queue.delete_key(state.queue, time_sent))

    let monitor = process.monitor(waiting.caller)
    let next = Active(conn: prev.conn, monitor:)

    let all_active =
      state.active
      |> dict.delete(caller)
      |> dict.insert(waiting.caller, next)

    process.cancel_timer(waiting.timer)
    process.demonitor_process(prev.monitor)

    let state =
      state.selector
      |> process.deselect_specific_monitor(prev.monitor)
      |> process.select_specific_monitor(next.monitor, mapping)
      |> selector(state)
      |> active(all_active)

    handler(waiting.client, prev.conn)

    state
  }
  |> result.lazy_unwrap(fn() {
    state.idle
    |> list.prepend(prev.conn)
    |> idle(state, _)
  })
}

pub fn with_connection(
  state: State(conn, msg, err),
  next: fn(Option(Result(conn, err))) -> State(conn, msg, err),
) -> State(conn, msg, err) {
  case state.idle {
    [] if state.current_size < state.max_size ->
      Some(state.handle_open()) |> next
    [] -> next(None)
    [conn, ..remaining] -> {
      Some(Ok(conn))
      |> next
      |> idle(remaining)
    }
  }
}

pub fn enqueue(
  state: State(conn, msg, err),
  caller: process.Pid,
  client: process.Subject(Result(conn, err)),
  timeout: Int,
  on_timeout handle_timeout: fn(Int, Int) -> msg,
  on_down handle_down: fn(process.Down) -> msg,
) -> State(conn, msg, err) {
  let now_in_ms = internal.now_in_ms()
  let subject = process.new_subject()
  let timer =
    process.send_after(subject, timeout, handle_timeout(now_in_ms, timeout))

  let monitor = process.monitor(caller)
  let waiting = Waiting(caller:, monitor:, client:, timer:)

  let assert Ok(Nil) = queue.insert(state.queue, now_in_ms, waiting)

  state.selector
  |> process.select(subject)
  |> process.select_specific_monitor(waiting.monitor, handle_down)
  |> selector(state)
}

pub fn claim(
  state: State(conn, msg, err),
  caller: process.Pid,
  conn: conn,
  mapping: fn(process.Down) -> msg,
) -> State(conn, msg, err) {
  let monitor = process.monitor(caller)
  let activated = Active(conn:, monitor:)
  let all_active = dict.insert(state.active, caller, activated)

  state.selector
  |> process.select_specific_monitor(activated.monitor, mapping)
  |> selector(state)
  |> active(all_active)
}

pub fn expire(
  state: State(conn, msg, err),
  sent: Int,
  timeout: Int,
  on_expiry next: fn(process.Subject(Result(conn, err))) -> Nil,
  or_else extend: fn(Int, Int) -> msg,
) -> State(conn, msg, err) {
  queue.lookup(state.queue, sent)
  |> result.map(fn(waiting) {
    let now = internal.now_in_ms()

    use <- bool.lazy_guard(when: { now < { sent + timeout } }, return: fn() {
      process.cancel_timer(waiting.timer)

      let subject = process.new_subject()
      let timer = process.send_after(subject, timeout, extend(sent, timeout))

      let assert Ok(Nil) =
        queue.insert(state.queue, sent, Waiting(..waiting, timer:))

      state.selector
      |> process.select(subject)
      |> selector(state)
    })

    let assert Ok(Nil) = queue.delete_key(state.queue, sent)

    next(waiting.client)

    process.cancel_timer(waiting.timer)
    process.demonitor_process(waiting.monitor)

    state.selector
    |> process.deselect_specific_monitor(waiting.monitor)
    |> selector(state)
  })
  |> result.unwrap(state)
}

pub fn shutdown(state: State(conn, msg, err)) -> Nil {
  case dict.size(state.active) {
    0 -> list.each(state.idle, state.handle_close)
    _ -> Nil
  }
}

pub fn ping(state: State(conn, msg, err), message: msg) -> State(conn, msg, err) {
  list.each(state.idle, state.handle_ping)

  let subject = process.new_subject()

  let _timer = process.send_after(subject, state.idle_interval, message)

  state.selector
  |> process.select(subject)
  |> selector(state)
}

pub fn close(state: State(conn, msg, err)) -> Nil {
  let assert Ok(Nil) = list.try_each(state.idle, state.handle_close)

  Nil
}
