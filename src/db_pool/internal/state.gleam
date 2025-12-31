import db_pool/internal
import db_pool/internal/queue.{type Queue}
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

pub opaque type Builder(conn, err) {
  Builder(
    // pool capacity
    max_size: Int,
    // open connection
    handle_open: fn() -> Result(conn, err),
    // close connection
    handle_close: fn(conn) -> Result(Nil, err),
    // called on each connection every `interval`
    handle_interval: fn(conn) -> Nil,
    // idle time between pings
    interval: Int,
  )
}

pub fn new() -> Builder(conn, err) {
  Builder(
    max_size: 1,
    handle_open: fn() { panic as "(db_pool) on_open not configured" },
    handle_close: fn(_) { Ok(Nil) },
    handle_interval: fn(_) { Nil },
    interval: 1000,
  )
}

pub fn max_size(state: Builder(conn, err), max_size: Int) -> Builder(conn, err) {
  Builder(..state, max_size:)
}

pub fn on_open(
  state: Builder(conn, err),
  handle_open: fn() -> Result(conn, err),
) -> Builder(conn, err) {
  Builder(..state, handle_open:)
}

pub fn on_close(
  state: Builder(conn, err),
  handle_close: fn(conn) -> Result(Nil, err),
) -> Builder(conn, err) {
  Builder(..state, handle_close:)
}

pub fn on_interval(
  state: Builder(conn, err),
  handle_interval: fn(conn) -> Nil,
) -> Builder(conn, err) {
  Builder(..state, handle_interval:)
}

pub fn interval(state: Builder(conn, err), interval: Int) -> Builder(conn, err) {
  Builder(..state, interval:)
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
    // called on each connection every `interval`
    handle_interval: fn(conn) -> Nil,
    // idle time between pings
    interval: Int,
    // idle connections
    idle: List(conn),
    // connections in use
    active: Dict(process.Pid, Active(conn)),
    // processes waiting for a connection
    queue: Queue(Int, Waiting(conn, err)),
  )
}

pub fn build(
  builder: Builder(conn, err),
  selector: process.Selector(msg),
) -> Result(State(conn, msg, err), String) {
  let connections = {
    list.repeat("", builder.max_size)
    |> list.try_map(fn(_) { builder.handle_open() })
    |> result.map_error(fn(_) { "(db_pool) Failed to open connections" })
  }

  use idle <- result.map(connections)

  State(
    selector:,
    max_size: builder.max_size,
    current_size: builder.max_size,
    handle_open: builder.handle_open,
    handle_close: builder.handle_close,
    handle_interval: builder.handle_interval,
    interval: builder.interval,
    idle:,
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

pub fn current_size(state: State(conn, msg, err)) -> Int {
  state.current_size
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

    let active =
      state.active
      |> dict.delete(caller)
      |> dict.insert(waiting.caller, next)

    process.cancel_timer(waiting.timer)
    process.demonitor_process(prev.monitor)

    let selector =
      state.selector
      |> process.deselect_specific_monitor(prev.monitor)
      |> process.select_specific_monitor(next.monitor, mapping)

    let state = State(..state, selector:, active:)

    handler(waiting.client, prev.conn)

    state
  }
  |> result.lazy_unwrap(fn() {
    let idle = list.prepend(state.idle, prev.conn)

    State(..state, idle:)
  })
}

pub fn current_connection(
  state: State(conn, msg, err),
  caller: process.Pid,
) -> Option(conn) {
  dict.get(state.active, caller)
  |> result.map(fn(active) { Some(active.conn) })
  |> result.unwrap(None)
}

pub fn next_connection(
  state: State(conn, msg, err),
  next: fn(Option(Result(conn, err))) -> State(conn, msg, err),
) -> State(conn, msg, err) {
  case state.idle {
    [] if state.current_size < state.max_size -> {
      let conn = state.handle_open()
      let state = next(Some(conn))

      case conn {
        Ok(_) -> State(..state, current_size: state.current_size + 1)
        _ -> state
      }
    }
    [] -> next(None)
    [conn, ..idle] -> {
      let state = Some(Ok(conn)) |> next

      State(..state, idle:)
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

  let selector =
    state.selector
    |> process.select(subject)
    |> process.select_specific_monitor(waiting.monitor, handle_down)

  State(..state, selector:)
}

pub fn claim(
  state: State(conn, msg, err),
  caller: process.Pid,
  conn: conn,
  mapping: fn(process.Down) -> msg,
) -> State(conn, msg, err) {
  let monitor = process.monitor(caller)
  let activated = Active(conn:, monitor:)
  let active = dict.insert(state.active, caller, activated)

  let selector =
    state.selector
    |> process.select_specific_monitor(activated.monitor, mapping)

  State(..state, selector:, active:)
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

      let selector = process.select(state.selector, subject)

      State(..state, selector:)
    })

    let assert Ok(Nil) = queue.delete_key(state.queue, sent)

    next(waiting.client)

    process.cancel_timer(waiting.timer)
    process.demonitor_process(waiting.monitor)

    let selector =
      process.deselect_specific_monitor(state.selector, waiting.monitor)

    State(..state, selector:)
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
  list.each(state.idle, state.handle_interval)

  let subject = process.new_subject()

  let _timer = process.send_after(subject, state.interval, message)

  let selector = process.select(state.selector, subject)

  State(..state, selector:)
}

pub fn close(state: State(conn, msg, err)) -> Nil {
  let assert Ok(Nil) = list.try_each(state.idle, state.handle_close)

  Nil
}
