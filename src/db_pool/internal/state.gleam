import gleam/bool
import gleam/dict.{type Dict}
import gleam/erlang/process
import gleam/list
import gleam/result
import rasa/counter
import rasa/monotonic
import rasa/queue.{type Queue}
import rasa/table

// --- Constants ---

const ns_per_ms = 1_000_000

pub opaque type Waiting(conn, err) {
  Waiting(
    caller: process.Pid,
    monitor: process.Monitor,
    client: process.Subject(Result(conn, err)),
    deadline: Int,
  )
}

pub opaque type Active(conn) {
  Active(
    conn: conn,
    monitor: process.Monitor,
    deadline_timer: process.Timer,
    checkout_time: Int,
  )
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
    // maximum acceptable queue delay in ms
    queue_target: Int,
    // measurement interval in ms
    queue_interval: Int,
  )
}

pub fn new() -> Builder(conn, err) {
  Builder(
    max_size: 1,
    handle_open: fn() { panic as "(db_pool) on_open not configured" },
    handle_close: fn(_) { Ok(Nil) },
    handle_interval: fn(_) { Nil },
    interval: 1000,
    queue_target: 50,
    queue_interval: 1000,
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

pub fn queue_target(
  state: Builder(conn, err),
  queue_target: Int,
) -> Builder(conn, err) {
  Builder(..state, queue_target:)
}

pub fn queue_interval(
  state: Builder(conn, err),
  queue_interval: Int,
) -> Builder(conn, err) {
  Builder(..state, queue_interval:)
}

pub opaque type State(conn, msg, err) {
  State(
    self: process.Subject(msg),
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
    // connections in use: caller pid -> active record
    active: Dict(process.Pid, Active(conn)),
    // processes waiting for a connection
    queue: Queue(Waiting(conn, err)),
    // monotonic time counter
    counter: counter.Counter,
    // maximum acceptable queue delay in ns
    queue_target: Int,
    // measurement interval in ns
    queue_interval: Int,
    // minimum delay observed in the current interval
    delay: Int,
    // whether the pool is in slow mode
    slow: Bool,
    // monotonic ns when the current interval ends
    next: Int,
  )
}

pub fn build(
  builder: Builder(conn, err),
  self: process.Subject(msg),
) -> Result(State(conn, msg, err), String) {
  let connections = {
    list.repeat("", builder.max_size)
    |> list.try_map(fn(_) { builder.handle_open() })
    |> result.map_error(fn(_) { "(db_pool) Failed to open connections" })
  }

  use conns <- result.map(connections)

  let counter = counter.monotonic(monotonic.Nanosecond)

  let queue = queue.new(counter, table.Private)

  let now = counter.next(counter)

  State(
    self:,
    max_size: builder.max_size,
    current_size: builder.max_size,
    handle_open: builder.handle_open,
    handle_close: builder.handle_close,
    handle_interval: builder.handle_interval,
    interval: builder.interval,
    idle: conns,
    active: dict.new(),
    queue:,
    counter:,
    queue_target: builder.queue_target * ns_per_ms,
    queue_interval: builder.queue_interval * ns_per_ms,
    delay: 0,
    slow: False,
    next: now + builder.queue_interval * ns_per_ms,
  )
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

pub fn is_slow(state: State(conn, msg, err)) -> Bool {
  state.slow
}

pub fn codel_delay(state: State(conn, msg, err)) -> Int {
  state.delay
}

/// Try to check out a connection. Returns Ok(state) if served
/// (re-entrant checkout or idle conn available), Error(Nil) if the
/// caller should be enqueued.
pub fn checkout(
  state: State(conn, msg, err),
  caller: process.Pid,
  deadline: Int,
  on_deadline: fn(process.Pid, Int) -> msg,
  next: fn(conn) -> Nil,
) -> Result(State(conn, msg, err), Nil) {
  // Check if caller already has a connection (re-entrant checkout)
  case dict.get(state.active, caller) {
    Ok(active) -> {
      next(active.conn)
      Ok(state)
    }
    Error(_) -> {
      // Try to pop an idle connection
      case state.idle {
        [conn, ..rest] -> {
          next(conn)

          let monitor = process.monitor(caller)
          let now = counter.next(state.counter)

          let deadline_timer =
            process.send_after(state.self, deadline, on_deadline(caller, now))

          let activated =
            Active(conn:, monitor:, deadline_timer:, checkout_time: now)
          let active = dict.insert(state.active, caller, activated)

          Ok(State(..state, idle: rest, active:))
        }
        [] -> Error(Nil)
      }
    }
  }
}

pub fn current_connection(
  state: State(conn, msg, err),
  caller: process.Pid,
) -> Result(conn, Nil) {
  dict.get(state.active, caller)
  |> result.map(fn(active) { active.conn })
}

/// Called when a client returns a connection to the pool.
/// Cleans up monitoring/deadline, then either serves a waiter
/// via CoDel or returns the connection to idle.
pub fn checkin(
  state: State(conn, msg, err),
  caller: process.Pid,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  case dict.get(state.active, caller) {
    Ok(prev) -> {
      let _ = process.cancel_timer(prev.deadline_timer)
      process.demonitor_process(prev.monitor)
      let active = dict.delete(state.active, caller)
      let state = State(..state, active:)

      let now = counter.next(state.counter)
      codel_dequeue(state, now, prev.conn, drop, on_deadline)
    }
    Error(_) -> {
      // No active record found — stale checkin, ignore.
      state
    }
  }
}

pub fn enqueue(
  state: State(conn, msg, err),
  caller: process.Pid,
  client: process.Subject(Result(conn, err)),
  timeout: Int,
  deadline: Int,
  on_timeout handle_timeout: fn(Int, Int) -> msg,
) -> State(conn, msg, err) {
  let monitor = process.monitor(caller)
  let waiting = Waiting(caller:, monitor:, client:, deadline:)

  let assert Ok(now_in_ms) = queue.push(state.queue, waiting)

  let _timer =
    process.send_after(state.self, timeout, handle_timeout(now_in_ms, timeout))

  state
}

pub fn expire(
  state: State(conn, msg, err),
  sent: Int,
  timeout: Int,
  on_expiry next: fn(process.Subject(Result(conn, err))) -> Nil,
  or_else extend: fn(Int, Int) -> msg,
) -> State(conn, msg, err) {
  queue.at(state.queue, sent)
  |> result.map(fn(waiting) {
    let now = counter.next(state.counter)

    use <- bool.lazy_guard(
      when: { now < { sent + timeout * ns_per_ms } },
      return: fn() {
        let _timer =
          process.send_after(state.self, timeout, extend(sent, timeout))

        state
      },
    )

    let assert Ok(Nil) = queue.delete(state.queue, sent)

    next(waiting.client)

    process.demonitor_process(waiting.monitor)

    state
  })
  |> result.unwrap(state)
}

/// Called when a caller process dies while holding a connection or waiting.
pub fn caller_down(
  state: State(conn, msg, err),
  pid: process.Pid,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  case dict.get(state.active, pid) {
    Ok(prev) -> {
      let _ = process.cancel_timer(prev.deadline_timer)
      process.demonitor_process(prev.monitor)
      let active = dict.delete(state.active, pid)
      let state = State(..state, active:)

      // Client crashed without checking in. Close old conn and replace.
      let _ = state.handle_close(prev.conn)

      case state.handle_open() {
        Ok(conn) -> {
          let now = counter.next(state.counter)
          codel_dequeue(state, now, conn, drop, on_deadline)
        }
        Error(_) -> State(..state, current_size: state.current_size - 1)
      }
    }
    Error(_) -> {
      // Not in active — this was likely a waiter whose monitor fired.
      // The waiter queue cleanup happens naturally when we try to serve them.
      state
    }
  }
}

/// Called when a deadline timer fires. Validates the checkout_time to guard
/// against stale timers, then closes the held connection and opens a
/// replacement. The replacement is fed through codel_dequeue to serve a
/// waiter or return to idle. If handle_open fails the pool shrinks by 1.
pub fn deadline_expired(
  state: State(conn, msg, err),
  caller: process.Pid,
  checkout_time: Int,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  dict.get(state.active, caller)
  |> result.map(fn(active) {
    use <- bool.guard(
      when: active.checkout_time != checkout_time,
      return: state,
    )

    // Deadline expired — caller still holds the connection.
    process.demonitor_process(active.monitor)

    let active_dict = dict.delete(state.active, caller)
    let state = State(..state, active: active_dict)

    // Close old connection
    let _ = state.handle_close(active.conn)

    case state.handle_open() {
      Ok(conn) -> {
        let now = counter.next(state.counter)
        codel_dequeue(state, now, conn, drop, on_deadline)
      }
      Error(_) -> State(..state, current_size: state.current_size - 1)
    }
  })
  |> result.unwrap(state)
}

// dispatches to the appropriate strategy based on
// whether we're at an interval boundary, in slow mode, or in fast mode.
fn codel_dequeue(
  state: State(conn, msg, err),
  now: Int,
  conn: conn,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  case now >= state.next {
    // evaluate whether we should be slow or fast
    True -> dequeue_first(state, now, conn, drop, on_deadline)
    // Mid-interval
    False ->
      case state.slow {
        False -> dequeue_fast(state, now, conn, drop, on_deadline)
        True ->
          dequeue_slow(
            state,
            now,
            state.queue_target * 2,
            conn,
            drop,
            on_deadline,
          )
      }
  }
}

// Called at an interval boundary.
fn dequeue_first(
  state: State(conn, msg, err),
  now: Int,
  conn: conn,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  let next = now + state.queue_interval
  let slow = state.delay > state.queue_target

  case queue.first(state.queue) {
    Ok(#(sent, waiting)) -> {
      let assert Ok(Nil) = queue.delete(state.queue, sent)

      let delay = now - sent
      let state = State(..state, next:, delay:, slow:)

      serve_waiter(state, waiting, conn, drop, on_deadline)
    }
    _ -> {
      // No waiters so return conn to idle
      let state = State(..state, idle: [conn, ..state.idle])
      State(..state, next:, delay: 0, slow:)
    }
  }
}

// serve the first waiter immediately, tracking minimum delay.
fn dequeue_fast(
  state: State(conn, msg, err),
  now: Int,
  conn: conn,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  case queue.first(state.queue) {
    Ok(#(sent, waiting)) -> {
      let assert Ok(Nil) = queue.delete(state.queue, sent)
      let delay = now - sent
      // Update min delay if this is smaller
      let state = case delay < state.delay {
        True -> State(..state, delay:)
        False -> state
      }
      serve_waiter(state, waiting, conn, drop, on_deadline)
    }
    _ -> {
      // No waiters, return conn to idle
      State(..state, idle: [conn, ..state.idle])
    }
  }
}

// drop waiters that have been waiting longer than the timeout
// threshold (target * 2), then serve the first valid waiter.
fn dequeue_slow(
  state: State(conn, msg, err),
  now: Int,
  timeout: Int,
  conn: conn,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  case queue.first(state.queue) {
    Ok(#(sent, waiting)) if now - sent > timeout -> {
      // This waiter is too old. Drop it and continue
      let assert Ok(Nil) = queue.delete(state.queue, sent)

      drop(waiting.client)
      process.demonitor_process(waiting.monitor)

      state
      |> dequeue_slow(now, timeout, conn, drop, on_deadline)
    }
    Ok(#(sent, waiting)) -> {
      // This waiter is fresh enough
      let assert Ok(Nil) = queue.delete(state.queue, sent)

      let delay = now - sent
      let state = case delay < state.delay {
        True -> State(..state, delay:)
        False -> state
      }

      serve_waiter(state, waiting, conn, drop, on_deadline)
    }
    _ -> {
      // No waiters, return conn to idle
      State(..state, idle: [conn, ..state.idle])
    }
  }
}

fn serve_waiter(
  state: State(conn, msg, err),
  waiting: Waiting(conn, err),
  conn: conn,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  // Check if the waiter is still alive before committing to the handoff.
  case process.is_alive(waiting.caller) {
    False -> {
      process.demonitor_process(waiting.monitor)

      let now = counter.next(state.counter)
      codel_dequeue(state, now, conn, drop, on_deadline)
    }
    True -> {
      // Send the connection to the waiter's client subject
      process.send(waiting.client, Ok(conn))

      let now = counter.next(state.counter)

      // Set up monitoring and deadline for the checkout
      let monitor = process.monitor(waiting.caller)

      let deadline_timer =
        process.send_after(
          state.self,
          waiting.deadline,
          on_deadline(waiting.caller, now),
        )

      let activated =
        Active(conn:, monitor:, deadline_timer:, checkout_time: now)
      let active = dict.insert(state.active, waiting.caller, activated)

      process.demonitor_process(waiting.monitor)

      State(..state, active:)
    }
  }
}

/// Called periodically by the poll timer.
pub fn poll(
  state: State(conn, msg, err),
  time: Int,
  last_sent: Int,
  on_poll schedule_poll: fn(Int, Int) -> msg,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
) -> State(conn, msg, err) {
  case queue.first(state.queue) {
    // oldest entry is the same as or older than the last poll
    Ok(#(sent, _)) if sent <= last_sent -> {
      let delay = time - sent

      state
      |> codel_timeout(delay, time, drop)
      |> start_poll(time, sent, schedule_poll)
    }
    // Queue is making progress
    Ok(#(sent, _)) -> start_poll(state, time, sent, schedule_poll)
    // Queue is empty
    _ -> start_poll(state, time, time, schedule_poll)
  }
}

// If we're at an interval boundary and the minimum delay exceeds the target,
// enter slow mode and drop stale waiters.
fn codel_timeout(
  state: State(conn, msg, err),
  delay: Int,
  time: Int,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
) -> State(conn, msg, err) {
  case time >= state.next, state.delay > state.queue_target {
    True, True -> {
      State(..state, slow: True, delay:, next: time + state.queue_interval)
      |> poll_drop_slow(time, state.queue_target * 2, drop)
    }
    True, False ->
      State(..state, slow: False, delay:, next: time + state.queue_interval)
    _, _ -> state
  }
}

fn poll_drop_slow(
  state: State(conn, msg, err),
  now: Int,
  timeout: Int,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
) -> State(conn, msg, err) {
  case queue.first(state.queue) {
    Ok(#(sent, waiting)) if now - sent > timeout -> {
      let assert Ok(Nil) = queue.delete(state.queue, sent)

      drop(waiting.client)
      process.demonitor_process(waiting.monitor)

      state
      |> poll_drop_slow(now, timeout, drop)
    }
    _ -> state
  }
}

fn start_poll(
  state: State(conn, msg, err),
  now: Int,
  last_sent: Int,
  schedule_poll: fn(Int, Int) -> msg,
) -> State(conn, msg, err) {
  let poll_time = now + state.queue_interval
  let _timer =
    process.send_after(
      state.self,
      state.queue_interval / ns_per_ms,
      schedule_poll(poll_time, last_sent),
    )

  state
}

pub fn shutdown(state: State(conn, msg, err)) -> Nil {
  case dict.size(state.active) {
    0 -> close_idle(state)
    _ -> Nil
  }
}

pub fn ping(state: State(conn, msg, err), message: msg) -> State(conn, msg, err) {
  list.each(state.idle, fn(conn) { state.handle_interval(conn) })

  let _timer = process.send_after(state.self, state.interval, message)

  state
}

pub fn close(state: State(conn, msg, err)) -> Nil {
  close_idle(state)
}

fn close_idle(state: State(conn, msg, err)) -> Nil {
  list.each(state.idle, fn(conn) {
    let _ = state.handle_close(conn)
    Nil
  })
}
