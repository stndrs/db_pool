import gleam/bool
import gleam/dict.{type Dict}
import gleam/erlang/process
import gleam/list
import gleam/option.{type Option}
import gleam/result
import rasa
import rasa/counter
import rasa/queue.{type Queue}

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
    queue: Queue(Waiting(conn, err)),
    // monotonic time counter
    counter: counter.Counter,
    // maximum acceptable queue delay in ms
    queue_target: Int,
    // measurement interval in ms
    queue_interval: Int,
    // minimum delay observed in the current interval
    delay: Int,
    // whether the pool is in slow mode
    slow: Bool,
    // monotonic ms when the current interval ends
    next: Int,
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

  let counter = counter.monotonic(counter.Millisecond)

  let queue =
    queue.build("db_pool_queue")
    |> queue.with_access(rasa.Private)
    |> queue.new(counter)

  // This shouldn't ever return `Error`
  let assert Ok(now) = counter.next(counter)

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
    queue:,
    counter:,
    queue_target: builder.queue_target,
    queue_interval: builder.queue_interval,
    delay: 0,
    slow: False,
    next: now + builder.queue_interval,
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

pub fn is_slow(state: State(conn, msg, err)) -> Bool {
  state.slow
}

pub fn codel_delay(state: State(conn, msg, err)) -> Int {
  state.delay
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
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  use prev <- with_active(state, caller)

  option.map(conn, fn(conn) {
    assert prev.conn == conn
  })

  // Cancel the deadline timer for the returned connection
  let _ = process.cancel_timer(prev.deadline_timer)

  // This shouldn't ever return `Error`
  let assert Ok(now) = counter.next(state.counter)

  let active = dict.delete(state.active, caller)
  process.demonitor_process(prev.monitor)
  let selector = process.deselect_specific_monitor(state.selector, prev.monitor)
  let state = State(..state, selector:, active:)

  codel_dequeue(state, now, prev.conn, mapping, handler, drop, on_deadline)
}

/// Called when a deadline timer fires. Validates the checkout_time to guard
/// against stale timers, then closes the held connection and opens a
/// replacement. The replacement is fed through codel_dequeue to serve a
/// waiter or return to idle. If handle_open fails the pool shrinks by 1.
pub fn deadline_expired(
  state: State(conn, msg, err),
  caller: process.Pid,
  checkout_time: Int,
  mapping: fn(process.Down) -> msg,
  handler: fn(process.Subject(Result(conn, err)), conn) -> Nil,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  dict.get(state.active, caller)
  |> result.map(fn(active) {
    use <- bool.guard(
      when: active.checkout_time != checkout_time,
      return: state,
    )

    process.demonitor_process(active.monitor)
    let selector =
      process.deselect_specific_monitor(state.selector, active.monitor)

    let active_dict = dict.delete(state.active, caller)
    let state = State(..state, selector:, active: active_dict)

    let _ = state.handle_close(active.conn)

    case state.handle_open() {
      Ok(conn) -> {
        let assert Ok(now) = counter.next(state.counter)
        codel_dequeue(state, now, conn, mapping, handler, drop, on_deadline)
      }
      Error(_) -> State(..state, current_size: state.current_size - 1)
    }
  })
  |> result.unwrap(state)
}

/// dispatches to the appropriate strategy based on
/// whether we're at an interval boundary, in slow mode, or in fast mode.
fn codel_dequeue(
  state: State(conn, msg, err),
  now: Int,
  conn: conn,
  mapping: fn(process.Down) -> msg,
  handler: fn(process.Subject(Result(conn, err)), conn) -> Nil,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  case now >= state.next {
    // evaluate whether we should be slow or fast
    True -> dequeue_first(state, now, conn, mapping, handler, on_deadline)
    // Mid-interval
    False ->
      case state.slow {
        False -> dequeue_fast(state, now, conn, mapping, handler, on_deadline)
        True ->
          dequeue_slow(
            state,
            now,
            state.queue_target * 2,
            conn,
            mapping,
            handler,
            drop,
            on_deadline,
          )
      }
  }
}

/// Called at an interval boundary. Measures the delay of the first waiter,
/// sets slow mode based on whether the previous interval's min delay
/// exceeded the target, and resets the interval.
fn dequeue_first(
  state: State(conn, msg, err),
  now: Int,
  conn: conn,
  mapping: fn(process.Down) -> msg,
  handler: fn(process.Subject(Result(conn, err)), conn) -> Nil,
  on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  let next = now + state.queue_interval
  let slow = state.delay > state.queue_target

  case queue.first(state.queue) {
    Ok(#(sent, waiting)) -> {
      let assert Ok(Nil) = queue.delete(state.queue, sent)

      let delay = now - sent
      let state = State(..state, next:, delay:, slow:)

      let state =
        serve_waiter(state, waiting, conn, mapping, handler, on_deadline)

      // Track the minimum delay
      case delay < state.delay {
        True -> State(..state, delay:)
        False -> state
      }
    }
    _ -> {
      // No waiters so return connection to idle
      let idle = list.prepend(state.idle, conn)
      State(..state, next:, delay: 0, slow:, idle:)
    }
  }
}

/// serve the first waiter immediately, tracking minimum delay.
fn dequeue_fast(
  state: State(conn, msg, err),
  now: Int,
  conn: conn,
  mapping: fn(process.Down) -> msg,
  handler: fn(process.Subject(Result(conn, err)), conn) -> Nil,
  on_deadline: fn(process.Pid, Int) -> msg,
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
      serve_waiter(state, waiting, conn, mapping, handler, on_deadline)
    }
    _ -> {
      // No waiters, return connection to idle
      let idle = list.prepend(state.idle, conn)
      State(..state, idle:, delay: 0)
    }
  }
}

/// drop waiters that have been waiting longer than the timeout
/// threshold (target * 2), then serve the first valid waiter.
fn dequeue_slow(
  state: State(conn, msg, err),
  now: Int,
  timeout: Int,
  conn: conn,
  mapping: fn(process.Down) -> msg,
  handler: fn(process.Subject(Result(conn, err)), conn) -> Nil,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  case queue.first(state.queue) {
    Ok(#(sent, waiting)) if now - sent > timeout -> {
      // This waiter is too old. Drop it and continue
      let assert Ok(Nil) = queue.delete(state.queue, sent)

      drop(waiting.client)
      process.demonitor_process(waiting.monitor)

      let selector =
        process.deselect_specific_monitor(state.selector, waiting.monitor)

      State(..state, selector:)
      |> dequeue_slow(now, timeout, conn, mapping, handler, drop, on_deadline)
    }
    Ok(#(sent, waiting)) -> {
      // This waiter is fresh enough
      let assert Ok(Nil) = queue.delete(state.queue, sent)

      let delay = now - sent
      let state = case delay < state.delay {
        True -> State(..state, delay:)
        False -> state
      }

      serve_waiter(state, waiting, conn, mapping, handler, on_deadline)
    }
    _ -> {
      // No waiters, return connection to idle
      let idle = list.prepend(state.idle, conn)
      State(..state, idle:, delay: 0)
    }
  }
}

fn serve_waiter(
  state: State(conn, msg, err),
  waiting: Waiting(conn, err),
  conn: conn,
  mapping: fn(process.Down) -> msg,
  handler: fn(process.Subject(Result(conn, err)), conn) -> Nil,
  on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  let monitor = process.monitor(waiting.caller)

  // This shouldn't ever return `Error`
  let assert Ok(now) = counter.next(state.counter)

  let deadline_subject = process.new_subject()
  let deadline_timer =
    process.send_after(
      deadline_subject,
      waiting.deadline,
      on_deadline(waiting.caller, now),
    )

  let next = Active(conn:, monitor:, deadline_timer:, checkout_time: now)
  let active = dict.insert(state.active, waiting.caller, next)

  process.demonitor_process(waiting.monitor)

  let selector =
    state.selector
    |> process.deselect_specific_monitor(waiting.monitor)
    |> process.select_specific_monitor(next.monitor, mapping)
    |> process.select(deadline_subject)

  let state = State(..state, selector:, active:)

  handler(waiting.client, conn)

  state
}

pub fn checkout(
  state: State(conn, msg, err),
  caller: process.Pid,
  handle_down: fn(process.Down) -> msg,
  deadline: Int,
  on_deadline: fn(process.Pid, Int) -> msg,
  next: fn(conn) -> Nil,
) -> Result(State(conn, msg, err), Nil) {
  current_connection(state, caller)
  |> result.map(fn(conn) {
    next(conn)

    state
  })
  |> result.lazy_or(fn() {
    case state.idle {
      [] if state.current_size < state.max_size -> {
        state.handle_open()
        |> result.map(fn(conn) {
          next(conn)

          let monitor = process.monitor(caller)

          // This shouldn't ever return `Error`
          let assert Ok(now) = counter.next(state.counter)

          let deadline_subject = process.new_subject()
          let deadline_timer =
            process.send_after(
              deadline_subject,
              deadline,
              on_deadline(caller, now),
            )

          let activated =
            Active(conn:, monitor:, deadline_timer:, checkout_time: now)
          let active = dict.insert(state.active, caller, activated)

          let selector =
            state.selector
            |> process.select_specific_monitor(activated.monitor, handle_down)
            |> process.select(deadline_subject)

          State(
            ..state,
            current_size: state.current_size + 1,
            selector:,
            active:,
          )
        })
        |> result.replace_error(Nil)
      }
      [] -> Error(Nil)
      [conn, ..idle] -> {
        next(conn)

        let monitor = process.monitor(caller)

        // This shouldn't ever return `Error`
        let assert Ok(now) = counter.next(state.counter)

        let deadline_subject = process.new_subject()
        let deadline_timer =
          process.send_after(
            deadline_subject,
            deadline,
            on_deadline(caller, now),
          )

        let activated =
          Active(conn:, monitor:, deadline_timer:, checkout_time: now)
        let active = dict.insert(state.active, caller, activated)

        let selector =
          state.selector
          |> process.select_specific_monitor(activated.monitor, handle_down)
          |> process.select(deadline_subject)

        Ok(State(..state, idle:, selector:, active:))
      }
    }
  })
}

pub fn current_connection(
  state: State(conn, msg, err),
  caller: process.Pid,
) -> Result(conn, Nil) {
  dict.get(state.active, caller)
  |> result.map(fn(active) { active.conn })
}

pub fn enqueue(
  state: State(conn, msg, err),
  caller: process.Pid,
  client: process.Subject(Result(conn, err)),
  timeout: Int,
  deadline: Int,
  on_timeout handle_timeout: fn(Int, Int) -> msg,
  on_down handle_down: fn(process.Down) -> msg,
) -> State(conn, msg, err) {
  let monitor = process.monitor(caller)
  let waiting = Waiting(caller:, monitor:, client:, deadline:)

  let assert Ok(now_in_ms) = queue.push(state.queue, waiting)

  let subject = process.new_subject()
  let _timer =
    process.send_after(subject, timeout, handle_timeout(now_in_ms, timeout))

  let selector =
    state.selector
    |> process.select(subject)
    |> process.select_specific_monitor(waiting.monitor, handle_down)

  State(..state, selector:)
}

pub fn expire(
  state: State(conn, msg, err),
  sent: Int,
  timeout: Int,
  on_expiry next: fn(process.Subject(Result(conn, err))) -> Nil,
  or_else extend: fn(Int, Int) -> msg,
) -> State(conn, msg, err) {
  queue.at(state.queue, sent)
  |> result.try(fn(waiting) {
    use now <- result.map(counter.next(state.counter))

    use <- bool.lazy_guard(when: { now < { sent + timeout } }, return: fn() {
      let subject = process.new_subject()
      let _timer = process.send_after(subject, timeout, extend(sent, timeout))

      let selector = process.select(state.selector, subject)

      State(..state, selector:)
    })

    let assert Ok(Nil) = queue.delete(state.queue, sent)

    next(waiting.client)

    process.demonitor_process(waiting.monitor)

    let selector =
      process.deselect_specific_monitor(state.selector, waiting.monitor)

    State(..state, selector:)
  })
  |> result.unwrap(state)
}

/// Called periodically by the poll timer. Checks whether the queue has
/// made progress since the last poll. If the queue has stalled (oldest
/// entry hasn't changed), evaluates CoDel timeout logic to potentially
/// enter slow mode and drop stale waiters.
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

/// If we're at an interval boundary and the minimum delay exceeds the target,
/// enter slow mode and drop stale waiters.
fn codel_timeout(
  state: State(conn, msg, err),
  delay: Int,
  time: Int,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
) -> State(conn, msg, err) {
  case time >= state.next, state.delay > state.queue_target {
    // Interval boundary and delay exceeds target, enter slow mode
    True, True -> {
      State(..state, slow: True, delay:, next: time + state.queue_interval)
      |> poll_drop_slow(time, state.queue_target * 2, drop)
    }
    // Interval boundary and delay within target, exit slow mode
    True, False ->
      State(..state, slow: False, delay:, next: time + state.queue_interval)
    _, _ -> state
  }
}

/// Drops all waiters from the front of the queue whose delay exceeds
/// the given timeout threshold. Used by the poll timer when the pool
/// is in slow mode.
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

      let selector =
        process.deselect_specific_monitor(state.selector, waiting.monitor)

      State(..state, selector:)
      |> poll_drop_slow(now, timeout, drop)
    }
    _ -> state
  }
}

/// Schedules the next poll timer.
fn start_poll(
  state: State(conn, msg, err),
  now: Int,
  last_sent: Int,
  schedule_poll: fn(Int, Int) -> msg,
) -> State(conn, msg, err) {
  let poll_time = now + state.queue_interval
  let subject = process.new_subject()
  let _timer =
    process.send_after(
      subject,
      state.queue_interval,
      schedule_poll(poll_time, last_sent),
    )
  let selector = process.select(state.selector, subject)

  State(..state, selector:)
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
