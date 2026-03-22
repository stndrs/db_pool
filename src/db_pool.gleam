import gleam/bool
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import rasa/counter
import rasa/monotonic
import rasa/queue.{type Queue}
import rasa/table

const ns_per_ms = 1_000_000

const reconnect_min_ms = 1000

const reconnect_max_ms = 30_000

/// Errors that can occur when interacting with the pool.
///
/// - `ConnectionError(err)` wraps an error returned by the `on_open` or
///   `on_close` callback.
/// - `ConnectionTimeout` is returned when a checkout request times out
///   while waiting in the queue.
/// - `ConnectionUnavailable` is returned when the CoDel algorithm drops
///   a request due to sustained overload, or when the pool shuts down
///   while callers are waiting.
pub type PoolError(err) {
  ConnectionError(err)
  ConnectionTimeout
  ConnectionUnavailable
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
    queue_target: Int,
    queue_interval: Int,
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

  Pool(
    size: 5,
    interval: 1000,
    queue_target: 50,
    queue_interval: 1000,
    handle_open:,
    handle_close:,
    handle_interval:,
  )
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

/// Sets the CoDel queue target in milliseconds. This is the maximum
/// acceptable queue delay before the pool considers itself overloaded.
/// Defaults to 50ms.
pub fn queue_target(pool: Pool(conn, err), target: Int) -> Pool(conn, err) {
  Pool(..pool, queue_target: target)
}

/// Sets the CoDel queue interval in milliseconds. This is the length
/// of each CoDel measurement interval. The pool evaluates queue health
/// at each interval boundary. Defaults to 1000ms.
pub fn queue_interval(pool: Pool(conn, err), interval: Int) -> Pool(conn, err) {
  Pool(..pool, queue_interval: interval)
}

// --- Internal types ---

type Waiting(conn, err) {
  Waiting(
    caller: Pid,
    monitor: process.Monitor,
    client: Subject(Result(conn, err)),
    deadline: Int,
  )
}

type Active(conn) {
  Active(
    conn: conn,
    monitor: process.Monitor,
    deadline_timer: process.Timer,
    checkout_time: Int,
  )
}

type State(conn, err) {
  State(
    self: Subject(Message(conn, err)),
    max_size: Int,
    current_size: Int,
    handle_open: fn() -> Result(conn, PoolError(err)),
    handle_close: fn(conn) -> Result(Nil, PoolError(err)),
    handle_interval: fn(conn) -> Nil,
    interval: Int,
    idle: List(conn),
    active: Dict(Pid, Active(conn)),
    queue: Queue(Waiting(conn, PoolError(err))),
    counter: counter.Counter,
    queue_target: Int,
    queue_interval: Int,
    delay: Int,
    slow: Bool,
    next: Int,
  )
}

/// Starts a connection pool and registers it under `name`. All
/// configured connections are opened eagerly during initialisation.
///
/// The `timeout` parameter is the maximum time in milliseconds allowed
/// for the actor to initialise (open all connections). It is **not**
/// a checkout timeout.
///
/// The pool actor traps exits so it can perform cleanup when its
/// parent or linked processes terminate.
pub fn start(
  pool: Pool(conn, err),
  name: process.Name(Message(conn, err)),
  timeout: Int,
) -> Result(Subject(Message(conn, err)), actor.StartError) {
  let counter = counter.monotonic_time(monotonic.Nanosecond)

  actor.new_with_initialiser(timeout, initialise_pool(_, pool, counter))
  |> actor.on_message(handle_message)
  |> actor.named(name)
  |> actor.start
  |> result.map(fn(started) {
    let time = counter.next(counter)

    let _timer = process.send_after(started.data, pool.interval, Interval)
    let _poll_timer =
      process.send_after(
        started.data,
        pool.queue_interval,
        Poll(time:, last_sent: time),
      )

    started.data
  })
}

/// Creates a `supervision.ChildSpecification` so the pool can be
/// added to an application's supervision tree.
///
/// The `timeout` parameter is used for both the actor initialisation
/// timeout and the supervisor's shutdown timeout. The restart strategy
/// is set to `Transient` — the pool is restarted only if it terminates
/// abnormally.
pub fn supervised(
  pool: Pool(conn, err),
  name: process.Name(Message(conn, err)),
  timeout: Int,
) -> supervision.ChildSpecification(Subject(Message(conn, err))) {
  supervision.worker(fn() {
    start(pool, name, timeout)
    |> result.map(fn(subject) {
      let assert Ok(pid) = process.subject_owner(subject)
      actor.Started(pid:, data: subject)
    })
  })
  |> supervision.timeout(timeout)
  |> supervision.restart(supervision.Transient)
}

pub opaque type Message(conn, err) {
  Interval
  CheckOut(
    client: Subject(Result(conn, PoolError(err))),
    caller: Pid,
    timeout: Int,
    deadline: Int,
  )
  CheckIn(caller: Pid, conn: conn)
  Timeout(time_sent: Int, timeout: Int)
  DeadlineExpired(caller: Pid, checkout_time: Int)
  Poll(time: Int, last_sent: Int)
  PoolExit(process.ExitMessage)
  CallerDown(process.Down)
  Reconnect(backoff: Int)
  Shutdown(client: Subject(Result(Nil, PoolError(err))))
}

/// Checks out a connection from the pool.
///
/// The `caller` should be `process.self()` of the calling process. The
/// pool monitors this process and reclaims the connection if it crashes.
///
/// If a connection is available it is returned immediately. If all
/// connections are in use the caller is added to a FIFO queue and will
/// receive a connection when one becomes available, or a
/// `ConnectionTimeout` error after `timeout` milliseconds.
///
/// The `deadline` parameter sets the maximum time in milliseconds that
/// the connection may be held. If the caller has not checked in by then,
/// the pool forcibly closes the connection, replaces it, and the caller
/// is left holding a now-closed connection.
///
/// Re-entrant: calling `checkout` again from the same process returns
/// the already checked-out connection. The original deadline is
/// preserved — a second checkout cannot extend it.
///
/// Note: this function uses `call_forever` internally. The `timeout` is
/// enforced by the pool actor, not on the client side. If the pool
/// actor is unreachable the caller will block indefinitely.
pub fn checkout(
  pool: Subject(Message(conn, err)),
  caller: Pid,
  timeout: Int,
  deadline: Int,
) -> Result(conn, PoolError(err)) {
  process.call_forever(pool, CheckOut(_, caller:, timeout:, deadline:))
}

/// Returns a connection back to the pool.
///
/// The `conn` value must be the same connection that was originally
/// checked out, and `caller` should be the `Pid` that checked it out.
/// If the caller has no active connection the checkin is silently
/// ignored.
pub fn checkin(
  pool: Subject(Message(conn, err)),
  conn: conn,
  caller: Pid,
) -> Nil {
  process.send(pool, CheckIn(caller:, conn:))
}

/// Shuts down the pool gracefully within `timeout` milliseconds.
///
/// All waiting callers in the queue are drained and sent a
/// `ConnectionUnavailable` error. Active (checked-out) connections
/// are closed, their deadline timers cancelled, and their monitors
/// removed. Idle connections are then closed via the configured
/// `on_close` callback.
pub fn shutdown(
  pool: Subject(Message(conn, err)),
  timeout: Int,
) -> Result(Nil, PoolError(err)) {
  process.call(pool, timeout, Shutdown)
}

// --- Actor initialisation ---

fn initialise_pool(
  self: Subject(Message(conn, err)),
  pool: Pool(conn, err),
  counter: counter.Counter,
) -> Result(
  actor.Initialised(
    State(conn, err),
    Message(conn, err),
    Subject(Message(conn, err)),
  ),
  String,
) {
  process.trap_exits(True)

  let selector =
    process.new_selector()
    |> process.select(self)
    |> process.select_trapped_exits(PoolExit)
    |> process.select_monitors(CallerDown)

  let connections =
    list.repeat("", pool.size)
    |> list.try_map(fn(_) { pool.handle_open() })
    |> result.map_error(fn(_) { "(db_pool) Failed to open connections" })

  use conns <- result.map(connections)

  let q =
    queue.new()
    |> queue.with_access(table.Private)
    |> queue.with_counter(counter)
    |> queue.build

  let now = counter.next(counter)

  let state =
    State(
      self:,
      max_size: pool.size,
      current_size: pool.size,
      handle_open: pool.handle_open,
      handle_close: pool.handle_close,
      handle_interval: pool.handle_interval,
      interval: pool.interval,
      idle: conns,
      active: dict.new(),
      queue: q,
      counter:,
      queue_target: pool.queue_target * ns_per_ms,
      queue_interval: pool.queue_interval * ns_per_ms,
      delay: 0,
      slow: False,
      next: now + pool.queue_interval * ns_per_ms,
    )

  actor.initialised(state)
  |> actor.selecting(selector)
  |> actor.returning(self)
}

// --- Message handler ---

fn handle_message(
  state: State(conn, err),
  msg: Message(conn, err),
) -> actor.Next(State(conn, err), Message(conn, err)) {
  case msg {
    Interval -> {
      // Spawn unlinked processes so health checks don't block the actor
      // mailbox and a crashing callback doesn't take down the pool.
      list.each(state.idle, fn(conn) {
        process.spawn_unlinked(fn() { state.handle_interval(conn) })
      })
      let _timer = process.send_after(state.self, state.interval, Interval)
      actor.continue(state)
    }
    CheckIn(caller:, conn:) -> {
      let state = do_checkin(state, caller, conn)
      actor.continue(state)
    }
    CheckOut(client:, caller:, timeout:, deadline:) -> {
      let state = {
        do_checkout(state, caller, client, deadline)
        |> result.lazy_unwrap(fn() {
          do_enqueue(state, caller, client, timeout, deadline)
        })
      }
      actor.continue(state)
    }
    Timeout(time_sent:, timeout:) -> {
      let state = do_expire(state, time_sent, timeout)
      actor.continue(state)
    }
    DeadlineExpired(caller:, checkout_time:) -> {
      state
      |> do_deadline_expired(caller, checkout_time)
      |> actor.continue
    }
    CallerDown(down) -> {
      let assert process.ProcessDown(pid:, ..) = down

      state
      |> do_caller_down(pid)
      |> actor.continue
    }
    Poll(time:, last_sent:) -> {
      state
      |> do_poll(time, last_sent)
      |> actor.continue
    }
    Reconnect(backoff:) -> {
      state
      |> do_reconnect(backoff)
      |> actor.continue
    }
    // Note: Interval, Poll, Reconnect, and deadline timers are not explicitly
    // cancelled on exit/shutdown. Their messages are silently dropped once the
    // actor stops and the subject becomes unreachable.
    PoolExit(exit) -> {
      drain_queue(state)
      let _ = close_active(state)
      close_idle(state)
      case exit.reason {
        process.Normal -> actor.stop()
        process.Killed -> actor.stop_abnormal("pool killed")
        process.Abnormal(_reason) ->
          actor.stop_abnormal("pool stopped abnormally")
      }
    }
    Shutdown(client:) -> {
      drain_queue(state)
      let _ = close_active(state)
      close_idle(state)
      actor.send(client, Ok(Nil))
      actor.stop()
    }
  }
}

/// Try to check out a connection. Returns Ok(state) if served
/// (re-entrant checkout or idle conn available), Error(Nil) if the
/// caller should be enqueued.
fn do_checkout(
  state: State(conn, err),
  caller: Pid,
  client: Subject(Result(conn, PoolError(err))),
  deadline: Int,
) -> Result(State(conn, err), Nil) {
  case dict.get(state.active, caller) {
    Ok(active) -> {
      // Subsequent checkouts: return the same connection. The original
      // deadline is preserved as callers cannot extend their deadline
      // by checking out again. A single process is limited to one
      // connection at a time.
      actor.send(client, Ok(active.conn))
      Ok(state)
    }
    _ -> {
      case state.idle {
        [conn, ..rest] -> {
          actor.send(client, Ok(conn))

          let monitor = process.monitor(caller)
          let now = counter.next(state.counter)

          let deadline_timer =
            process.send_after(
              state.self,
              deadline,
              DeadlineExpired(caller, now),
            )

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

/// Called when a client returns a connection to the pool.
/// Cleans up monitoring/deadline, then either serves a waiter
/// via CoDel or returns the connection to idle.
fn do_checkin(
  state: State(conn, err),
  caller: Pid,
  conn: conn,
) -> State(conn, err) {
  case dict.get(state.active, caller) {
    Ok(prev) -> {
      assert prev.conn == conn

      let _ = process.cancel_timer(prev.deadline_timer)
      process.demonitor_process(prev.monitor)
      let active = dict.delete(state.active, caller)
      let state = State(..state, active:)

      let now = counter.next(state.counter)
      codel_dequeue(state, now, prev.conn)
    }
    _ -> state
  }
}

fn do_enqueue(
  state: State(conn, err),
  caller: Pid,
  client: Subject(Result(conn, PoolError(err))),
  timeout: Int,
  deadline: Int,
) -> State(conn, err) {
  let monitor = process.monitor(caller)
  let waiting = Waiting(caller:, monitor:, client:, deadline:)

  // Shouldn't fail since enqueueing is done via the actor's message queue.
  // It shouldn't be possible for two waiters to be pushed at the same time.
  let assert Ok(sent_at) = queue.push(state.queue, waiting)

  let _timer =
    process.send_after(state.self, timeout, Timeout(sent_at, timeout))

  state
}

fn do_expire(
  state: State(conn, err),
  sent: Int,
  timeout: Int,
) -> State(conn, err) {
  queue.at(state.queue, sent)
  |> result.map(fn(waiting) {
    let now = counter.next(state.counter)

    use <- bool.lazy_guard(
      when: { now < { sent + timeout * ns_per_ms } },
      return: fn() {
        let remaining_ns = sent + timeout * ns_per_ms - now
        let remaining_ms = remaining_ns / ns_per_ms

        let _timer =
          process.send_after(state.self, remaining_ms, Timeout(sent, timeout))

        state
      },
    )

    let assert Ok(Nil) = queue.delete(state.queue, sent)

    actor.send(waiting.client, Error(ConnectionTimeout))

    process.demonitor_process(waiting.monitor)

    state
  })
  |> result.unwrap(state)
}

/// Called when a caller process dies while holding a connection or waiting.
/// If the caller held an active connection, the connection is closed and
/// replaced. If the caller was waiting in the queue, the entry is cleaned
/// up lazily: `serve_waiter` checks `process.is_alive` at dequeue time,
/// and `do_expire` removes entries when their timeout fires. The queue is
/// keyed by timestamp, so there is no efficient PID-based removal.
fn do_caller_down(state: State(conn, err), pid: Pid) -> State(conn, err) {
  case dict.get(state.active, pid) {
    Ok(prev) -> {
      let _ = process.cancel_timer(prev.deadline_timer)
      process.demonitor_process(prev.monitor)
      let active = dict.delete(state.active, pid)
      let state = State(..state, active:)

      let _ = state.handle_close(prev.conn)
      let state = State(..state, current_size: state.current_size - 1)

      case state.handle_open() {
        Ok(conn) -> {
          let state = State(..state, current_size: state.current_size + 1)
          let now = counter.next(state.counter)
          codel_dequeue(state, now, conn)
        }
        _ -> {
          schedule_reconnect(state, reconnect_min_ms)
          state
        }
      }
    }
    _ -> state
  }
}

/// Called when a deadline timer fires. The connection is closed and the
/// caller is removed from active, but the caller process is NOT killed
/// or notified — they still hold a reference to the now-closed connection
/// and will discover it is dead on their next operation. The deadline is
/// a hard cutoff, and the pool must reclaim connections from overrunning
/// callers to stay healthy.
fn do_deadline_expired(
  state: State(conn, err),
  caller: Pid,
  checkout_time: Int,
) -> State(conn, err) {
  dict.get(state.active, caller)
  |> result.map(fn(active) {
    use <- bool.guard(
      when: active.checkout_time != checkout_time,
      return: state,
    )

    process.demonitor_process(active.monitor)

    let active_dict = dict.delete(state.active, caller)
    let state = State(..state, active: active_dict)

    let _ = state.handle_close(active.conn)
    let state = State(..state, current_size: state.current_size - 1)

    case state.handle_open() {
      Ok(conn) -> {
        let state = State(..state, current_size: state.current_size + 1)
        let now = counter.next(state.counter)
        codel_dequeue(state, now, conn)
      }
      _ -> {
        schedule_reconnect(state, reconnect_min_ms)
        state
      }
    }
  })
  |> result.unwrap(state)
}

/// Called when a reconnect timer fires. Attempts to open a replacement
/// connection. On success, the connection is fed through CoDel to serve
/// a waiter or return to idle. On failure, another reconnect is scheduled
/// with increased backoff (randomized exponential, capped at 30s).
fn do_reconnect(state: State(conn, err), backoff: Int) -> State(conn, err) {
  case state.handle_open() {
    Ok(conn) -> {
      let state = State(..state, current_size: state.current_size + 1)
      let now = counter.next(state.counter)
      codel_dequeue(state, now, conn)
    }
    _ -> {
      schedule_reconnect(state, backoff)
      state
    }
  }
}

fn schedule_reconnect(state: State(conn, err), backoff: Int) -> Nil {
  let half = backoff / 2
  let delay = half + int.random(half + 1)
  let next_backoff = int.min(backoff * 2, reconnect_max_ms)
  let _timer = process.send_after(state.self, delay, Reconnect(next_backoff))
  Nil
}

// --- CoDel algorithm ---

// Dispatches to the appropriate strategy based on
// whether we're at an interval boundary, in slow mode, or in fast mode.
fn codel_dequeue(
  state: State(conn, err),
  now: Int,
  conn: conn,
) -> State(conn, err) {
  case { now >= state.next }, state.slow {
    True, _ -> dequeue_first(state, now, conn)
    False, False -> dequeue_fast(state, now, conn)
    False, True -> dequeue_slow(state, now, state.queue_target * 2, conn)
  }
}

// Called at an interval boundary.
fn dequeue_first(
  state: State(conn, err),
  now: Int,
  conn: conn,
) -> State(conn, err) {
  let next = now + state.queue_interval
  let slow = state.delay > state.queue_target

  case queue.first(state.queue) {
    Ok(#(sent, waiting)) -> {
      let assert Ok(Nil) = queue.delete(state.queue, sent)

      let delay = now - sent
      let state = State(..state, next:, delay:, slow:)

      serve_waiter(state, waiting, conn)
    }
    _ -> {
      let state = State(..state, idle: [conn, ..state.idle])
      State(..state, next:, delay: 0, slow:)
    }
  }
}

// Serve the first waiter immediately, tracking minimum delay.
fn dequeue_fast(
  state: State(conn, err),
  now: Int,
  conn: conn,
) -> State(conn, err) {
  case queue.first(state.queue) {
    Ok(#(sent, waiting)) -> {
      let assert Ok(Nil) = queue.delete(state.queue, sent)
      let delay = now - sent
      let state = case delay < state.delay {
        True -> State(..state, delay:)
        False -> state
      }
      serve_waiter(state, waiting, conn)
    }
    _ -> State(..state, idle: [conn, ..state.idle])
  }
}

// Drop waiters that have been waiting longer than the timeout
// threshold (target * 2), then serve the first valid waiter.
fn dequeue_slow(
  state: State(conn, err),
  now: Int,
  timeout: Int,
  conn: conn,
) -> State(conn, err) {
  case queue.first(state.queue) {
    Ok(#(sent, waiting)) if now - sent > timeout -> {
      let assert Ok(Nil) = queue.delete(state.queue, sent)

      drop_waiter(waiting)

      state
      |> dequeue_slow(now, timeout, conn)
    }
    Ok(#(sent, waiting)) -> {
      let assert Ok(Nil) = queue.delete(state.queue, sent)

      let delay = now - sent
      let state = case delay < state.delay {
        True -> State(..state, delay:)
        False -> state
      }

      serve_waiter(state, waiting, conn)
    }
    _ -> State(..state, idle: [conn, ..state.idle])
  }
}

fn serve_waiter(
  state: State(conn, err),
  waiting: Waiting(conn, PoolError(err)),
  conn: conn,
) -> State(conn, err) {
  case process.is_alive(waiting.caller) {
    False -> {
      process.demonitor_process(waiting.monitor)

      let now = counter.next(state.counter)
      codel_dequeue(state, now, conn)
    }
    True -> {
      process.send(waiting.client, Ok(conn))

      let now = counter.next(state.counter)

      let deadline_timer =
        process.send_after(
          state.self,
          waiting.deadline,
          DeadlineExpired(waiting.caller, now),
        )

      let activated =
        Active(
          conn:,
          monitor: waiting.monitor,
          deadline_timer:,
          checkout_time: now,
        )
      let active = dict.insert(state.active, waiting.caller, activated)

      State(..state, active:)
    }
  }
}

// --- CoDel polling ---

fn do_poll(
  state: State(conn, err),
  time: Int,
  last_sent: Int,
) -> State(conn, err) {
  case queue.first(state.queue) {
    Ok(#(sent, _)) if sent <= last_sent -> {
      let delay = time - sent

      state
      |> codel_timeout(delay, time)
      |> start_poll(time, sent)
    }
    Ok(#(sent, _)) -> start_poll(state, time, sent)
    _ -> start_poll(state, time, time)
  }
}

fn codel_timeout(
  state: State(conn, err),
  delay: Int,
  time: Int,
) -> State(conn, err) {
  case time >= state.next, state.delay > state.queue_target {
    True, True -> {
      State(..state, slow: True, delay:, next: time + state.queue_interval)
      |> poll_drop_slow(time, state.queue_target * 2)
    }
    True, False ->
      State(..state, slow: False, delay:, next: time + state.queue_interval)
    _, _ -> state
  }
}

fn poll_drop_slow(
  state: State(conn, err),
  now: Int,
  timeout: Int,
) -> State(conn, err) {
  case queue.first(state.queue) {
    Ok(#(sent, waiting)) if now - sent > timeout -> {
      let assert Ok(Nil) = queue.delete(state.queue, sent)

      drop_waiter(waiting)

      state
      |> poll_drop_slow(now, timeout)
    }
    _ -> state
  }
}

fn start_poll(
  state: State(conn, err),
  now: Int,
  last_sent: Int,
) -> State(conn, err) {
  let poll_time = now + state.queue_interval
  let _timer =
    process.send_after(
      state.self,
      state.queue_interval / ns_per_ms,
      Poll(poll_time, last_sent),
    )

  state
}

// --- Helpers ---

fn drop_waiter(waiting: Waiting(conn, PoolError(err))) -> Nil {
  actor.send(waiting.client, Error(ConnectionUnavailable))
  process.demonitor_process(waiting.monitor)
}

fn drain_queue(state: State(conn, err)) -> Nil {
  case queue.pop(state.queue) {
    Ok(waiting) -> {
      drop_waiter(waiting)
      drain_queue(state)
    }
    _ -> Nil
  }
}

fn close_active(state: State(conn, err)) -> Nil {
  dict.each(state.active, fn(_pid, active) {
    let _ = process.cancel_timer(active.deadline_timer)
    process.demonitor_process(active.monitor)
    let _ = state.handle_close(active.conn)
    Nil
  })
}

fn close_idle(state: State(conn, err)) -> Nil {
  list.each(state.idle, fn(conn) {
    let _ = state.handle_close(conn)
    Nil
  })
}
