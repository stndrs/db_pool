import db_pool/internal/codel.{type Codel}
import db_pool/internal/time.{type Clock, type Instant}
import gleam/bool
import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/erlang/atom
import gleam/erlang/process.{type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/order
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import gleam/string
import gleam/time/duration.{type Duration}
import logging

fn reconnect_min() -> Duration {
  duration.milliseconds(1000)
}

fn reconnect_max() -> Duration {
  duration.milliseconds(30_000)
}

// Minimum interval for checking for idle connections.
fn close_min_interval() -> Duration {
  duration.milliseconds(1000)
}

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
/// for opening and closing connections.
pub opaque type Pool(conn, err) {
  Pool(
    size: Int,
    max_idle_connections: Option(Int),
    max_idle_time: Option(Int),
    queue_target: Int,
    queue_interval: Int,
    handle_open: fn() -> Result(conn, PoolError(err)),
    handle_close: fn(conn) -> Result(Nil, PoolError(err)),
    handle_idle: fn(conn) -> Nil,
    handle_active: fn(conn) -> Nil,
    error_to_string: Option(fn(err) -> String),
  )
}

/// Returns a `Pool` that needs to be configured.
pub fn new() -> Pool(conn, err) {
  let handle_open = fn() { Error(ConnectionTimeout) }
  let handle_close = fn(_) { Ok(Nil) }

  Pool(
    size: 5,
    max_idle_connections: None,
    max_idle_time: None,
    queue_target: 50,
    queue_interval: 1000,
    handle_open:,
    handle_close:,
    handle_idle: fn(_) { Nil },
    handle_active: fn(_) { Nil },
    error_to_string: None,
  )
}

/// Sets the size of the pool. At startup the pool will create `size`
/// number of connections.
pub fn size(pool: Pool(conn, err), size: Int) -> Pool(conn, err) {
  // A pool must have at least one connection to be able to serve callers.
  Pool(..pool, size: int.max(size, 1))
}

/// Sets the maximum number of idle connections the pool holds. When set,
/// the pool becomes elastic. The pool's `size` becomes the ceiling for
/// the number of open connections. At pool startup, `max_idle_connections`
/// connections are eagerly opened.
///
/// If more connections than `max_idle_connections` are needed, extra
/// connections are opened, limited by `size`. Any idle connections beyond
/// `max_idle_connections` are closed after sitting idle for `max_idle_time`.
///
/// If this value is not set, the pool keeps `size` connections open.
pub fn max_idle_connections(
  pool: Pool(conn, err),
  num: Int,
) -> Pool(conn, err) {
  Pool(..pool, max_idle_connections: Some(int.max(num, 0)))
}

/// Sets the maximum time in milliseconds a connection may sit idle before
/// the pool closes it. Closed connections are not replaced unless pool demand
/// requires more to be opened.
///
/// Defaults to disabled, and has a lower limit of 1 millisecond. If passed
/// 0 or negative, a value of 1 millisecond will be used.
pub fn max_idle_time(pool: Pool(conn, err), ms: Int) -> Pool(conn, err) {
  Pool(..pool, max_idle_time: Some(int.max(ms, 1)))
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
/// called on each connection when the pool is shut down or exits.
pub fn on_close(
  pool: Pool(conn, err),
  handle_close: fn(conn) -> Result(Nil, err),
) -> Pool(conn, err) {
  let handle_close = fn(conn) {
    handle_close(conn) |> result.map_error(ConnectionError)
  }

  Pool(..pool, handle_close:)
}

/// Sets the `Pool`'s `on_idle` function. The provided function will be
/// called on connections when they're checked back in to the pool. If
/// the connection is immediately passed to a waiting caller, the callback
/// will not be called. The callback is also called on every connection
/// at startup.
pub fn on_idle(
  pool: Pool(conn, err),
  handle_idle: fn(conn) -> Nil,
) -> Pool(conn, err) {
  Pool(..pool, handle_idle:)
}

/// Sets the `Pool`'s `on_active` function. The provided function will be
/// called on connections as they're removed from the pool's list of
/// idle connections and become active.
pub fn on_active(
  pool: Pool(conn, err),
  handle_active: fn(conn) -> Nil,
) -> Pool(conn, err) {
  Pool(..pool, handle_active:)
}

/// Sets the CoDel queue target in milliseconds. This is the maximum
/// acceptable queue delay before the pool considers itself overloaded.
/// Defaults to 50ms.
pub fn queue_target(pool: Pool(conn, err), target: Int) -> Pool(conn, err) {
  Pool(..pool, queue_target: int.max(target, 0))
}

/// Sets the CoDel queue interval in milliseconds. This is the length
/// of each CoDel measurement interval. The pool evaluates queue health
/// at each interval boundary. Defaults to 1000ms.
pub fn queue_interval(pool: Pool(conn, err), interval: Int) -> Pool(conn, err) {
  // Clamp to a 1ms floor: a 0ms interval would busy-spin the poll loop and a
  // negative interval would crash `send_after` during initialisation.
  Pool(..pool, queue_interval: int.max(interval, 1))
}

/// Sets a function used to convert the `Pool`'s `err` type into a `String`
/// for logging. When set, `ConnectionError`s encountered internally by the
/// pool (for example during startup) are logged using this function.
pub fn error_to_string(
  pool: Pool(conn, err),
  to_string: fn(err) -> String,
) -> Pool(conn, err) {
  Pool(..pool, error_to_string: Some(to_string))
}

// --- Internal types ---

type Waiting(conn, err) {
  Waiting(
    caller: Pid,
    monitor: process.Monitor,
    client: Subject(Result(conn, err)),
    deadline: Duration,
  )
}

type Active(conn) {
  Active(
    conn: conn,
    monitor: process.Monitor,
    deadline_timer: process.Timer,
    checkout_time: Instant,
    depth: Int,
  )
}

// An in-flight connection open. The pool monitors the opener process and
// carries the `backoff` to use if this open fails so retries keep their
// exponential schedule.
type Opener {
  Opener(monitor: process.Monitor, backoff: Duration)
}

// An idle connection paired with the instant at which it went idle. The idle
// list stays LIFO so hot connections are reused from the head and cold ones
// age at the tail.
type Idle(conn) {
  Idle(conn: conn, since: Instant)
}

// How a draining pool should finish once its last in-flight open resolves.
// `Shutdown` carries the caller to reply to. The exit variants remember the
// termination reason so the pool stops the same way it would have.
type DrainMode(err) {
  DrainShutdown(client: Subject(Result(Nil, PoolError(err))))
  DrainExitNormal
  DrainExitKilled
  DrainExitAbnormal
}

type State(conn, err) {
  State(
    self: Subject(Message(conn, err)),
    pool: Pool(conn, err),
    current_size: Int,
    idle: List(Idle(conn)),
    active: Dict(Pid, Active(conn)),
    // In-flight opens, keyed by opener pid. `dict.size` is the pool's
    // `pending_opens`.
    openers: Dict(Pid, Opener),
    // The queue of waiting callers and the overload state governing it.
    codel: Codel(Waiting(conn, PoolError(err))),
    clock: Clock,
    // Set to `Some` mode when shutdown/exit has begun but in-flight opens
    // remain. The pool stays alive to close arriving connections, then
    // finishes.
    draining: Option(DrainMode(err)),
  )
}

// The number of connections the pool keeps idle. Without an explicit
// `max_idle_connections` the pool is fixed-size and holds all `size`
// connections; with one it is capped by `size`.
fn max_idle(pool: Pool(conn, err)) -> Int {
  case pool.max_idle_connections {
    None -> pool.size
    Some(n) -> int.min(n, pool.size)
  }
}

/// Starts a connection pool and registers it under `name`. All
/// configured connections are opened eagerly during initialisation.
///
/// The `timeout` parameter is the maximum time in milliseconds allowed
/// for the actor to initialise (open all connections).
///
/// The pool actor traps exits so it can perform cleanup when its
/// parent or linked processes terminate.
pub fn start(
  pool: Pool(conn, err),
  name: process.Name(Message(conn, err)),
  timeout: Int,
) -> Result(actor.Started(Subject(Message(conn, err))), actor.StartError) {
  let clock = time.clock()

  actor.new_with_initialiser(timeout, initialise_pool(_, pool, clock))
  |> actor.on_message(handle_message)
  |> actor.named(name)
  |> actor.start
}

/// Creates a `supervision.ChildSpecification` so the pool can be
/// added to an application's supervision tree.
///
/// The `timeout` parameter is used for both the actor initialisation
/// timeout and the supervisor's shutdown timeout. The restart strategy
/// is set to `Transient`, meaning the pool is restarted only if it terminates
/// abnormally.
pub fn supervised(
  pool: Pool(conn, err),
  name: process.Name(Message(conn, err)),
  timeout: Int,
) -> supervision.ChildSpecification(Subject(Message(conn, err))) {
  supervision.worker(fn() { start(pool, name, timeout) })
  |> supervision.timeout(timeout)
  |> supervision.restart(supervision.Transient)
}

pub opaque type Message(conn, err) {
  CheckOut(
    client: Subject(Result(conn, PoolError(err))),
    caller: Pid,
    timeout: Duration,
    deadline: Duration,
  )
  CheckIn(caller: Pid, conn: conn)
  Timeout(key: Int, timeout: Duration)
  DeadlineExpired(caller: Pid, checkout_time: Instant)
  Poll(last_queue_key: Int)
  PoolExit(process.ExitMessage)
  CallerDown(process.Down)
  // Delivered by an opener process with the outcome of a `handle_open`
  // call. `pid` identifies the opener so the pool can reconcile it against
  // its `openers` set. `backoff` is the retry delay to use on failure.
  OpenResult(pid: Pid, result: Result(conn, PoolError(err)), backoff: Duration)
  // Fired by a retry timer after a failed open. Re-checks demand before
  // spawning a fresh opener.
  RetryOpen(backoff: Duration)
  // Fired by the periodic close timer when `max_idle_time` is set; closes
  // connections that have been idle past the limit.
  CloseIdle
  Shutdown(client: Subject(Result(Nil, PoolError(err))))
}

// Extra time in ms added to the pool-side timeout when making a
// `process.call`. The pool handles timeouts internally and replies
// before this buffer expires. If the pool actor is truly unreachable
// the caller panics after `timeout + client_timeout_buffer_ms`.
const client_timeout_buffer_ms = 5000

/// Checks out a connection from the pool.
///
/// The connection is associated with the calling process (`process.self()`).
/// The pool monitors this process and reclaims the connection if it crashes.
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
/// Panics if the pool actor is unreachable.
pub fn checkout(
  pool: Subject(Message(conn, err)),
  timeout: Int,
  deadline: Int,
) -> Result(conn, PoolError(err)) {
  // The caller is always the calling process: `process.call` replies to it
  // anyway, and deriving it here keeps the monitor, re-entrancy key, and
  // reclaim logic attached to the right process.
  let caller = process.self()

  // Clamp so negative values can't crash the shared pool actor's internal
  // `send_after` calls (badarg) and take down every other caller.
  let timeout_ms = int.max(timeout, 0)
  let deadline_ms = int.max(deadline, 0)

  process.call(pool, timeout_ms + client_timeout_buffer_ms, CheckOut(
    _,
    caller:,
    timeout: duration.milliseconds(timeout_ms),
    deadline: duration.milliseconds(deadline_ms),
  ))
}

/// Returns a connection back to the pool.
///
/// Expects the `conn` value to be the same connection that was originally
/// checked out by the calling process. If the calling process has no active
/// connection the checkin is silently ignored.
pub fn checkin(pool: Subject(Message(conn, err)), conn: conn) -> Nil {
  let caller = process.self()
  process.send(pool, CheckIn(caller:, conn:))
}

/// Checks out a connection from the pool and passes it to the provided
/// callback function. The connection is automatically checked back in
/// after the callback function returns.
///
/// If the callback panics, the connection is not checked in immediately.
/// It is reclaimed when the caller process exits (via the pool's
/// monitor).
///
/// Panics if the pool actor is unreachable (crashed or shut down).
pub fn with_connection(
  pool: Subject(Message(conn, err)),
  timeout: Int,
  deadline: Int,
  next: fn(conn) -> t,
) -> Result(t, PoolError(err)) {
  let caller = process.self()

  // Clamp so negative values can't crash the shared pool actor's internal
  // `send_after` calls (badarg) and take down every other caller.
  let timeout_ms = int.max(timeout, 0)
  let deadline_ms = int.max(deadline, 0)

  process.call(pool, timeout_ms + client_timeout_buffer_ms, CheckOut(
    _,
    caller:,
    timeout: duration.milliseconds(timeout_ms),
    deadline: duration.milliseconds(deadline_ms),
  ))
  |> result.map(fn(conn) {
    let res = next(conn)

    process.send(pool, CheckIn(caller:, conn:))

    res
  })
}

/// Shuts down the pool gracefully within `timeout` milliseconds.
///
/// All waiting callers in the queue are drained and sent a
/// `ConnectionUnavailable` error. Active (checked-out) connections
/// are closed, their deadline timers cancelled, and their monitors
/// removed. Idle connections are then closed via the configured
/// `on_close` callback.
///
/// Panics if the pool actor is unreachable or does not respond
/// within the timeout.
pub fn shutdown(
  pool: Subject(Message(conn, err)),
  timeout: Int,
) -> Result(Nil, PoolError(err)) {
  process.call(pool, timeout + client_timeout_buffer_ms, Shutdown)
}

// --- Actor initialisation ---

fn initialise_pool(
  self: Subject(Message(conn, err)),
  pool: Pool(conn, err),
  clock: Clock,
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
    list.repeat("", max_idle(pool))
    |> list.try_map(fn(_) { pool.handle_open() })
    |> result.map_error(log_error(pool.error_to_string, _))

  use conns <- result.map(connections)

  list.each(conns, pool.handle_idle)

  let now = time.now(clock)

  let codel =
    codel.new(
      duration.milliseconds(pool.queue_target),
      duration.milliseconds(pool.queue_interval),
      now,
    )

  // The poll is seeded with key 0 rather than a clock reading: queue keys come
  // from `erlang:unique_integer([monotonic])`, which starts near
  // -576460752303423488, so every key the VM issues is negative and the first
  // poll's `key <= last_key` test holds - the same branch the old nanosecond
  // seed produced. A key counter starting at 1 would flip that branch.
  let _poll_timer =
    process.send_after(self, pool.queue_interval, Poll(last_queue_key: 0))

  // Arm the idle-close sweep only when a limit is configured.
  pool.max_idle_time
  |> option.map(fn(ms) {
    let _close_timer =
      process.send_after(
        self,
        duration.to_milliseconds(close_interval(duration.milliseconds(ms))),
        CloseIdle,
      )

    Nil
  })
  |> option.unwrap(Nil)

  let idle = list.map(conns, fn(conn) { Idle(conn:, since: now) })

  let state =
    State(
      self:,
      pool:,
      current_size: max_idle(pool),
      idle:,
      active: dict.new(),
      openers: dict.new(),
      codel:,
      clock:,
      draining: None,
    )

  actor.initialised(state)
  |> actor.selecting(selector)
  |> actor.returning(self)
}

fn handle_message(
  state: State(conn, err),
  msg: Message(conn, err),
) -> actor.Next(State(conn, err), Message(conn, err)) {
  case state.draining {
    Some(mode) -> handle_draining(state, mode, msg)
    None -> handle_running(state, msg)
  }
}

fn handle_running(
  state: State(conn, err),
  msg: Message(conn, err),
) -> actor.Next(State(conn, err), Message(conn, err)) {
  case msg {
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
    Timeout(key:, timeout:) -> {
      let state = do_expire(state, key, timeout)
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
    Poll(last_queue_key:) -> {
      state
      |> do_poll(last_queue_key)
      |> actor.continue
    }
    OpenResult(pid:, result:, backoff:) -> {
      state
      |> do_open_result(pid, result, backoff)
      |> actor.continue
    }
    RetryOpen(backoff:) -> {
      state
      |> do_retry_open(backoff)
      |> actor.continue
    }
    CloseIdle -> {
      state
      |> do_close_idle
      |> actor.continue
    }
    PoolExit(exit) -> {
      let mode = exit_reason_to_drain_mode(exit.reason)

      begin_shutdown(state, mode)
    }
    Shutdown(client:) -> begin_shutdown(state, DrainShutdown(client))
  }
}

fn exit_reason_to_drain_mode(reason: process.ExitReason) -> DrainMode(err) {
  case reason {
    process.Normal -> {
      logging.log(logging.Info, "Normal Pool Exit")

      DrainExitNormal
    }
    process.Killed -> {
      logging.log(logging.Warning, "Pool Killed")

      DrainExitKilled
    }
    process.Abnormal(reason) -> {
      let decoder = {
        atom.decoder()
        |> decode.map(atom.to_string)
        |> decode.one_of(or: [decode.string])
      }

      reason
      |> decode.run(decoder)
      |> result.map(string.append("Abnormal Pool Exit: ", _))
      |> result.unwrap(or: "Abnormal Pool Exit")
      |> logging.log(logging.Warning, _)

      DrainExitAbnormal
    }
  }
}

// Close everything the pool currently holds and either finish immediately or,
// if opens are still in flight, enter the draining state so those
// connections are closed as they arrive rather than leaked.
fn begin_shutdown(
  state: State(conn, err),
  mode: DrainMode(err),
) -> actor.Next(State(conn, err), Message(conn, err)) {
  let state =
    drain_queue(state)
    |> close_each_active
    |> close_idle

  case dict.size(state.openers) > 0 {
    True -> {
      State(..state, draining: Some(mode), idle: [], active: dict.new())
      |> actor.continue
    }
    False -> finish_drain(mode)
  }
}

// Handles messages while draining. Only in-flight opens and the down signals
// of the opener processes matter.
fn handle_draining(
  state: State(conn, err),
  mode: DrainMode(err),
  msg: Message(conn, err),
) -> actor.Next(State(conn, err), Message(conn, err)) {
  case msg {
    OpenResult(pid:, result:, ..) -> {
      result
      |> result.try(state.pool.handle_close)
      |> result.unwrap(Nil)

      state
      |> drop_opener(pid)
      |> finish_or_continue_draining(mode)
    }
    CallerDown(down) -> {
      let assert process.ProcessDown(pid:, ..) = down

      state
      |> drop_opener(pid)
      |> finish_or_continue_draining(mode)
    }
    CheckOut(client:, ..) -> {
      actor.send(client, Error(ConnectionUnavailable))
      actor.continue(state)
    }
    CheckIn(conn:, ..) -> {
      let _ = state.pool.handle_close(conn)
      actor.continue(state)
    }
    Shutdown(client:) -> {
      actor.send(client, Error(ConnectionUnavailable))
      actor.continue(state)
    }
    // Poll, Timeout, DeadlineExpired, RetryOpen, CloseIdle, PoolExit do
    // nothing while draining.
    _ -> actor.continue(state)
  }
}

// Drop an opener from the in-flight set.
fn drop_opener(state: State(conn, err), pid: Pid) -> State(conn, err) {
  dict.get(state.openers, pid)
  |> result.map(fn(opener) {
    process.demonitor_process(opener.monitor)

    State(..state, openers: dict.delete(state.openers, pid))
  })
  |> result.unwrap(state)
}

fn finish_or_continue_draining(
  state: State(conn, err),
  mode: DrainMode(err),
) -> actor.Next(State(conn, err), Message(conn, err)) {
  case dict.size(state.openers) > 0 {
    True -> actor.continue(state)
    False -> finish_drain(mode)
  }
}

// Stop the pool the way the original shutdown/exit intended, replying to the
// shutdown caller if there was one.
fn finish_drain(
  mode: DrainMode(err),
) -> actor.Next(State(conn, err), Message(conn, err)) {
  case mode {
    DrainShutdown(client:) -> {
      actor.send(client, Ok(Nil))
      actor.stop()
    }

    DrainExitNormal -> actor.stop()
    DrainExitKilled -> actor.stop_abnormal("pool killed")
    DrainExitAbnormal -> actor.stop_abnormal("pool stopped abnormally")
  }
}

/// Try to check out a connection. Returns Ok(state) if served
/// (re-entrant checkout or idle conn available), Error(Nil) if the
/// caller should be enqueued.
fn do_checkout(
  state: State(conn, err),
  caller: Pid,
  client: Subject(Result(conn, PoolError(err))),
  deadline: Duration,
) -> Result(State(conn, err), Nil) {
  dict.get(state.active, caller)
  |> result.map(fn(active) {
    // Subsequent checkouts return the same connection. The original
    // deadline is preserved as callers cannot extend their deadline
    // by checking out again. A single process is limited to one
    // connection at a time. The checkout depth is tracked so the
    // connection is only released once every nested checkout has
    // checked in.
    let active = Active(..active, depth: active.depth + 1)
    let active_dict = dict.insert(state.active, caller, active)

    actor.send(client, Ok(active.conn))

    State(..state, active: active_dict)
  })
  |> result.lazy_or(fn() {
    case state.idle {
      [Idle(conn:, ..), ..rest] -> {
        let monitor = process.monitor(caller)
        let now = time.now(state.clock)

        let deadline_timer =
          process.send_after(
            state.self,
            duration.to_milliseconds(deadline),
            DeadlineExpired(caller, now),
          )

        let activated =
          Active(conn:, monitor:, deadline_timer:, checkout_time: now, depth: 1)

        let active = dict.insert(state.active, caller, activated)

        state.pool.handle_active(conn)

        actor.send(client, Ok(conn))

        Ok(State(..state, idle: rest, active:))
      }
      [] -> Error(Nil)
    }
  })
}

/// Called when a client returns a connection to the pool.
/// Cleans up monitoring/deadline, then either serves a waiter
/// via CoDel or returns the connection to idle.
fn do_checkin(
  state: State(conn, err),
  caller: Pid,
  conn: conn,
) -> State(conn, err) {
  dict.get(state.active, caller)
  |> result.map(fn(prev) {
    case prev.conn == conn {
      True -> Nil
      False -> {
        "(db_pool) unexpected connection checked in for the current process"
        |> logging.log(logging.Warning, _)
      }
    }

    case prev.depth > 1 {
      True -> {
        let active =
          state.active
          |> dict.insert(caller, Active(..prev, depth: prev.depth - 1))

        State(..state, active:)
      }
      False -> {
        let _ = process.cancel_timer(prev.deadline_timer)
        process.demonitor_process(prev.monitor)
        let active = dict.delete(state.active, caller)
        let state = State(..state, active:)

        let now = time.now(state.clock)
        codel_dequeue(state, now, prev.conn)
      }
    }
  })
  |> result.unwrap(state)
}

fn do_enqueue(
  state: State(conn, err),
  caller: Pid,
  client: Subject(Result(conn, PoolError(err))),
  timeout: Duration,
  deadline: Duration,
) -> State(conn, err) {
  let monitor = process.monitor(caller)
  let sent_at = time.now(state.clock)
  let waiting = Waiting(caller:, monitor:, client:, deadline:)

  // The queue counter is strictly unique, so `push` never fails on a key
  // collision here. We still pattern-match defensively rather than crash the
  // whole pool. On the impossible-Error path we drop this single caller with
  // `ConnectionUnavailable` and tear down its monitor instead of taking down
  // every connection and waiter.
  codel.push(state.codel, waiting, sent_at)
  |> result.map(fn(key) {
    let _timer =
      process.send_after(
        state.self,
        duration.to_milliseconds(timeout),
        Timeout(key, timeout),
      )

    // Grow to meet demand: one open per enqueue, bounded by the capacity
    // invariant so a burst of N waiters spawns at most enough opens to
    // reach `max_size`.
    case at_capacity(state) {
      True -> state
      False -> spawn_opener(state, reconnect_min())
    }
  })
  |> result.lazy_unwrap(fn() {
    actor.send(client, Error(ConnectionUnavailable))
    process.demonitor_process(monitor)
    state
  })
}

fn do_expire(
  state: State(conn, err),
  key: Int,
  timeout: Duration,
) -> State(conn, err) {
  codel.at(state.codel, key)
  |> result.map(fn(entry) {
    let codel.Entry(sent_at:, item: waiting) = entry

    let now = time.now(state.clock)
    let expires_at = time.advance(sent_at, by: timeout)

    use <- bool.lazy_guard(
      when: time.compare(now, expires_at) == order.Lt,
      return: fn() {
        let remaining_ms =
          duration.to_milliseconds(time.since(from: now, to: expires_at))

        let _timer =
          process.send_after(state.self, remaining_ms, Timeout(key, timeout))

        state
      },
    )

    codel.delete(state.codel, key)
    |> result.map(fn(_) {
      actor.send(waiting.client, Error(ConnectionTimeout))

      process.demonitor_process(waiting.monitor)

      state
    })
    |> result.unwrap(state)
  })
  |> result.unwrap(state)
}

// Called when a caller process dies while holding a connection or waiting.
// If the caller held an active connection, the connection is closed and
// replaced. If the caller was waiting in the queue, the entry is cleaned
// up lazily. `codel_dequeue` checks `process.is_alive` at dequeue time,
// and `do_expire` removes entries when their timeout fires.
fn do_caller_down(state: State(conn, err), pid: Pid) -> State(conn, err) {
  // DOWN can come from an opener we monitor as well as from a caller. An
  // opener that dies without delivering an `OpenResult` is a failed open.
  dict.get(state.openers, pid)
  |> result.map(fn(opener) {
    let openers = dict.delete(state.openers, pid)

    State(..state, openers:)
    |> retry_if_demand(opener.backoff)
  })
  |> result.lazy_or(fn() {
    use prev <- result.map(dict.get(state.active, pid))

    let _ = process.cancel_timer(prev.deadline_timer)

    close_active(state, prev, pid)
  })
  |> result.unwrap(state)
}

// Called when a deadline timer fires. The connection is closed and the
// caller is removed from active, but the caller process is not killed
// or notified. The caller still holds a reference to the now-closed connection
// and will discover it is dead on their next operation. The deadline is
// a hard cutoff and the pool must reclaim connections from overrunning
// callers to stay healthy.
fn do_deadline_expired(
  state: State(conn, err),
  caller: Pid,
  checkout_time: Instant,
) -> State(conn, err) {
  dict.get(state.active, caller)
  |> result.map(fn(active) {
    use <- bool.guard(active.checkout_time != checkout_time, return: state)

    close_active(state, active, caller)
  })
  |> result.unwrap(state)
}

fn close_active(
  state: State(conn, err),
  active: Active(conn),
  pid: Pid,
) -> State(conn, err) {
  process.demonitor_process(active.monitor)

  let active_dict = dict.delete(state.active, pid)

  let _ = state.pool.handle_close(active.conn)
  let state =
    State(..state, active: active_dict, current_size: state.current_size - 1)

  // Open a replacement only when a waiter needs it and capacity allows. Used
  // by the demand-driven crash/deadline replacement paths.
  case queue_has_waiter(state) && !at_capacity(state) {
    True -> spawn_opener(state, reconnect_min())
    False -> state
  }
}

// --- Async connection opener ---

// True when opening another connection would breach the capacity ceiling.
fn at_capacity(state: State(conn, err)) -> Bool {
  state.current_size + dict.size(state.openers) >= state.pool.size
}

// True when at least one caller is waiting in the queue. Liveness of the
// head waiter is not checked here; dead waiters are closeed when served or
// when their timeout fires. This is only a demand heuristic.
fn queue_has_waiter(state: State(conn, err)) -> Bool {
  result.is_ok(codel.first(state.codel))
}

// Spawn a monitored opener process that runs `handle_open` off the actor
// loop and reports the outcome via `OpenResult`. `pending_opens` grows by
// one until the result or the opener's down signal is handled.
fn spawn_opener(
  state: State(conn, err),
  backoff: Duration,
) -> State(conn, err) {
  let self = state.self
  let handle_open = state.pool.handle_open

  let pid =
    process.spawn_unlinked(fn() {
      let unlinked = process.self()
      let result = handle_open()

      process.send(self, OpenResult(pid: unlinked, result:, backoff:))
    })

  let monitor = process.monitor(pid)
  let openers = dict.insert(state.openers, pid, Opener(monitor:, backoff:))

  State(..state, openers:)
}

// After a failed open, retry only if demand still exists (a waiter queued
// and capacity available). Otherwise the failure is dropped; a later
// checkout starts a fresh open.
fn retry_if_demand(
  state: State(conn, err),
  backoff: Duration,
) -> State(conn, err) {
  case queue_has_waiter(state) && !at_capacity(state) {
    True -> schedule_retry(state, backoff)
    False -> Nil
  }

  state
}

/// Handles the outcome of an in-flight open. The opener is reconciled by
/// pid: if it is unknown (already handled via its down signal) an `Ok`
/// connection is closed to avoid a leak and an `Error` is ignored.
fn do_open_result(
  state: State(conn, err),
  pid: Pid,
  open_result: Result(conn, PoolError(err)),
  backoff: Duration,
) -> State(conn, err) {
  dict.get(state.openers, pid)
  |> result.try(fn(opener) {
    // Flush drops any pending down signal for this opener.
    process.demonitor_process(opener.monitor)

    let openers = dict.delete(state.openers, pid)
    let state = State(..state, openers:)

    open_result
    |> result.map(fn(conn) {
      let now = time.now(state.clock)

      State(..state, current_size: state.current_size + 1)
      |> codel_dequeue(now, conn)
    })
    |> result.try_recover(fn(pool_error) {
      let _ = log_error(state.pool.error_to_string, pool_error)

      Ok(retry_if_demand(state, backoff))
    })
  })
  |> result.lazy_unwrap(fn() {
    open_result
    |> result.try(state.pool.handle_close)
    |> result.unwrap(Nil)

    state
  })
}

// Fired by a retry timer. Re-checks demand and spawns a fresh opener if
// it still exists.
fn do_retry_open(
  state: State(conn, err),
  backoff: Duration,
) -> State(conn, err) {
  case queue_has_waiter(state) && !at_capacity(state) {
    True -> spawn_opener(state, backoff)
    False -> state
  }
}

fn schedule_retry(state: State(conn, err), backoff: Duration) -> Nil {
  let half_ms = duration.to_milliseconds(time.halve(backoff))
  let delay = half_ms + int.random(half_ms + 1)
  let next_backoff = time.min(time.double(backoff), reconnect_max())

  let _timer = process.send_after(state.self, delay, RetryOpen(next_backoff))

  Nil
}

// --- Idle closeing ---

// The close sweep interval for a given idle limit: half the limit, floored so
// the timer can never fire too aggressively. `max_idle_time` is non-negative
// here because the public `max_idle_time` setter clamps it to at least 1.
fn close_interval(max_idle_time: Duration) -> Duration {
  time.max(time.halve(max_idle_time), close_min_interval())
}

/// Fired by the close timer. Closes every idle connection that has been idle
/// longer than `max_idle_time`, decrementing `current_size` for each, then
/// re-arms the timer. Closed connections are not replaced. The pool regrows
/// on demand.
fn do_close_idle(state: State(conn, err)) -> State(conn, err) {
  case state.pool.max_idle_time {
    None -> state
    Some(ms) -> {
      let now = time.now(state.clock)
      let max_idle = duration.milliseconds(ms)

      let #(expired, live) =
        list.partition(state.idle, fn(idle) {
          duration.compare(time.since(from: idle.since, to: now), max_idle)
          == order.Gt
        })

      list.each(expired, fn(idle) {
        let _ = state.pool.handle_close(idle.conn)
        Nil
      })

      let _close_timer =
        process.send_after(
          state.self,
          duration.to_milliseconds(close_interval(max_idle)),
          CloseIdle,
        )

      State(
        ..state,
        idle: live,
        current_size: state.current_size - list.length(expired),
      )
    }
  }
}

// --- Serving waiters ---

// Hands a connection to the next waiter CoDel selects, closing out any waiters
// it sheds on the way. A waiter whose caller has died is skipped and the
// algorithm is asked again.
fn codel_dequeue(
  state: State(conn, err),
  now: Instant,
  conn: conn,
) -> State(conn, err) {
  let #(c, outcome) = codel.dequeue(state.codel, now)
  let state = State(..state, codel: c)

  case outcome {
    codel.Serve(item: waiting, dropped:) -> {
      list.each(dropped, drop_waiter)

      case process.is_alive(waiting.caller) {
        True -> serve_waiter(state, waiting, conn)
        False -> {
          process.demonitor_process(waiting.monitor)

          codel_dequeue(state, time.now(state.clock), conn)
        }
      }
    }
    codel.Empty(dropped:) -> {
      list.each(dropped, drop_waiter)

      push_idle(state, now, conn)
    }
  }
}

// Return a connection to the idle set, stamping it with the current time,
// or close it when the set is already at `max_idle_connections`. Closing
// shrinks `current_size`; the connection is not replaced.
fn push_idle(
  state: State(conn, err),
  now: Instant,
  conn: conn,
) -> State(conn, err) {
  case list.length(state.idle) >= max_idle(state.pool) {
    True -> {
      let _ = state.pool.handle_close(conn)
      State(..state, current_size: state.current_size - 1)
    }
    False -> {
      state.pool.handle_idle(conn)
      State(..state, idle: [Idle(conn:, since: now), ..state.idle])
    }
  }
}

fn serve_waiter(
  state: State(conn, err),
  waiting: Waiting(conn, PoolError(err)),
  conn: conn,
) -> State(conn, err) {
  let now = time.now(state.clock)

  let deadline_timer =
    process.send_after(
      state.self,
      duration.to_milliseconds(waiting.deadline),
      DeadlineExpired(waiting.caller, now),
    )

  let activated =
    Active(
      conn:,
      monitor: waiting.monitor,
      deadline_timer:,
      checkout_time: now,
      depth: 1,
    )

  let active = dict.insert(state.active, waiting.caller, activated)

  process.send(waiting.client, Ok(conn))

  state.pool.handle_active(conn)

  State(..state, active:)
}

// --- CoDel polling ---

fn do_poll(state: State(conn, err), last_queue_key: Int) -> State(conn, err) {
  let now = time.now(state.clock)

  let #(c, polled) = codel.poll(state.codel, now, last_queue_key)
  let codel.Polled(dropped:, last_key:) = polled

  list.each(dropped, drop_waiter)

  start_poll(State(..state, codel: c), last_key)
}

fn start_poll(
  state: State(conn, err),
  last_queue_key: Int,
) -> State(conn, err) {
  let _timer =
    process.send_after(
      state.self,
      state.pool.queue_interval,
      Poll(last_queue_key:),
    )

  state
}

// --- Helpers ---

// Renders a `PoolError` into the log/InitFailed string, using the optional
// `error_to_string` to describe the wrapped `ConnectionError` payload.
fn describe_error(
  error_to_string: Option(fn(err) -> String),
  pool_error: PoolError(err),
) -> String {
  let err_string = case pool_error {
    ConnectionError(err) ->
      case error_to_string {
        Some(to_string) -> "ConnectionError: " <> to_string(err)
        None -> "ConnectionError"
      }
    ConnectionTimeout -> "ConnectionTimeout"
    ConnectionUnavailable -> "ConnectionUnavailable"
  }

  "(db_pool) " <> err_string
}

// Logs a `PoolError` and returns the rendered message (used as the
// `InitFailed` reason at startup).
fn log_error(
  error_to_string: Option(fn(err) -> String),
  pool_error: PoolError(err),
) -> String {
  let message = describe_error(error_to_string, pool_error)
  logging.log(logging.Error, message)
  message
}

fn drop_waiter(waiting: Waiting(conn, PoolError(err))) -> Nil {
  actor.send(waiting.client, Error(ConnectionUnavailable))

  process.demonitor_process(waiting.monitor)
}

fn drain_queue(state: State(conn, err)) -> State(conn, err) {
  codel.pop_all(state.codel)
  |> list.each(drop_waiter)

  state
}

fn close_each_active(state: State(conn, err)) -> State(conn, err) {
  dict.each(state.active, fn(_pid, active) {
    let _ = process.cancel_timer(active.deadline_timer)

    process.demonitor_process(active.monitor)

    let _ = state.pool.handle_close(active.conn)

    Nil
  })

  state
}

fn close_idle(state: State(conn, err)) -> State(conn, err) {
  list.each(state.idle, fn(idle) {
    let _ = state.pool.handle_close(idle.conn)

    Nil
  })

  state
}
