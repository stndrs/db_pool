import db_pool/internal/state.{type HolderRef, type Shared, type State}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import rasa/counter
import rasa/monotonic
import rasa/table

pub type PoolError(err) {
  ConnectionError(err)
  ConnectionTimeout
  ConnectionUnavailable
  ConnectionDeadlineExceeded
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

/// Handle returned by `start` — wraps the actor subject plus shared state
/// needed for fast-path checkout/checkin.
pub type PoolHandle(conn, err) {
  PoolHandle(subject: Subject(Message(conn, err)), shared: Shared(conn))
}

/// Starts a connection pool.
pub fn start(
  pool: Pool(conn, err),
  name: process.Name(Message(conn, err)),
  timeout: Int,
) -> Result(PoolHandle(conn, err), actor.StartError) {
  start_raw(pool, name, timeout)
  |> result.map(fn(pair) {
    let #(started, shared) = pair
    PoolHandle(subject: started.data, shared:)
  })
}

/// Internal start that returns the raw Started + Shared for supervised use.
fn start_raw(
  pool: Pool(conn, err),
  name: process.Name(Message(conn, err)),
  timeout: Int,
) -> Result(
  #(actor.Started(Subject(Message(conn, err))), Shared(conn)),
  actor.StartError,
) {
  actor.new_with_initialiser(timeout, initialise_pool(_, pool))
  |> actor.on_message(handle_message)
  |> actor.named(name)
  |> actor.start
  |> result.map(fn(started) {
    let cntr = counter.monotonic(monotonic.Nanosecond)
    let time = counter.next(cntr)

    let _timer = process.send_after(started.data, pool.interval, Interval)
    let _poll_timer =
      process.send_after(
        started.data,
        pool.queue_interval,
        Poll(time:, last_sent: time),
      )

    // Extract the shared state by sending a GetShared request
    let shared =
      process.call_forever(started.data, fn(reply) { GetShared(reply) })

    #(started, shared)
  })
}

/// Creates a `supervision.ChildSpecification` so the pool can be
/// added to an application's supervision tree.
pub fn supervised(
  pool: Pool(conn, err),
  name: process.Name(Message(conn, err)),
  timeout: Int,
) -> supervision.ChildSpecification(PoolHandle(conn, err)) {
  supervision.worker(fn() {
    start_raw(pool, name, timeout)
    |> result.map(fn(pair) {
      let #(started, shared) = pair
      let handle = PoolHandle(subject: started.data, shared:)
      actor.Started(pid: started.pid, data: handle)
    })
  })
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
  process.trap_exits(True)

  let selector =
    process.new_selector()
    |> process.select(self)
    |> process.select_trapped_exits(PoolExit)
    |> process.select_monitors(CallerDown)

  state.new()
  |> state.max_size(pool.size)
  |> state.on_open(pool.handle_open)
  |> state.on_close(pool.handle_close)
  |> state.on_interval(pool.handle_interval)
  |> state.interval(pool.interval)
  |> state.queue_target(pool.queue_target)
  |> state.queue_interval(pool.queue_interval)
  |> state.build(selector)
  |> result.map(fn(state) {
    actor.initialised(state)
    |> actor.selecting(selector)
    |> actor.returning(self)
  })
}

pub opaque type Message(conn, err) {
  Interval
  /// Slow-path checkout request (client waits for actor)
  CheckOut(
    client: Subject(Result(conn, PoolError(err))),
    caller: Pid,
    timeout: Int,
    deadline: Int,
  )
  /// Fast-path notification: client already has the holder, pool sets up monitor/deadline
  FastCheckoutNotify(caller: Pid, holder_ref: HolderRef(conn), deadline: Int)
  /// Fast-path checkin: client returns holder to pool
  CheckIn(caller: Pid, holder_ref: HolderRef(conn))
  Timeout(time_sent: Int, timeout: Int)
  DeadlineExpired(caller: Pid, checkout_time: Int)
  Poll(time: Int, last_sent: Int)
  PoolExit(process.ExitMessage)
  CallerDown(process.Down)
  Shutdown(client: process.Subject(Result(Nil, PoolError(err))))
  /// Internal message to extract shared state after init
  GetShared(reply: Subject(Shared(conn)))
}

/// Attempts to check out a connection from the pool.
/// Uses the fast-path (atomic counter + ETS) when idle connections are available,
/// falling back to the slow-path (actor mailbox) when the pool is busy.
pub fn checkout(
  handle: PoolHandle(conn, err),
  caller: Pid,
  timeout: Int,
  deadline: Int,
) -> Result(conn, PoolError(err)) {
  let shared = handle.shared

  // Check if caller already has a connection checked out (re-entrant checkout)
  case table.lookup(shared.checkout_map, caller) {
    Ok(existing_holder) -> {
      case state.get_conn(existing_holder) {
        Ok(conn) -> Ok(conn)
        Error(_) -> Error(ConnectionUnavailable)
      }
    }
    Error(_) -> {
      // Fast path: atomically claim an idle connection slot
      case state.counter_checkout(shared.pool_counter) {
        Ok(_count) -> {
          // We won a slot. Grab the first idle holder from the ETS ordered_set.
          case table.delete_first(shared.idle_queue) {
            Ok(#(_key, holder_ref)) -> {
              // Record in checkout_map for checkin lookup
              let assert Ok(Nil) =
                table.insert(shared.checkout_map, caller, holder_ref)

              // Notify the pool actor to set up monitor + deadline
              process.send(
                handle.subject,
                FastCheckoutNotify(caller:, holder_ref:, deadline:),
              )

              // Read the connection from the holder (public table, no ownership needed)
              case state.get_conn(holder_ref) {
                Ok(conn) -> Ok(conn)
                Error(_) -> {
                  // Holder lost its conn somehow; clean up and error
                  let _ = table.delete(shared.checkout_map, caller)
                  let _ = state.counter_checkin(shared.pool_counter)
                  Error(ConnectionUnavailable)
                }
              }
            }
            Error(_) -> {
              // Counter said idle conns exist but ETS empty (race). Undo and slow path.
              let _ = state.counter_checkin(shared.pool_counter)
              slow_path_checkout(handle, caller, timeout, deadline)
            }
          }
        }
        Error(_) -> {
          // No idle connections available — slow path
          slow_path_checkout(handle, caller, timeout, deadline)
        }
      }
    }
  }
}

fn slow_path_checkout(
  handle: PoolHandle(conn, err),
  caller: Pid,
  timeout: Int,
  deadline: Int,
) -> Result(conn, PoolError(err)) {
  process.call_forever(handle.subject, CheckOut(_, caller:, timeout:, deadline:))
}

/// Returns a connection back to the pool.
/// Client-side fast-path: immediately makes the holder available for other
/// callers by inserting into idle_queue and incrementing pool_counter,
/// then notifies the actor for cleanup (demonitor, cancel deadline, serve waiters).
pub fn checkin(handle: PoolHandle(conn, err), _conn: conn, caller: Pid) -> Nil {
  let shared = handle.shared

  // Look up the holder from checkout_map
  case table.lookup(shared.checkout_map, caller) {
    Ok(holder_ref) -> {
      // Remove from checkout map (client side)
      let _ = table.delete(shared.checkout_map, caller)

      // CLIENT-SIDE FAST PATH: make holder immediately available
      // Use unique_integer([monotonic]) for a key that is both unique
      // and monotonically increasing — safe for concurrent insertions.
      let key = monotonic.unique()
      let assert Ok(Nil) = table.insert(shared.idle_queue, key, holder_ref)
      let _ = state.counter_checkin(shared.pool_counter)

      // Notify actor for cleanup (demonitor, cancel deadline timer, serve waiters)
      process.send(handle.subject, CheckIn(caller:, holder_ref:))
      Nil
    }
    Error(_) -> {
      // No holder found — might be a stale checkin. Ignore.
      Nil
    }
  }
}

/// Shuts down the pool and any idle connections.
pub fn shutdown(
  handle: PoolHandle(conn, err),
  timeout: Int,
) -> Result(Nil, PoolError(err)) {
  process.call(handle.subject, timeout, Shutdown)
}

fn handle_message(
  state: State(conn, Message(conn, err), PoolError(err)),
  msg: Message(conn, err),
) -> actor.Next(
  State(conn, Message(conn, err), PoolError(err)),
  Message(conn, err),
) {
  case msg {
    GetShared(reply) -> {
      process.send(reply, state.shared(state))
      actor.continue(state)
    }
    Interval -> {
      let state = state.ping(state, Interval)

      use selector <- state.with_selector(state)

      actor.continue(state)
      |> actor.with_selector(selector)
    }
    CheckIn(caller:, holder_ref: _) -> {
      let on_drop = fn(client) {
        actor.send(client, Error(ConnectionUnavailable))
      }
      let state =
        state.checkin_notify(
          state,
          caller,
          on_drop: on_drop,
          on_deadline: DeadlineExpired,
        )

      use selector <- state.with_selector(state)

      actor.continue(state)
      |> actor.with_selector(selector)
    }
    FastCheckoutNotify(caller:, holder_ref:, deadline:) -> {
      let state =
        state.register_fast_checkout(
          state,
          caller,
          holder_ref,
          deadline,
          DeadlineExpired,
        )

      use selector <- state.with_selector(state)

      actor.continue(state)
      |> actor.with_selector(selector)
    }
    CheckOut(client:, caller:, timeout:, deadline:) -> {
      let state = {
        state.checkout(state, caller, deadline, DeadlineExpired, fn(conn) {
          actor.send(client, Ok(conn))
        })
        |> result.lazy_unwrap(fn() {
          state.enqueue(
            state,
            caller,
            client,
            timeout,
            deadline,
            on_timeout: Timeout,
          )
        })
      }

      use selector <- state.with_selector(state)

      actor.continue(state)
      |> actor.with_selector(selector)
    }
    Timeout(time_sent:, timeout:) -> {
      let state = {
        let on_expiry = actor.send(_, Error(ConnectionTimeout))

        state.expire(state, time_sent, timeout, on_expiry:, or_else: Timeout)
      }

      use selector <- state.with_selector(state)

      actor.continue(state)
      |> actor.with_selector(selector)
    }
    DeadlineExpired(caller:, checkout_time:) -> {
      let on_drop = fn(client) {
        actor.send(client, Error(ConnectionUnavailable))
      }
      let state =
        state.deadline_expired(
          state,
          caller,
          checkout_time,
          on_drop: on_drop,
          on_deadline: DeadlineExpired,
        )

      use selector <- state.with_selector(state)

      actor.continue(state)
      |> actor.with_selector(selector)
    }
    CallerDown(down) -> {
      let assert process.ProcessDown(pid:, ..) = down

      let on_drop = fn(client) {
        actor.send(client, Error(ConnectionUnavailable))
      }
      let state =
        state.caller_down(
          state,
          pid,
          on_drop: on_drop,
          on_deadline: DeadlineExpired,
        )

      use selector <- state.with_selector(state)

      actor.continue(state)
      |> actor.with_selector(selector)
    }
    Poll(time:, last_sent:) -> {
      let on_drop = actor.send(_, Error(ConnectionUnavailable))
      let state = state.poll(state, time, last_sent, on_poll: Poll, on_drop:)

      use selector <- state.with_selector(state)

      actor.continue(state)
      |> actor.with_selector(selector)
    }
    PoolExit(exit) -> {
      state.close(state)

      case exit.reason {
        process.Normal -> actor.stop()
        process.Killed -> actor.stop_abnormal("pool killed")
        process.Abnormal(_reason) ->
          actor.stop_abnormal("pool stopped abnormally")
      }
    }
    Shutdown(client:) -> {
      state.shutdown(state)

      actor.send(client, Ok(Nil))
      actor.stop()
    }
  }
}
// --- FFI bindings ---
