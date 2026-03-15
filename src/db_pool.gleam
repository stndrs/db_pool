import db_pool/internal/state.{type State}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import rasa/counter
import rasa/monotonic

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

/// Starts a connection pool. Returns a Subject for sending messages
/// to the pool actor.
pub fn start(
  pool: Pool(conn, err),
  name: process.Name(Message(conn, err)),
  timeout: Int,
) -> Result(Subject(Message(conn, err)), actor.StartError) {
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

    started.data
  })
}

/// Creates a `supervision.ChildSpecification` so the pool can be
/// added to an application's supervision tree.
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
  |> state.build(self)
  |> result.map(fn(state) {
    actor.initialised(state)
    |> actor.selecting(selector)
    |> actor.returning(self)
  })
}

pub opaque type Message(conn, err) {
  Interval
  /// Checkout request: client waits for actor to serve or enqueue
  CheckOut(
    client: Subject(Result(conn, PoolError(err))),
    caller: Pid,
    timeout: Int,
    deadline: Int,
  )
  /// Checkin: client returns a connection to the pool
  CheckIn(caller: Pid, conn: conn)
  Timeout(time_sent: Int, timeout: Int)
  DeadlineExpired(caller: Pid, checkout_time: Int)
  Poll(time: Int, last_sent: Int)
  PoolExit(process.ExitMessage)
  CallerDown(process.Down)
  Shutdown(client: process.Subject(Result(Nil, PoolError(err))))
}

/// Checks out a connection from the pool. All checkouts go through
/// the actor mailbox.
pub fn checkout(
  pool: Subject(Message(conn, err)),
  caller: Pid,
  timeout: Int,
  deadline: Int,
) -> Result(conn, PoolError(err)) {
  process.call_forever(pool, CheckOut(_, caller:, timeout:, deadline:))
}

/// Returns a connection back to the pool.
pub fn checkin(
  pool: Subject(Message(conn, err)),
  conn: conn,
  caller: Pid,
) -> Nil {
  process.send(pool, CheckIn(caller:, conn:))
}

/// Shuts down the pool and any idle connections.
pub fn shutdown(
  pool: Subject(Message(conn, err)),
  timeout: Int,
) -> Result(Nil, PoolError(err)) {
  process.call(pool, timeout, Shutdown)
}

fn handle_message(
  state: State(conn, Message(conn, err), PoolError(err)),
  msg: Message(conn, err),
) -> actor.Next(
  State(conn, Message(conn, err), PoolError(err)),
  Message(conn, err),
) {
  case msg {
    Interval -> {
      let state = state.ping(state, Interval)

      actor.continue(state)
    }
    CheckIn(caller:, conn: _) -> {
      let on_drop = fn(client) {
        actor.send(client, Error(ConnectionUnavailable))
      }
      let state =
        state.checkin(
          state,
          caller,
          on_drop: on_drop,
          on_deadline: DeadlineExpired,
        )

      actor.continue(state)
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

      actor.continue(state)
    }
    Timeout(time_sent:, timeout:) -> {
      let state = {
        let on_expiry = actor.send(_, Error(ConnectionTimeout))

        state.expire(state, time_sent, timeout, on_expiry:, or_else: Timeout)
      }

      actor.continue(state)
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

      actor.continue(state)
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

      actor.continue(state)
    }
    Poll(time:, last_sent:) -> {
      let on_drop = actor.send(_, Error(ConnectionUnavailable))
      let state = state.poll(state, time, last_sent, on_poll: Poll, on_drop:)

      actor.continue(state)
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
