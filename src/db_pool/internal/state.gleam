import gleam/bool
import gleam/dict.{type Dict}
import gleam/erlang/process
import gleam/list
import gleam/result
import rasa/atomic.{type Atomic}
import rasa/counter
import rasa/monotonic
import rasa/queue.{type Queue}
import rasa/table.{type Table}

// --- HolderRef: per-connection ETS table wrapper ---

/// Reference to an unnamed, public ETS table holding a single connection.
/// Each connection in the pool gets its own holder for crash isolation.
pub opaque type HolderRef(conn) {
  HolderRef(table: Table(Nil, conn))
}

/// Create a new holder containing the given connection.
pub fn new_holder(conn: conn) -> HolderRef(conn) {
  let t =
    table.build()
    |> table.with_access(table.Public)
    |> table.new
  let assert Ok(Nil) = table.insert(t, Nil, conn)
  HolderRef(table: t)
}

/// Read the connection from the holder.
pub fn get_conn(ref: HolderRef(conn)) -> Result(conn, Nil) {
  table.lookup(ref.table, Nil)
}

/// Overwrite the connection stored in the holder.
pub fn store_conn(ref: HolderRef(conn), conn: conn) -> Result(Nil, Nil) {
  table.insert(ref.table, Nil, conn)
}

/// Delete the holder table entirely. Used during cleanup.
pub fn destroy_holder(ref: HolderRef(conn)) -> Result(Nil, Nil) {
  table.drop(ref.table)
}

// --- PoolCounter: atomic idle connection counter ---

/// Atomic counter for tracking idle connection count.
/// Positive values mean idle connections available; zero or negative means busy.
pub opaque type PoolCounter {
  PoolCounter(atomic: Atomic)
}

/// Create a new pool counter initialized to the given value.
pub fn new_pool_counter(initial: Int) -> PoolCounter {
  let a = atomic.new()
  atomic.put(a, initial)
  PoolCounter(atomic: a)
}

/// Attempt to claim an idle connection slot.
/// Returns `Ok(new_value)` if slot claimed, `Error(Nil)` if none available.
pub fn counter_checkout(ctr: PoolCounter) -> Result(Int, Nil) {
  let val = atomic.sub_get(ctr.atomic, 1)
  case val >= 0 {
    True -> Ok(val)
    False -> {
      let _ = atomic.add_get(ctr.atomic, 1)
      Error(Nil)
    }
  }
}

/// Return an idle connection slot.
pub fn counter_checkin(ctr: PoolCounter) -> Int {
  atomic.add_get(ctr.atomic, 1)
}

/// Read the current counter value without modifying it.
pub fn counter_get(ctr: PoolCounter) -> Int {
  atomic.get(ctr.atomic)
}

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
    holder: HolderRef(conn),
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

/// Shared state accessible from both the pool actor and client processes.
/// This is returned as part of the PoolHandle.
pub type Shared(conn) {
  Shared(
    /// Atomic counter tracking idle connection count
    pool_counter: PoolCounter,
    /// ETS ordered_set of idle holders: timestamp -> HolderRef
    idle_queue: Table(Int, HolderRef(conn)),
    /// ETS set mapping client Pid -> HolderRef for checkin lookup
    checkout_map: Table(process.Pid, HolderRef(conn)),
    /// Pid of the pool actor (for give_away targets)
    pool_pid: process.Pid,
  )
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
    // shared state (accessible from clients)
    shared: Shared(conn),
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
  selector: process.Selector(msg),
) -> Result(State(conn, msg, err), String) {
  let connections = {
    list.repeat("", builder.max_size)
    |> list.try_map(fn(_) { builder.handle_open() })
    |> result.map_error(fn(_) { "(db_pool) Failed to open connections" })
  }

  use conns <- result.map(connections)

  let counter = counter.monotonic(monotonic.Nanosecond)

  let queue = queue.new(counter, table.Private)

  // Create shared state
  let pool_ctr = new_pool_counter(builder.max_size)

  let idle_queue =
    table.build()
    |> table.with_kind(table.OrderedSet)
    |> table.with_access(table.Public)
    |> table.new

  let checkout_map =
    table.build()
    |> table.with_kind(table.Set)
    |> table.with_access(table.Public)
    |> table.new

  let pool_pid = process.self()

  let shared =
    Shared(pool_counter: pool_ctr, idle_queue:, checkout_map:, pool_pid:)

  // Create a holder for each connection and insert into idle_queue
  list.each(conns, fn(conn) {
    let ref = new_holder(conn)
    // Insert into idle queue with a timestamp key
    let now = counter.next(counter)
    let assert Ok(Nil) = table.insert(idle_queue, now, ref)
  })

  // This shouldn't ever return `Error`
  let now = counter.next(counter)

  State(
    selector:,
    max_size: builder.max_size,
    current_size: builder.max_size,
    handle_open: builder.handle_open,
    handle_close: builder.handle_close,
    handle_interval: builder.handle_interval,
    interval: builder.interval,
    shared:,
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

/// Returns the shared state needed by clients for fast-path operations.
pub fn shared(state: State(conn, msg, err)) -> Shared(conn) {
  state.shared
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

/// Called when a fast-path checkout notification arrives from a client.
/// The client already has the holder data (public table); we just need
/// to set up monitoring and deadline tracking.
pub fn register_fast_checkout(
  state: State(conn, msg, err),
  caller: process.Pid,
  holder_ref: HolderRef(conn),
  deadline: Int,
  on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  let monitor = process.monitor(caller)
  let now = counter.next(state.counter)

  let deadline_subject = process.new_subject()
  let deadline_timer =
    process.send_after(deadline_subject, deadline, on_deadline(caller, now))

  let activated =
    Active(holder: holder_ref, monitor:, deadline_timer:, checkout_time: now)
  let active = dict.insert(state.active, caller, activated)

  let selector = process.select(state.selector, deadline_subject)

  State(..state, selector:, active:)
}

/// Called when a client-side fast-path checkin notification arrives.
/// The holder is ALREADY in idle_queue and pool_counter is ALREADY incremented
/// by the client. We just do cleanup (demonitor, cancel deadline) and then
/// check if there are waiters to serve. If waiters exist, we pop from
/// idle_queue (atomically) and serve them via CoDel.
pub fn checkin_notify(
  state: State(conn, msg, err),
  caller: process.Pid,
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  // Clean up the active record for this caller
  let state = case dict.get(state.active, caller) {
    Ok(prev) -> {
      let _ = process.cancel_timer(prev.deadline_timer)
      process.demonitor_process(prev.monitor)
      let active = dict.delete(state.active, caller)
      State(..state, active:)
    }
    Error(_) -> {
      // Already cleaned up (duplicate message or fast-path race). Continue
      // to check for waiters anyway since the holder is already in idle_queue.
      state
    }
  }

  // If there are waiters, try to pop an idle holder and serve them.
  // The holder was already added to idle_queue by the client, so we need
  // to claim it back via pool_counter + pop from idle_queue.
  case queue.size(state.queue) |> result.unwrap(0) > 0 {
    True -> {
      case counter_checkout(state.shared.pool_counter) {
        Ok(_) -> {
          case table.delete_first(state.shared.idle_queue) {
            Ok(#(_key, holder_ref)) -> {
              let now = counter.next(state.counter)
              codel_dequeue(state, now, holder_ref, drop, on_deadline)
            }
            Error(_) -> {
              // Counter said idle but ETS empty (race). Undo.
              let _ = counter_checkin(state.shared.pool_counter)
              state
            }
          }
        }
        Error(_) -> {
          // No idle conns available (another fast-path checkout grabbed it)
          state
        }
      }
    }
    False -> {
      // No waiters — holder is already idle. Nothing to do.
      state
    }
  }
}

/// Called when the actor receives a CallerDown message (client process died).
/// This handles the case where the client was a waiter (no holder yet) or
/// where the client crashed while holding a connection.
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

      // Check if the caller already did a client-side checkin (holder is
      // in idle_queue). Erlang guarantees CheckIn arrives before DOWN, so
      // this is defensive only — normally checkin_notify already cleaned up.
      case table.lookup(state.shared.checkout_map, pid) {
        Error(_) -> {
          // Client already checked in. Just clean up actor-side state.
          state
        }
        Ok(_) -> {
          // Client crashed without checking in. Close old conn and replace.
          let _ = table.delete(state.shared.checkout_map, pid)

          case get_conn(prev.holder) {
            Ok(conn) -> {
              let _ = state.handle_close(conn)
              Nil
            }
            Error(_) -> Nil
          }
          let _ = destroy_holder(prev.holder)

          case state.handle_open() {
            Ok(conn) -> {
              let new_ref = new_holder(conn)

              let now = counter.next(state.counter)
              codel_dequeue(state, now, new_ref, drop, on_deadline)
            }
            Error(_) -> State(..state, current_size: state.current_size - 1)
          }
        }
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

    // Guard against race with client-side checkin: if the caller already
    // removed themselves from checkout_map (via checkin), the holder is
    // already in idle_queue. Don't destroy it — just clean up tracking.
    case table.lookup(state.shared.checkout_map, caller) {
      Error(_) -> {
        // Client already checked in. Just clean up actor-side state.
        let _ = process.cancel_timer(active.deadline_timer)
        process.demonitor_process(active.monitor)
        let active_dict = dict.delete(state.active, caller)
        State(..state, active: active_dict)
      }
      Ok(_) -> {
        // Normal deadline expiry — caller still holds the connection.
        process.demonitor_process(active.monitor)

        let active_dict = dict.delete(state.active, caller)
        let _ = table.delete(state.shared.checkout_map, caller)
        let state = State(..state, active: active_dict)

        // Close old connection and destroy holder
        case get_conn(active.holder) {
          Ok(conn) -> {
            let _ = state.handle_close(conn)
            Nil
          }
          Error(_) -> Nil
        }
        let _ = destroy_holder(active.holder)

        case state.handle_open() {
          Ok(conn) -> {
            let new_ref = new_holder(conn)

            let now = counter.next(state.counter)
            codel_dequeue(state, now, new_ref, drop, on_deadline)
          }
          Error(_) -> State(..state, current_size: state.current_size - 1)
        }
      }
    }
  })
  |> result.unwrap(state)
}

/// Slow-path checkout: the pool has no idle connections, so try to open
/// a new one (if under max_size) or return Error to signal the caller
/// should be enqueued.
pub fn checkout(
  state: State(conn, msg, err),
  caller: process.Pid,
  deadline: Int,
  on_deadline: fn(process.Pid, Int) -> msg,
  next: fn(conn) -> Nil,
) -> Result(State(conn, msg, err), Nil) {
  // Check if caller already has a connection
  case dict.get(state.active, caller) {
    Ok(active) -> {
      case get_conn(active.holder) {
        Ok(conn) -> {
          next(conn)
          Ok(state)
        }
        Error(_) -> Error(Nil)
      }
    }
    Error(_) -> {
      // Try to open a new connection if under max_size
      case state.current_size < state.max_size {
        True -> {
          state.handle_open()
          |> result.map(fn(conn) {
            next(conn)

            let ref = new_holder(conn)

            let monitor = process.monitor(caller)
            let now = counter.next(state.counter)

            let deadline_subject = process.new_subject()
            let deadline_timer =
              process.send_after(
                deadline_subject,
                deadline,
                on_deadline(caller, now),
              )

            let activated =
              Active(holder: ref, monitor:, deadline_timer:, checkout_time: now)
            let active = dict.insert(state.active, caller, activated)
            let assert Ok(Nil) =
              table.insert(state.shared.checkout_map, caller, ref)

            let selector = process.select(state.selector, deadline_subject)

            State(
              ..state,
              current_size: state.current_size + 1,
              selector:,
              active:,
            )
          })
          |> result.replace_error(Nil)
        }
        False -> Error(Nil)
      }
    }
  }
}

pub fn current_connection(
  state: State(conn, msg, err),
  caller: process.Pid,
) -> Result(conn, Nil) {
  dict.get(state.active, caller)
  |> result.try(fn(active) { get_conn(active.holder) })
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

  let subject = process.new_subject()
  let _timer =
    process.send_after(subject, timeout, handle_timeout(now_in_ms, timeout))

  let selector = process.select(state.selector, subject)

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
  |> result.map(fn(waiting) {
    let now = counter.next(state.counter)

    use <- bool.lazy_guard(
      when: { now < { sent + timeout * ns_per_ms } },
      return: fn() {
        let subject = process.new_subject()
        let _timer = process.send_after(subject, timeout, extend(sent, timeout))

        let selector = process.select(state.selector, subject)

        State(..state, selector:)
      },
    )

    let assert Ok(Nil) = queue.delete(state.queue, sent)

    next(waiting.client)

    process.demonitor_process(waiting.monitor)

    state
  })
  |> result.unwrap(state)
}

// dispatches to the appropriate strategy based on
// whether we're at an interval boundary, in slow mode, or in fast mode.
fn codel_dequeue(
  state: State(conn, msg, err),
  now: Int,
  holder_ref: HolderRef(conn),
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  case now >= state.next {
    // evaluate whether we should be slow or fast
    True -> dequeue_first(state, now, holder_ref, drop, on_deadline)
    // Mid-interval
    False ->
      case state.slow {
        False -> dequeue_fast(state, now, holder_ref, drop, on_deadline)
        True ->
          dequeue_slow(
            state,
            now,
            state.queue_target * 2,
            holder_ref,
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
  holder_ref: HolderRef(conn),
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

      serve_waiter(state, waiting, holder_ref, drop, on_deadline)
    }
    _ -> {
      // No waiters so return holder to idle
      let state = return_to_idle(state, now, holder_ref)
      State(..state, next:, delay: 0, slow:)
    }
  }
}

// serve the first waiter immediately, tracking minimum delay.
fn dequeue_fast(
  state: State(conn, msg, err),
  now: Int,
  holder_ref: HolderRef(conn),
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
      serve_waiter(state, waiting, holder_ref, drop, on_deadline)
    }
    _ -> {
      // No waiters, return holder to idle
      return_to_idle(state, now, holder_ref)
    }
  }
}

// drop waiters that have been waiting longer than the timeout
// threshold (target * 2), then serve the first valid waiter.
fn dequeue_slow(
  state: State(conn, msg, err),
  now: Int,
  timeout: Int,
  holder_ref: HolderRef(conn),
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
      |> dequeue_slow(now, timeout, holder_ref, drop, on_deadline)
    }
    Ok(#(sent, waiting)) -> {
      // This waiter is fresh enough
      let assert Ok(Nil) = queue.delete(state.queue, sent)

      let delay = now - sent
      let state = case delay < state.delay {
        True -> State(..state, delay:)
        False -> state
      }

      serve_waiter(state, waiting, holder_ref, drop, on_deadline)
    }
    _ -> {
      // No waiters, return holder to idle
      return_to_idle(state, now, holder_ref)
    }
  }
}

/// Return a holder to the idle queue and increment the pool counter.
fn return_to_idle(
  state: State(conn, msg, err),
  now: Int,
  holder_ref: HolderRef(conn),
) -> State(conn, msg, err) {
  let assert Ok(Nil) = table.insert(state.shared.idle_queue, now, holder_ref)
  let _ = counter_checkin(state.shared.pool_counter)
  state
}

fn serve_waiter(
  state: State(conn, msg, err),
  waiting: Waiting(conn, err),
  holder_ref: HolderRef(conn),
  on_drop drop: fn(process.Subject(Result(conn, err))) -> Nil,
  on_deadline on_deadline: fn(process.Pid, Int) -> msg,
) -> State(conn, msg, err) {
  // Check if the waiter is still alive before committing to the handoff.
  case process.is_alive(waiting.caller) {
    False -> {
      process.demonitor_process(waiting.monitor)

      let now = counter.next(state.counter)
      codel_dequeue(state, now, holder_ref, drop, on_deadline)
    }
    True -> {
      // Serve the connection to the waiter
      let assert Ok(conn) = get_conn(holder_ref)

      let now = counter.next(state.counter)

      // Record in checkout_map for checkin lookup
      let assert Ok(Nil) =
        table.insert(state.shared.checkout_map, waiting.caller, holder_ref)

      // Send the connection to the waiter's client subject
      process.send(waiting.client, Ok(conn))

      // Set up monitoring and deadline for the checkout
      let monitor = process.monitor(waiting.caller)

      let deadline_subject = process.new_subject()
      let deadline_timer =
        process.send_after(
          deadline_subject,
          waiting.deadline,
          on_deadline(waiting.caller, now),
        )

      let activated =
        Active(
          holder: holder_ref,
          monitor:,
          deadline_timer:,
          checkout_time: now,
        )
      let active = dict.insert(state.active, waiting.caller, activated)

      process.demonitor_process(waiting.monitor)

      let selector = process.select(state.selector, deadline_subject)

      State(..state, selector:, active:)
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
  let subject = process.new_subject()
  let _timer =
    process.send_after(
      subject,
      state.queue_interval / ns_per_ms,
      schedule_poll(poll_time, last_sent),
    )
  let selector = process.select(state.selector, subject)

  State(..state, selector:)
}

pub fn shutdown(state: State(conn, msg, err)) -> Nil {
  case dict.size(state.active) {
    0 -> {
      // Close all idle connections and destroy their holders
      close_idle_holders(state)
    }
    _ -> Nil
  }
}

pub fn ping(state: State(conn, msg, err), message: msg) -> State(conn, msg, err) {
  // Ping all idle connections by reading them from idle_queue
  case table.to_list(state.shared.idle_queue) {
    Ok(entries) -> {
      list.each(entries, fn(entry) {
        let #(_ts, ref) = entry
        case get_conn(ref) {
          Ok(conn) -> state.handle_interval(conn)
          Error(_) -> Nil
        }
      })
    }
    Error(_) -> Nil
  }

  let subject = process.new_subject()
  let _timer = process.send_after(subject, state.interval, message)
  let selector = process.select(state.selector, subject)

  State(..state, selector:)
}

pub fn close(state: State(conn, msg, err)) -> Nil {
  close_idle_holders(state)
}

fn close_idle_holders(state: State(conn, msg, err)) -> Nil {
  case table.to_list(state.shared.idle_queue) {
    Ok(entries) -> {
      list.each(entries, fn(entry) {
        let #(ts, ref) = entry
        case get_conn(ref) {
          Ok(conn) -> {
            let _ = state.handle_close(conn)
            Nil
          }
          Error(_) -> Nil
        }
        let _ = destroy_holder(ref)
        let _ = table.delete(state.shared.idle_queue, ts)
      })
    }
    Error(_) -> Nil
  }
}
