# ETS Fast-Path Implementation

A port of [pgo_pool.erl](https://github.com/erleans/pgo/blob/main/src/pgo_pool.erl)'s "holder" pattern to Gleam. Atomic counters and public ETS reads bypass the actor mailbox on both checkout and checkin, reducing uncontended p50 latency from ~11ms to ~1us.

## Architecture overview

```
                    ┌──────────────────────────────────────────────┐
                    │              Shared state (ETS)              │
                    │                                              │
                    │  pool_counter   idle_queue     checkout_map  │
                    │  (atomics)      (ordered_set)  (set)         │
                    │  [signed int]   {key→HolderRef} {Pid→HolderRef}│
                    └──────────┬───────────┬──────────┬────────────┘
                               │           │          │
              ┌────────────────┘           │          └─────────────────┐
              │                            │                            │
    ┌─────────▼─────────┐      ┌──────────▼──────────┐     ┌──────────▼──────────┐
    │   Client process  │      │   Client process     │     │   Pool actor        │
    │   (checkout)      │      │   (checkin)          │     │   (cleanup only)    │
    │                   │      │                      │     │                     │
    │ 1. sub_get(ctr)   │      │ 1. delete(ckout_map) │     │ - monitor/demonitor │
    │ 2. pop(idle_q)    │      │ 2. insert(idle_q)    │     │ - deadline timers   │
    │ 3. insert(ck_map) │      │ 3. add_get(ctr)      │     │ - waiter queue      │
    │ 4. read(holder)   │      │ 4. send CheckIn msg  │     │ - crash recovery    │
    └───────────────────┘      └──────────────────────┘     └─────────────────────┘
```

The pool actor still exists and handles: monitoring callers, deadline timers, CoDel waiter queue management, and crash recovery. But the hot path (checkout + checkin when idle connections exist) never waits on the actor.

## Data structures

### HolderRef (`state.gleam`)

Each connection lives in its own **unnamed, public ETS `set` table** via `rasa/table`. The pool actor always owns these tables (no `give_away` or `set_heir`). Any process can read/write because they're public.

`HolderRef(conn)` is an opaque type wrapping `rasa/table.Table(Nil, conn)`. It stores a single record: `{Nil, conn}`. The `conn` type parameter cascades through `Active(conn)`, `Shared(conn)`, `State(conn, msg, err)`, `PoolHandle(conn, err)`, and related `Message` variants.

Previous versions stored 4 key-value pairs per holder (`Conn`, `CheckoutTime`, `DeadlineMs`, `PoolPid`). These were dead writes — `checkout_time` is tracked in `Active.checkout_time`, deadline info in `Active.deadline_timer`/`Waiting.deadline`, and `pool_pid` in `Shared.pool_pid`. The simplified single-record design eliminates this waste.

Functions (all in `state.gleam`):
- `new_holder(conn)` — create table and insert connection
- `get_conn(ref)` — read connection value
- `store_conn(ref, conn)` — overwrite connection value
- `destroy_holder(ref)` — delete the ETS table

### PoolCounter (`state.gleam`)

A signed atomic integer (via `rasa/atomic`) tracking the number of idle connections.

- **Positive** = idle connections available
- **Zero/negative** = all connections busy
- Initialized to `max_size` at startup

Checkout uses `atomic.sub_get` — atomically decrement and read. If result < 0, undo with `atomic.add_get` and fall back to slow path. Checkin uses `atomic.add_get` to increment.

Functions (all in `state.gleam`):
- `new_pool_counter(n)` — create counter starting at n
- `counter_checkout(ctr)` — atomic decrement, undo if negative
- `counter_checkin(ctr)` — atomic increment
- `counter_get(ctr)` — read current value

### Idle queue

A **public, named ETS `ordered_set`** (via `rasa/table`). Keys are unique monotonic integers from `rasa/monotonic.unique()`, values are `HolderRef`. The ordered_set means `table.first` always returns the oldest idle connection (FIFO).

### Checkout map

A **public, named ETS `set`** (via `rasa/table`) mapping `Pid → HolderRef`. Used by:
- `checkin` to look up which holder a caller has
- `deadline_expired`/`caller_down` to detect if a caller already checked in

### Shared state

All four structures above are bundled into a `Shared` record returned as part of `PoolHandle` from `start`:

```gleam
pub type Shared(conn) {
  Shared(
    pool_counter: PoolCounter,
    idle_queue: table.Table(Int, HolderRef(conn)),
    checkout_map: table.Table(process.Pid, HolderRef(conn)),
    pool_pid: process.Pid,
  )
}
```

### Actor state (`State`)

The pool actor maintains private state for tracking and coordination:

- `active: Dict(Pid, Active(conn))` — each checked-out connection's monitor, deadline timer, holder ref, and checkout time
- `queue: Queue(Waiting)` — CoDel-managed waiter queue via `rasa/queue` (processes waiting for a connection)
- `counter: counter.Counter` — monotonic nanosecond counter via `rasa/counter` for CoDel timestamps
- CoDel state: `queue_target`, `queue_interval`, `delay`, `slow`, `next`

## Checkout flow

### Fast path (runs in caller's process, no actor involved)

```
checkout(handle, caller, timeout, deadline):

1. Re-entrant check:
   lookup(checkout_map, caller)
   → if found, return existing connection (same process checking out twice)

 2. Atomic claim:
    counter_checkout()  // atomic.sub_get(1)
    → if result < 0: undo, go to slow path

3. Pop idle holder:
   ets_first_and_delete(idle_queue)
   → if empty (race): undo counter, go to slow path

4. Record checkout:
   insert(checkout_map, caller, holder_ref)

5. Notify actor (async, fire-and-forget):
   send(pool_subject, FastCheckoutNotify(caller, holder_ref, deadline))

6. Read connection:
   get_conn(holder_ref)
   → if error: clean up checkout_map + counter, return ConnectionUnavailable
   → if ok: return connection
```

The actor processes `FastCheckoutNotify` later to set up monitoring and the deadline timer. This is not on the critical path.

### Slow path (actor mailbox, used when pool is busy)

```
1. send call_forever(pool_subject, CheckOut(client, caller, timeout, deadline))

Actor receives CheckOut:
2. Try state.checkout() — opens a new connection if current_size < max_size
3. If no capacity: state.enqueue() — add to CoDel waiter queue with timeout
4. Connection delivered when a checkin triggers codel_dequeue → serve_waiter
```

## Checkin flow

### Client side (runs in caller's process)

```
checkin(handle, conn, caller):

1. Look up holder:
   lookup(checkout_map, caller)
   → if not found: stale checkin, ignore

2. Remove from checkout_map:
   delete(checkout_map, caller)

 3. Return to idle (client-side fast path):
    key = monotonic.unique()
    table.insert(idle_queue, key, holder_ref)
    counter_checkin()  // atomic.add_get(1)

4. Notify actor (async):
   send(pool_subject, CheckIn(caller, holder_ref))
```

Steps 2-3 make the connection immediately available for other callers. The next `checkout` can find this connection without waiting for the actor to process the `CheckIn` message.

### Actor side (`checkin_notify`)

```
on CheckIn(caller, holder_ref):

1. Cleanup active record (if exists):
   - cancel_timer(deadline_timer)
   - demonitor(monitor)
   - delete from active dict

 2. Check for waiters:
    if queue.size > 0:
      counter_checkout()  // claim the idle holder back
      pop_idle = ets_first_and_delete(idle_queue)
      codel_dequeue(holder_ref)  // serve waiter or return to idle
    else:
      no-op (holder already idle from client-side insert)
```

The `pop_idle` function is injected from `db_pool.gleam` as a callback to avoid exposing the FFI to `state.gleam`. This should eventually be upstreamed into `rasa` as `table.pop_first`.

### Why `monotonic.unique()` for idle_queue keys

The idle_queue is an `ordered_set` keyed by integers. Multiple clients may insert concurrently (client-side checkin). `rasa/counter.monotonic(Nanosecond)` wraps `erlang:monotonic_time/1` which is **not** guaranteed unique across concurrent callers (two calls in the same nanosecond can return the same value). `rasa/monotonic.unique()` wraps `erlang:unique_integer([monotonic])` which is both monotonically increasing AND guaranteed unique — two calls never return the same value, even from different processes.

## CoDel queue management

The pool implements the Controlled Delay (CoDel) algorithm for managing the waiter queue. This prevents the pool from serving stale waiters during overload.

Three dequeue strategies are used depending on timing:

- **dequeue_first** — at interval boundaries: evaluate whether to enter slow mode based on `min_delay > target`
- **dequeue_fast** — mid-interval, not slow: serve immediately, track minimum delay
- **dequeue_slow** — mid-interval, slow mode: drop waiters older than `target * 2`, then serve

`codel_dequeue` dispatches to the appropriate strategy. It's called:
- When `checkin_notify` finds waiters and pops an idle holder
- When `caller_down` or `deadline_expired` creates a replacement connection
- From `return_to_idle` at the end of dequeue chains (when a served waiter is dead and we retry)

## Crash recovery

### Caller crashes with connection checked out (`caller_down`)

1. Cancel deadline timer, demonitor, remove from `active` dict
2. Check `checkout_map` — if caller already removed themselves (checkin was called before crash), just clean up actor state. The holder is already in idle_queue.
3. If still in `checkout_map`: close old connection, destroy holder, open replacement, feed through `codel_dequeue`
4. If `handle_open` fails: shrink pool by 1 (`current_size - 1`)

Erlang guarantees messages sent by a process arrive before that process's DOWN signal. So if a caller called `checkin` (sending `CheckIn`) and then crashed, `CheckIn` is processed before `CallerDown`.

### Deadline expiry (`deadline_expired`)

Per-checkout deadlines. Each `checkout` call specifies a `deadline` in milliseconds. The actor sets a timer at checkout time.

1. Validate `checkout_time` matches (guards against stale timers from a previous checkout by the same caller)
2. Check `checkout_map` — if caller already removed themselves (race with client-side checkin), just clean up actor state
3. If still in `checkout_map`: close connection, destroy holder, open replacement, feed through `codel_dequeue`
4. If `handle_open` fails: shrink pool by 1

### Dead waiter in `serve_waiter`

Before handing a connection to a waiter, `process.is_alive(waiting.caller)` is checked. If dead, demonitor and retry `codel_dequeue` with the same holder (tries the next waiter).

## Actor messages

| Message | Source | Purpose |
|---------|--------|---------|
| `FastCheckoutNotify(caller, holder_ref, deadline)` | Client (fast-path checkout) | Actor sets up monitor + deadline timer |
| `CheckIn(caller, holder_ref)` | Client (fast-path checkin) | Actor does cleanup + checks for waiters |
| `CheckOut(client, caller, timeout, deadline)` | Client (slow-path) | Synchronous checkout when pool is busy |
| `CallerDown(Down)` | Monitor | Crash recovery for checked-out connections |
| `DeadlineExpired(caller, checkout_time)` | Timer | Per-checkout deadline enforcement |
| `Timeout(time_sent, timeout)` | Timer | Waiter queue timeout |
| `Poll(time, last_sent)` | Timer | CoDel interval evaluation |
| `Interval` | Timer | Periodic ping of idle connections |
| `GetShared(reply)` | `start` | One-shot: extract shared state after init |
| `Shutdown(client)` | Client | Graceful shutdown |
| `PoolExit(ExitMessage)` | Trapped exit | Process exit handling |

## File layout

```
src/
  db_pool.gleam              — Public API, actor message handler, FFI bindings
  db_pool_ffi.erl            — Erlang FFI: ets_first_and_delete, raw_send
  db_pool/internal/
    state.gleam              — Core state machine: HolderRef(conn), PoolCounter,
                               Shared(conn), State(conn, msg, err), Active(conn),
                               Waiting, Builder, all holder/counter functions,
                               checkin_notify, caller_down, deadline_expired,
                               CoDel dequeue chain, serve_waiter, return_to_idle

dev/
  db_pool/
    benchmark.gleam          — Performance benchmark (4 scenarios)
```

Dependencies:
- `rasa/table` — ETS table management (idle_queue, checkout_map, holder tables)
- `rasa/atomic` — lock-free atomic integers (pool_counter)
- `rasa/counter` + `rasa/monotonic` — monotonic timestamps and unique keys
- `rasa/queue` — ETS-backed FIFO queue (CoDel waiter queue)

## FFI reference (`db_pool_ffi.erl`)

Only operations not provided by `rasa` live in the custom FFI:

| Function | Erlang call | Purpose |
|----------|-------------|---------|
| `ets_first_and_delete/1` | `ets:first` + `ets:lookup` + `ets:delete` | Atomic pop from ordered_set |
| `raw_send/2` | `erlang:send(Pid, Msg)` | Bypass Subject/selector |

`ets_first_and_delete` combines three ETS operations into one FFI call to minimize the race window during concurrent fast-path checkout. This should be upstreamed into `rasa` as a `table.pop_first` operation. It unwraps `rasa`'s opaque `Table` type via `element(2, Tab)` to access the underlying ETS identifier.

Previously handled by custom FFI, now provided by `rasa`:
- Atomics (`atomics_new`, `atomics_sub_get`, `atomics_add_get`, `atomics_get`, `atomics_put`) → `rasa/atomic`
- Unique monotonic integers (`unique_integer_monotonic`) → `rasa/monotonic.unique()`
- Holder CRUD (`holder_new`, `holder_insert`, `holder_lookup`, `holder_delete`) → `rasa/table`

## Key design decisions

1. **No `give_away` / `set_heir`**: The pool actor always owns all holder tables. Public access means any process can read/write. This eliminates ownership transfer races entirely. The original pgo_pool.erl uses `give_away`/`set_heir` for crash recovery, but we use monitor-based recovery instead, which is simpler and sufficient.

2. **Single-record holders**: Each holder stores just the connection value (`{Nil, conn}`). Previous versions stored 4 key-value pairs (`Conn`, `CheckoutTime`, `DeadlineMs`, `PoolPid`) but these were dead writes — the information is already tracked in `Active`, `Waiting`, and `Shared` records. The simplified design avoids `rasa/table`'s single-value-type constraint.

3. **Client-side checkin**: The client inserts the holder into idle_queue and increments pool_counter before notifying the actor. This is the critical optimization — without it, every checkin blocks on the actor mailbox and the next checkout can't find the connection until the actor processes it.

4. **Per-checkout deadlines**: Each `checkout` call specifies its own deadline in milliseconds. The actor sets a timer. The `DeadlineExpired` handler validates `checkout_time` to guard against stale timers. It also checks `checkout_map` to detect if the client already checked in (race with client-side checkin).

5. **`checkout_map` as race detector**: Both `deadline_expired` and `caller_down` check the checkout_map before destroying a holder. If the caller already removed themselves from checkout_map (via client-side checkin), the holder is in idle_queue and must not be destroyed.

6. **`monotonic.unique()`** for idle_queue keys instead of `monotonic_time` because concurrent client-side checkins need guaranteed unique keys.

7. **Inlined modules**: `HolderRef` and `PoolCounter` are defined in `state.gleam` rather than separate modules. They're small (4 functions each) and tightly coupled to the state machine. Separate modules added import ceremony without meaningful encapsulation.

## Performance

Benchmarked with pool=10, workers=10, hold=0ms (pure checkout+checkin throughput):

| Metric | Value |
|--------|-------|
| p50 | 1us |
| p95 | 2us |
| p99 | 3us |
| Throughput | ~554k ops/sec |

The actor mailbox receives ~2 fire-and-forget messages per operation (`FastCheckoutNotify` + `CheckIn`). Under sustained high throughput this creates a large mailbox backlog, but it doesn't affect latency because the hot path never waits on the actor.

Full benchmark results (4 scenarios):

| Scenario | Throughput | p50 | p95 | p99 | Error rate |
|----------|-----------|-----|-----|-----|------------|
| Uncontended (pool=10, workers=10, hold=0ms) | 554k ops/sec | 1us | 2us | 3us | 0.0% |
| Contended (pool=10, workers=50, hold=2ms) | 2.7k ops/sec | 3.0ms | 3.2ms | 200ms | 0.5% |
| Overloaded (pool=5, workers=100, hold=5ms) | 1.3k ops/sec | 6.0ms | 6.4ms | 750ms | 37.3% |
| Deadline pressure (pool=5, workers=20, hold=50ms) | 378 ops/sec | 51ms | 62ms | 73ms | 0.0% |
