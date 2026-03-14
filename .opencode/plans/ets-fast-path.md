# ETS Fast-Path Checkout Plan

## Problem

Every checkout and checkin serializes through the actor mailbox. Uncontended p50 is ~6.5ms vs ~0.1-0.3ms in pgo_pool. The mailbox is the bottleneck.

## Solution

Port pgo_pool.erl's "holder" pattern with simplifications:
- Each connection lives in its own per-connection ETS table ("holder") via `rasa/table`
- An atomic counter tracks idle count; clients atomically decrement it to claim a checkout slot
- Public tables — no `give_away`/`set_heir` needed; any process can read/write
- Monitor-based crash recovery instead of heir mechanism

## Decisions (confirmed)

| Decision | Choice |
|----------|--------|
| Fast-path coordination | Atomic counter via `rasa/atomic` |
| Checkin mechanism | Client-side insert to idle_queue + counter increment (no `give_away`) |
| Holder access | Public (unnamed ETS tables via `rasa/table`) |
| Holder contents | Single record: just the connection value |
| Crash recovery | Monitor-based (not heir-based) |
| Module structure | `HolderRef` and `PoolCounter` inlined into `state.gleam` |

---

## Module structure

Everything lives in `state.gleam`. No separate `holder.gleam` or `pool_counter.gleam` — they were too small (4 functions each) to justify their own modules.

### HolderRef (`state.gleam`)

```
HolderRef(conn) — opaque, wraps rasa/table.Table(Nil, conn)

Functions:
  new_holder(conn) — create unnamed public set table, insert connection
  get_conn(ref) — read connection value
  store_conn(ref, conn) — overwrite connection value
  destroy_holder(ref) — delete the ETS table
```

Single-record design: stores `{Nil, conn}`. Previous multi-key pattern (`Conn | CheckoutTime | DeadlineMs | PoolPid`) was dropped because those fields were dead writes — already tracked in `Active`, `Waiting`, and `Shared`.

### PoolCounter (`state.gleam`)

```
PoolCounter — opaque, wraps rasa/atomic.Atomic

Functions:
  new_pool_counter(initial) — create counter starting at initial
  counter_checkout(counter) -> Result(Int, Nil) — atomic.sub_get; undo if negative
  counter_checkin(counter) -> Int — atomic.add_get
  counter_get(counter) -> Int — atomic.get
```

### FFI (`db_pool_ffi.erl`)

Only two functions remain:

- `ets_first_and_delete/1` — atomic pop-first from ordered_set (should be upstreamed into `rasa` as `table.pop_first`)
- `raw_send/2` — `erlang:send/2` wrapper

Previously in FFI, now handled by `rasa`: atomics, unique monotonic integers, holder CRUD.

---

## State Machine

### Shared state

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

### Active record

```gleam
pub opaque type Active(conn) {
  Active(
    holder: HolderRef(conn),
    monitor: process.Monitor,
    deadline_timer: process.Timer,
    checkout_time: Int,
  )
}
```

### Pool modes via atomic counter

No explicit mode field. Counter encodes mode:
- `counter > 0` → Ready (idle conns available)
- `counter <= 0` → Busy (all conns checked out or waiters queued)

---

## Checkout Flow

### Fast Path (client-side, no actor)

1. `counter_checkout(counter)` — atomic decrement via `rasa/atomic`
2. If Ok: `ets_first_and_delete(idle_queue)` — pop oldest idle holder
3. `table.insert(checkout_map, caller, holder_ref)`
4. Send `FastCheckoutNotify(caller, holder_ref, deadline)` to pool (for monitor + deadline setup)
5. `get_conn(holder_ref)` — read connection

Race fallback: if idle_queue empty despite counter, undo counter and go to slow path.

### Slow Path (through actor)

Same as current: `process.call_forever(pool, CheckOut(...))` → actor enqueues waiter → on checkin, pool serves waiter via `codel_dequeue` → `serve_waiter`.

---

## Checkin Flow

### Client-side

1. Look up `HolderRef` from `checkout_map` by caller Pid
2. `table.delete(checkout_map, caller)`
3. `key = monotonic.unique()`, `table.insert(idle_queue, key, holder_ref)`
4. `counter_checkin()` — atomic increment
5. `send(pool_subject, CheckIn(caller, holder_ref))`

Steps 2-4 make the connection immediately available. No actor round-trip needed.

### Actor-side (`checkin_notify`)

1. Cancel deadline timer, demonitor, remove from active dict
2. If waiters in queue: reclaim idle holder (`counter_checkout` + `ets_first_and_delete`), feed through `codel_dequeue`
3. If no waiters: no-op (holder already idle from client-side insert)

---

## Crash Recovery

Monitor-based, not heir-based:

1. Client crashes → DOWN signal received by pool actor
2. Pool checks `checkout_map` — if caller already checked in (race), just clean up actor state
3. If still checked out: close connection, destroy holder, open replacement, feed through `codel_dequeue`
4. If `handle_open` fails: shrink pool by 1

Erlang guarantees: messages sent by a process arrive before its DOWN signal. So `CheckIn` is always processed before `CallerDown`.

---

## CoDel Integration

CoDel logic unchanged algorithmically. The waiter queue uses `rasa/queue` (ETS-backed FIFO). `codel_dequeue` dispatches to dequeue_first/dequeue_fast/dequeue_slow based on timing. `serve_waiter` checks `process.is_alive` before handing off.

---

## Deadline Integration

Deadlines handled on actor side:
1. Fast-path sends `FastCheckoutNotify` to pool
2. Pool sets up monitor + deadline timer
3. On fire: validate checkout_time, check checkout_map (race detection), close conn, open replacement, codel_dequeue

---

## Implementation Phases (as executed)

### Phase 1: Holder + PoolCounter + FFI (done)
- Created `rasa/table`-based `HolderRef(conn)` replacing custom Erlang FFI
- Created `PoolCounter` wrapping `rasa/atomic`
- Removed `holder_new/0`, `holder_insert/3`, `holder_lookup/2`, `holder_delete/1` from FFI
- Threaded `conn` type parameter through `Active`, `Shared`, `State`, `PoolHandle`, `Message`
- Removed dead writes (`store_checkout_time`, `store_deadline`, `store_pool_pid`)

### Phase 2: Inline into state.gleam (done)
- Moved `HolderRef` and `PoolCounter` from separate modules into `state.gleam`
- Flattened function names (e.g. `holder.new` → `new_holder`)
- Merged test files into `state_test.gleam`
- Deleted `holder.gleam`, `pool_counter.gleam`, `holder_test.gleam`, `pool_counter_test.gleam`
- All 37 tests pass, benchmarks show no regression

### Future: Upstream `ets_first_and_delete`
- Propose `table.pop_first` for `rasa` to eliminate the last custom FFI coupling to rasa internals

---

## Performance

| Scenario | Throughput | p50 | p95 | p99 | Error rate |
|----------|-----------|-----|-----|-----|------------|
| Uncontended (pool=10, workers=10, hold=0ms) | 554k ops/sec | 1us | 2us | 3us | 0.0% |
| Contended (pool=10, workers=50, hold=2ms) | 2.7k ops/sec | 3.0ms | 3.2ms | 200ms | 0.5% |
| Overloaded (pool=5, workers=100, hold=5ms) | 1.3k ops/sec | 6.0ms | 6.4ms | 750ms | 37.3% |
| Deadline pressure (pool=5, workers=20, hold=50ms) | 378 ops/sec | 51ms | 62ms | 73ms | 0.0% |

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Atom exhaustion | Unnamed ETS tables for holders (refs, not atoms); `rasa/table` supports both named and unnamed |
| Race conditions | Atomic counter (`rasa/atomic`) + idempotent ETS ops + fallback to slow path |
| Memory leak (orphaned holders) | Monitor-based crash recovery + deadline timers |
| Breaking public API | PoolHandle wraps existing Subject; migration is additive |
