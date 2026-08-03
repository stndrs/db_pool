import db_pool/internal/codel

const ms = 1_000_000

const target_ms = 10

const interval_ms = 100

// --- Fast mode ---

pub fn new_starts_in_fast_mode_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  let assert Ok(_) = codel.push(c, "old", 0)

  let #(_c, outcome) = codel.dequeue(c, 50 * ms)

  assert outcome == codel.Serve(item: "old", dropped: [])
}

pub fn fast_mode_serves_head_in_fifo_order_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  let assert Ok(_) = codel.push(c, "a", 0)
  let assert Ok(_) = codel.push(c, "b", 0)

  let #(c, first) = codel.dequeue(c, 10 * ms)
  let #(c, second) = codel.dequeue(c, 20 * ms)
  let #(_c, third) = codel.dequeue(c, 30 * ms)

  assert first == codel.Serve(item: "a", dropped: [])
  assert second == codel.Serve(item: "b", dropped: [])
  assert third == codel.Empty(dropped: [])
}

/// The interval's recorded delay is the minimum seen during it, so an
/// observation larger than the one on record must not replace it.
pub fn fast_mode_does_not_raise_recorded_delay_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  // The boundary at 100ms records a 5ms delay, within the 10ms target.
  let assert Ok(_) = codel.push(c, "first", 95 * ms)
  let #(c, _) = codel.dequeue(c, 100 * ms)

  // A fast-mode dequeue observing 50ms must leave the 5ms on record. Were
  // it to replace it, the recorded delay would cross the target.
  let assert Ok(_) = codel.push(c, "older", 100 * ms)
  let #(c, _) = codel.dequeue(c, 150 * ms)

  // So the next boundary stays in fast mode, and serves a waiter that slow
  // mode would have shed.
  let #(c, _) = codel.dequeue(c, 200 * ms)
  let assert Ok(_) = codel.push(c, "old", 200 * ms)

  let #(_c, outcome) = codel.dequeue(c, 250 * ms)

  assert outcome == codel.Serve(item: "old", dropped: [])
}

pub fn fast_mode_lowers_recorded_delay_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  // The boundary at 100ms records a 100ms delay.
  let assert Ok(_) = codel.push(c, "first", 0)
  let #(c, _) = codel.dequeue(c, 100 * ms)

  // A fast-mode dequeue observing 5ms replaces it.
  let assert Ok(_) = codel.push(c, "quick", 145 * ms)
  let #(c, _) = codel.dequeue(c, 150 * ms)

  // So the next boundary sees 5ms, stays in fast mode, and serves a waiter
  // that slow mode would have shed.
  let #(c, _) = codel.dequeue(c, 200 * ms)
  let assert Ok(_) = codel.push(c, "old", 200 * ms)

  let #(_c, outcome) = codel.dequeue(c, 250 * ms)

  assert outcome == codel.Serve(item: "old", dropped: [])
}

// --- Interval boundaries ---

pub fn boundary_enters_slow_when_previous_delay_exceeds_target_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  let assert Ok(_) = codel.push(c, "first", 0)
  let #(c, _) = codel.dequeue(c, 100 * ms)

  // The boundary at 200ms judges the interval that just ended - a 100ms
  // delay against a 10ms target - and enters slow mode.
  let #(c, at_boundary) = codel.dequeue(c, 200 * ms)

  assert at_boundary == codel.Empty(dropped: [])

  let assert Ok(_) = codel.push(c, "stale", 200 * ms)

  let #(_c, outcome) = codel.dequeue(c, 250 * ms)

  assert outcome == codel.Empty(dropped: ["stale"])
}

pub fn boundary_leaves_slow_when_previous_delay_within_target_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  let assert Ok(_) = codel.push(c, "first", 0)
  let #(c, _) = codel.dequeue(c, 100 * ms)
  let #(c, _) = codel.dequeue(c, 200 * ms)

  // In slow mode with a 0ms delay on record, the boundary at 300ms returns
  // to fast mode.
  let assert Ok(_) = codel.push(c, "fresh", 295 * ms)
  let #(c, _) = codel.dequeue(c, 300 * ms)

  let assert Ok(_) = codel.push(c, "old", 300 * ms)

  let #(_c, outcome) = codel.dequeue(c, 350 * ms)

  assert outcome == codel.Serve(item: "old", dropped: [])
}

/// A boundary reached with nothing queued records a delay of zero, so a pool
/// that has gone quiet returns to fast mode on the following boundary.
pub fn boundary_with_empty_queue_resets_delay_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  let assert Ok(_) = codel.push(c, "first", 0)
  let #(c, _) = codel.dequeue(c, 100 * ms)

  // Enters slow mode on the 100ms delay, and resets the delay to zero.
  let #(c, _) = codel.dequeue(c, 200 * ms)
  // Which is within target, so this boundary leaves slow mode again.
  let #(c, _) = codel.dequeue(c, 300 * ms)

  let assert Ok(_) = codel.push(c, "old", 300 * ms)

  let #(_c, outcome) = codel.dequeue(c, 350 * ms)

  assert outcome == codel.Serve(item: "old", dropped: [])
}

/// A boundary reached late restarts the interval from now, rather than
/// advancing it by one interval from where it was.
pub fn boundary_advances_next_from_now_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  let assert Ok(_) = codel.push(c, "first", 0)
  let #(c, _) = codel.dequeue(c, 100 * ms)

  // The interval ends at 200ms but is not crossed until 250ms, so the next
  // one ends at 350ms rather than 300ms.
  let #(c, _) = codel.dequeue(c, 250 * ms)

  let assert Ok(_) = codel.push(c, "stale", 250 * ms)

  // At 300ms this is therefore still slow mode, which sheds the waiter. Had
  // the interval ended at 300ms this would have been a boundary, which
  // serves.
  let #(_c, outcome) = codel.dequeue(c, 300 * ms)

  assert outcome == codel.Empty(dropped: ["stale"])
}

// --- Slow mode ---

pub fn slow_mode_drops_stale_and_serves_fresh_test() {
  let c = enter_slow_mode()

  let assert Ok(_) = codel.push(c, "stale-1", 200 * ms)
  let assert Ok(_) = codel.push(c, "stale-2", 200 * ms)
  let assert Ok(_) = codel.push(c, "fresh", 245 * ms)

  let #(_c, outcome) = codel.dequeue(c, 250 * ms)

  assert outcome == codel.Serve(item: "fresh", dropped: ["stale-1", "stale-2"])
}

pub fn slow_mode_all_stale_returns_empty_with_drops_test() {
  let c = enter_slow_mode()

  let assert Ok(_) = codel.push(c, "stale-1", 200 * ms)
  let assert Ok(_) = codel.push(c, "stale-2", 200 * ms)

  let #(_c, outcome) = codel.dequeue(c, 250 * ms)

  assert outcome == codel.Empty(dropped: ["stale-1", "stale-2"])
}

/// Staleness is strict: an entry sitting at exactly twice the target survives.
pub fn slow_mode_keeps_entry_at_exactly_two_targets_test() {
  let c = enter_slow_mode()

  let assert Ok(_) = codel.push(c, "edge", 230 * ms)

  let #(_c, outcome) = codel.dequeue(c, 250 * ms)

  assert outcome == codel.Serve(item: "edge", dropped: [])
}

// Returns a `Codel` in slow mode whose current interval ends at 300ms, with
// an empty queue and a delay of zero on record.
fn enter_slow_mode() -> codel.Codel(String) {
  let c = codel.new(target_ms, interval_ms, 0)

  let assert Ok(_) = codel.push(c, "first", 0)
  // Records a 100ms delay.
  let #(c, _) = codel.dequeue(c, 100 * ms)
  // Which is above target, so this boundary enters slow mode.
  let #(c, _) = codel.dequeue(c, 200 * ms)

  c
}

// --- Polling ---

pub fn poll_before_next_is_noop_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  // The boundary at 100ms records a 100ms delay, above the 10ms target, so
  // an interval evaluated now would enter slow mode and start shedding.
  let assert Ok(_) = codel.push(c, "first", 0)
  let #(c, _) = codel.dequeue(c, 100 * ms)

  let assert Ok(key) = codel.push(c, "stale", 100 * ms)

  // But the interval does not end until 200ms, so this poll does nothing.
  let #(c, polled) = codel.poll(c, 150 * ms, key)

  assert polled == codel.Polled(dropped: [], last_key: key)

  // And the waiter it would have shed is still queued, to be served.
  let #(_c, outcome) = codel.dequeue(c, 160 * ms)

  assert outcome == codel.Serve(item: "stale", dropped: [])
}

/// When the head of the queue has not changed since the last poll, the poll
/// judges the interval and sheds waiters that have waited too long.
pub fn poll_with_unchanged_head_evaluates_interval_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  // Records a 100ms delay; the interval now ends at 200ms.
  let assert Ok(_) = codel.push(c, "first", 0)
  let #(c, _) = codel.dequeue(c, 100 * ms)

  let assert Ok(key) = codel.push(c, "stale", 100 * ms)

  let #(_c, polled) = codel.poll(c, 250 * ms, key)

  assert polled == codel.Polled(dropped: ["stale"], last_key: key)
}

/// A poll advances the interval by exactly one interval, rather than
/// restarting it from now.
pub fn poll_advances_next_by_one_interval_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  let assert Ok(_) = codel.push(c, "first", 0)
  let #(c, _) = codel.dequeue(c, 100 * ms)

  let assert Ok(key) = codel.push(c, "stale", 100 * ms)

  // The interval ended at 200ms, so this poll at 250ms moves it to 300ms,
  // not to 350ms.
  let #(c, _) = codel.poll(c, 250 * ms, key)

  let assert Ok(_) = codel.push(c, "next-one", 280 * ms)

  // 310ms is therefore a boundary, which serves. Had the interval ended at
  // 350ms this would have been slow mode, which sheds a 30ms-old waiter.
  let #(_c, outcome) = codel.dequeue(c, 310 * ms)

  assert outcome == codel.Serve(item: "next-one", dropped: [])
}

/// A head newer than the one the last poll saw means the queue turned over,
/// so there is no standing delay to judge and nothing is shed.
pub fn poll_with_new_head_only_rearms_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  let assert Ok(first_key) = codel.push(c, "first", 0)
  let #(c, _) = codel.dequeue(c, 100 * ms)

  let assert Ok(second_key) = codel.push(c, "second", 100 * ms)

  let #(_c, polled) = codel.poll(c, 250 * ms, first_key)

  assert polled == codel.Polled(dropped: [], last_key: second_key)
}

/// An interval whose recorded delay is within target returns the queue to
/// fast mode, even though the entry standing at the head right now has waited
/// far longer than target. The mode is decided by the interval that just
/// ended, not by the delay this poll observes.
pub fn poll_leaves_slow_mode_when_recorded_delay_within_target_test() {
  // Slow mode, a delay of zero on record, and an interval ending at 300ms.
  let c = enter_slow_mode()

  let assert Ok(key) = codel.push(c, "old", 200 * ms)

  // The poll observes a 100ms delay, but the 0ms on record is what it judges,
  // so it returns to fast mode and sheds nobody.
  let #(c, polled) = codel.poll(c, 300 * ms, key)

  assert polled == codel.Polled(dropped: [], last_key: key)

  // And in fast mode the waiter slow mode would have shed is served.
  let #(_c, outcome) = codel.dequeue(c, 350 * ms)

  assert outcome == codel.Serve(item: "old", dropped: [])
}

/// A poll that leaves the queue in fast mode still advances the interval by
/// one interval, so the next boundary lands where it would have anyway.
pub fn poll_in_fast_mode_advances_next_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  let assert Ok(_) = codel.push(c, "a", 95 * ms)
  let assert Ok(key) = codel.push(c, "b", 95 * ms)

  // The boundary at 100ms records a 5ms delay, within target.
  let #(c, _) = codel.dequeue(c, 100 * ms)

  // So the poll at the 200ms boundary stays in fast mode and moves the
  // interval to 300ms, not to 400ms and not leaving it at 200ms.
  let #(c, polled) = codel.poll(c, 200 * ms, key)

  assert polled == codel.Polled(dropped: [], last_key: key)

  // Fast mode, so this serves rather than shedding a 155ms-old waiter.
  let #(c, second) = codel.dequeue(c, 250 * ms)

  assert second == codel.Serve(item: "b", dropped: [])

  let assert Ok(_) = codel.push(c, "c", 250 * ms)

  // Still before the boundary at 300ms, and still fast. Had the poll left
  // `next` at 200ms, the dequeue above would have been a boundary that
  // entered slow mode on the 105ms recorded delay, and this would shed.
  let #(_c, third) = codel.dequeue(c, 280 * ms)

  assert third == codel.Serve(item: "c", dropped: [])
}

/// A poll that enters slow mode records the delay it observed, replacing the
/// one that put it there, so an interval that recovers is judged on the new
/// figure.
pub fn poll_records_observed_delay_when_entering_slow_mode_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  // The boundary at 100ms records a 100ms delay; the interval ends at 200ms.
  let assert Ok(_) = codel.push(c, "first", 0)
  let #(c, _) = codel.dequeue(c, 100 * ms)

  let assert Ok(key) = codel.push(c, "fresh", 195 * ms)

  // The poll at the 200ms boundary enters slow mode on the 100ms on record,
  // sheds nobody - the head has waited only 5ms - and records that 5ms.
  let #(c, polled) = codel.poll(c, 200 * ms, key)

  assert polled == codel.Polled(dropped: [], last_key: key)

  // The boundary at 300ms therefore judges 5ms, within target, and returns
  // to fast mode. Had the poll left the 100ms on record it would have stayed
  // slow and shed the waiter below.
  let #(c, _) = codel.dequeue(c, 300 * ms)

  let assert Ok(_) = codel.push(c, "old", 300 * ms)

  let #(_c, outcome) = codel.dequeue(c, 350 * ms)

  assert outcome == codel.Serve(item: "old", dropped: [])
}

/// Poll-side staleness is strict, as it is on the dequeue side: an entry
/// sitting at exactly twice the target is not shed.
pub fn poll_keeps_entry_at_exactly_two_targets_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  // Records a 100ms delay, so the poll below enters slow mode and sheds.
  let assert Ok(_) = codel.push(c, "first", 0)
  let #(c, _) = codel.dequeue(c, 100 * ms)

  let assert Ok(key) = codel.push(c, "edge", 230 * ms)

  let #(c, polled) = codel.poll(c, 250 * ms, key)

  assert polled == codel.Polled(dropped: [], last_key: key)
  assert codel.first(c) == Ok(codel.Entry(sent_at: 230 * ms, item: "edge"))
}

/// A poll sheds from the head, and reports what it shed oldest first. It
/// stops at the first entry that is not stale.
pub fn poll_drops_stale_oldest_first_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  // Records a 100ms delay, so the poll below enters slow mode.
  let assert Ok(_) = codel.push(c, "first", 0)
  let #(c, _) = codel.dequeue(c, 100 * ms)

  let assert Ok(key) = codel.push(c, "stale-1", 100 * ms)
  let assert Ok(_) = codel.push(c, "stale-2", 150 * ms)
  let assert Ok(_) = codel.push(c, "fresh", 245 * ms)

  let #(c, polled) = codel.poll(c, 250 * ms, key)

  assert polled == codel.Polled(dropped: ["stale-1", "stale-2"], last_key: key)

  // The first entry that is not stale stopped the shedding, and is still
  // queued.
  assert codel.first(c) == Ok(codel.Entry(sent_at: 245 * ms, item: "fresh"))
}

/// The same order holds when shedding runs the queue empty rather than
/// stopping at a fresh entry.
pub fn poll_drops_whole_queue_oldest_first_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  let assert Ok(_) = codel.push(c, "first", 0)
  let #(c, _) = codel.dequeue(c, 100 * ms)

  let assert Ok(key) = codel.push(c, "stale-1", 100 * ms)
  let assert Ok(_) = codel.push(c, "stale-2", 150 * ms)

  let #(c, polled) = codel.poll(c, 250 * ms, key)

  assert polled == codel.Polled(dropped: ["stale-1", "stale-2"], last_key: key)
  assert codel.first(c) == Error(Nil)
}

pub fn poll_with_empty_queue_returns_last_key_test() {
  let c: codel.Codel(String) = codel.new(target_ms, interval_ms, 0)

  let #(_c, polled) = codel.poll(c, 500 * ms, 42)

  assert polled == codel.Polled(dropped: [], last_key: 42)
}

// --- Queue access ---

/// `at` and `delete` are pinned to the key they are given, not to the head of
/// the queue, so a later entry is reachable while an older one is still
/// queued ahead of it.
pub fn at_and_delete_roundtrip_and_missing_key_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  let assert Ok(head_key) = codel.push(c, "head", 5 * ms)
  let assert Ok(key) = codel.push(c, "a", 7 * ms)

  assert codel.at(c, head_key) == Ok(codel.Entry(sent_at: 5 * ms, item: "head"))
  assert codel.at(c, key) == Ok(codel.Entry(sent_at: 7 * ms, item: "a"))

  assert codel.delete(c, key) == Ok(Nil)
  assert codel.at(c, key) == Error(Nil)

  // Deleting the second entry left the head where it was.
  assert codel.at(c, head_key) == Ok(codel.Entry(sent_at: 5 * ms, item: "head"))

  // Deleting a key that is no longer queued still succeeds.
  assert codel.delete(c, key) == Ok(Nil)
}

pub fn first_and_pop_all_test() {
  let c: codel.Codel(String) = codel.new(target_ms, interval_ms, 0)

  assert codel.first(c) == Error(Nil)
  assert codel.pop_all(c) == []

  let assert Ok(_) = codel.push(c, "a", 1 * ms)
  let assert Ok(_) = codel.push(c, "b", 2 * ms)
  let assert Ok(_) = codel.push(c, "c", 3 * ms)

  assert codel.first(c) == Ok(codel.Entry(sent_at: 1 * ms, item: "a"))
  assert codel.pop_all(c) == ["a", "b", "c"]
  assert codel.first(c) == Error(Nil)
}

pub fn push_returns_increasing_keys_test() {
  let c = codel.new(target_ms, interval_ms, 0)

  let assert Ok(first) = codel.push(c, "a", 0)
  let assert Ok(second) = codel.push(c, "b", 0)

  assert second > first
}
