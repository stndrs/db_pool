import db_pool/internal/codel
import db_pool/internal/time.{type Instant}
import gleam/time/duration.{type Duration}

const target_ms = 10

const interval_ms = 100

fn at(milliseconds: Int) -> Instant {
  time.from_nanoseconds(milliseconds * 1_000_000)
}

fn ms(milliseconds: Int) -> Duration {
  duration.milliseconds(milliseconds)
}

fn new_codel() -> codel.Codel(String) {
  codel.new(ms(target_ms), ms(interval_ms), at(0))
}

// --- Fast mode ---

pub fn new_starts_in_fast_mode_test() {
  let c = new_codel()

  let assert Ok(_) = codel.push(c, "old", at(0))

  let #(_c, outcome) = codel.dequeue(c, at(50))

  assert outcome == codel.Serve(item: "old", dropped: [])
}

pub fn fast_mode_serves_head_in_fifo_order_test() {
  let c = new_codel()

  let assert Ok(_) = codel.push(c, "a", at(0))
  let assert Ok(_) = codel.push(c, "b", at(0))

  let #(c, first) = codel.dequeue(c, at(10))
  let #(c, second) = codel.dequeue(c, at(20))
  let #(_c, third) = codel.dequeue(c, at(30))

  assert first == codel.Serve(item: "a", dropped: [])
  assert second == codel.Serve(item: "b", dropped: [])
  assert third == codel.Empty(dropped: [])
}

pub fn fast_mode_does_not_raise_recorded_delay_test() {
  let c = new_codel()

  // The boundary at 100ms records a 5ms delay, within the 10ms target.
  let assert Ok(_) = codel.push(c, "first", at(95))
  let #(c, _) = codel.dequeue(c, at(100))

  // A fast-mode dequeue observing 50ms must leave the 5ms on record. Were
  // it to replace it, the recorded delay would cross the target.
  let assert Ok(_) = codel.push(c, "older", at(100))
  let #(c, _) = codel.dequeue(c, at(150))

  // So the next boundary stays in fast mode, and serves a waiter that slow
  // mode would have shed.
  let #(c, _) = codel.dequeue(c, at(200))
  let assert Ok(_) = codel.push(c, "old", at(200))

  let #(_c, outcome) = codel.dequeue(c, at(250))

  assert outcome == codel.Serve(item: "old", dropped: [])
}

pub fn fast_mode_lowers_recorded_delay_test() {
  let c = new_codel()

  // The boundary at 100ms records a 100ms delay.
  let assert Ok(_) = codel.push(c, "first", at(0))
  let #(c, _) = codel.dequeue(c, at(100))

  // A fast-mode dequeue observing 5ms replaces it.
  let assert Ok(_) = codel.push(c, "quick", at(145))
  let #(c, _) = codel.dequeue(c, at(150))

  // So the next boundary sees 5ms, stays in fast mode, and serves a waiter
  // that slow mode would have shed.
  let #(c, _) = codel.dequeue(c, at(200))
  let assert Ok(_) = codel.push(c, "old", at(200))

  let #(_c, outcome) = codel.dequeue(c, at(250))

  assert outcome == codel.Serve(item: "old", dropped: [])
}

// --- Interval boundaries ---

pub fn boundary_enters_slow_when_previous_delay_exceeds_target_test() {
  let c = new_codel()

  let assert Ok(_) = codel.push(c, "first", at(0))
  let #(c, _) = codel.dequeue(c, at(100))

  // The boundary at 200ms judges the interval that just ended - a 100ms
  // delay against a 10ms target - and enters slow mode.
  let #(c, at_boundary) = codel.dequeue(c, at(200))

  assert at_boundary == codel.Empty(dropped: [])

  let assert Ok(_) = codel.push(c, "stale", at(200))

  let #(_c, outcome) = codel.dequeue(c, at(250))

  assert outcome == codel.Empty(dropped: ["stale"])
}

pub fn boundary_leaves_slow_when_previous_delay_within_target_test() {
  let c = new_codel()

  let assert Ok(_) = codel.push(c, "first", at(0))
  let #(c, _) = codel.dequeue(c, at(100))
  let #(c, _) = codel.dequeue(c, at(200))

  // In slow mode with a 0ms delay on record, the boundary at 300ms returns
  // to fast mode.
  let assert Ok(_) = codel.push(c, "fresh", at(295))
  let #(c, _) = codel.dequeue(c, at(300))

  let assert Ok(_) = codel.push(c, "old", at(300))

  let #(_c, outcome) = codel.dequeue(c, at(350))

  assert outcome == codel.Serve(item: "old", dropped: [])
}

pub fn boundary_with_empty_queue_resets_delay_test() {
  let c = new_codel()

  let assert Ok(_) = codel.push(c, "first", at(0))
  let #(c, _) = codel.dequeue(c, at(100))

  // Enters slow mode on the 100ms delay, and resets the delay to zero.
  let #(c, _) = codel.dequeue(c, at(200))
  // Which is within target, so this boundary leaves slow mode again.
  let #(c, _) = codel.dequeue(c, at(300))

  let assert Ok(_) = codel.push(c, "old", at(300))

  let #(_c, outcome) = codel.dequeue(c, at(350))

  assert outcome == codel.Serve(item: "old", dropped: [])
}

pub fn boundary_advances_next_from_now_test() {
  let c = new_codel()

  let assert Ok(_) = codel.push(c, "first", at(0))
  let #(c, _) = codel.dequeue(c, at(100))

  // The interval ends at 200ms but is not crossed until 250ms, so the next
  // one ends at 350ms rather than 300ms.
  let #(c, _) = codel.dequeue(c, at(250))

  let assert Ok(_) = codel.push(c, "stale", at(250))

  // At 300ms this is therefore still slow mode, which sheds the waiter. Had
  // the interval ended at 300ms this would have been a boundary, which
  // serves.
  let #(_c, outcome) = codel.dequeue(c, at(300))

  assert outcome == codel.Empty(dropped: ["stale"])
}

// --- Slow mode ---

pub fn slow_mode_drops_stale_and_serves_fresh_test() {
  let c = enter_slow_mode()

  let assert Ok(_) = codel.push(c, "stale-1", at(200))
  let assert Ok(_) = codel.push(c, "stale-2", at(200))
  let assert Ok(_) = codel.push(c, "fresh", at(245))

  let #(_c, outcome) = codel.dequeue(c, at(250))

  assert outcome == codel.Serve(item: "fresh", dropped: ["stale-1", "stale-2"])
}

pub fn slow_mode_all_stale_returns_empty_with_drops_test() {
  let c = enter_slow_mode()

  let assert Ok(_) = codel.push(c, "stale-1", at(200))
  let assert Ok(_) = codel.push(c, "stale-2", at(200))

  let #(_c, outcome) = codel.dequeue(c, at(250))

  assert outcome == codel.Empty(dropped: ["stale-1", "stale-2"])
}

pub fn slow_mode_keeps_entry_at_exactly_two_targets_test() {
  let c = enter_slow_mode()

  let assert Ok(_) = codel.push(c, "edge", at(230))

  let #(_c, outcome) = codel.dequeue(c, at(250))

  assert outcome == codel.Serve(item: "edge", dropped: [])
}

fn enter_slow_mode() -> codel.Codel(String) {
  let c = new_codel()

  let assert Ok(_) = codel.push(c, "first", at(0))
  // Records a 100ms delay.
  let #(c, _) = codel.dequeue(c, at(100))
  // Which is above target, so this boundary enters slow mode.
  let #(c, _) = codel.dequeue(c, at(200))

  c
}

// --- Polling ---

pub fn poll_before_next_is_noop_test() {
  let c = new_codel()

  // The boundary at 100ms records a 100ms delay, above the 10ms target, so
  // an interval evaluated now would enter slow mode and start shedding.
  let assert Ok(_) = codel.push(c, "first", at(0))
  let #(c, _) = codel.dequeue(c, at(100))

  let assert Ok(key) = codel.push(c, "stale", at(100))

  // But the interval does not end until 200ms, so this poll does nothing.
  let #(c, polled) = codel.poll(c, at(150), key)

  assert polled == codel.Polled(dropped: [], last_key: key)

  // And the waiter it would have shed is still queued, to be served.
  let #(_c, outcome) = codel.dequeue(c, at(160))

  assert outcome == codel.Serve(item: "stale", dropped: [])
}

pub fn poll_with_unchanged_head_evaluates_interval_test() {
  let c = new_codel()

  // Records a 100ms delay; the interval now ends at 200ms.
  let assert Ok(_) = codel.push(c, "first", at(0))
  let #(c, _) = codel.dequeue(c, at(100))

  let assert Ok(key) = codel.push(c, "stale", at(100))

  let #(_c, polled) = codel.poll(c, at(250), key)

  assert polled == codel.Polled(dropped: ["stale"], last_key: key)
}

pub fn poll_advances_next_by_one_interval_test() {
  let c = new_codel()

  let assert Ok(_) = codel.push(c, "first", at(0))
  let #(c, _) = codel.dequeue(c, at(100))

  let assert Ok(key) = codel.push(c, "stale", at(100))

  // The interval ended at 200ms, so this poll at 250ms moves it to 300ms,
  // not to 350ms.
  let #(c, _) = codel.poll(c, at(250), key)

  let assert Ok(_) = codel.push(c, "next-one", at(280))

  // 310ms is therefore a boundary, which serves. Had the interval ended at
  // 350ms this would have been slow mode, which sheds a 30ms-old waiter.
  let #(_c, outcome) = codel.dequeue(c, at(310))

  assert outcome == codel.Serve(item: "next-one", dropped: [])
}

pub fn poll_with_new_head_only_rearms_test() {
  let c = new_codel()

  let assert Ok(first_key) = codel.push(c, "first", at(0))
  let #(c, _) = codel.dequeue(c, at(100))

  let assert Ok(second_key) = codel.push(c, "second", at(100))

  let #(_c, polled) = codel.poll(c, at(250), first_key)

  assert polled == codel.Polled(dropped: [], last_key: second_key)
}

pub fn poll_leaves_slow_mode_when_recorded_delay_within_target_test() {
  // Slow mode, a delay of zero on record, and an interval ending at 300ms.
  let c = enter_slow_mode()

  let assert Ok(key) = codel.push(c, "old", at(200))

  // The poll observes a 100ms delay, but the 0ms on record is what it judges,
  // so it returns to fast mode and sheds nobody.
  let #(c, polled) = codel.poll(c, at(300), key)

  assert polled == codel.Polled(dropped: [], last_key: key)

  // And in fast mode the waiter slow mode would have shed is served.
  let #(_c, outcome) = codel.dequeue(c, at(350))

  assert outcome == codel.Serve(item: "old", dropped: [])
}

pub fn poll_in_fast_mode_advances_next_test() {
  let c = new_codel()

  let assert Ok(_) = codel.push(c, "a", at(95))
  let assert Ok(key) = codel.push(c, "b", at(95))

  // The boundary at 100ms records a 5ms delay, within target.
  let #(c, _) = codel.dequeue(c, at(100))

  // So the poll at the 200ms boundary stays in fast mode and moves the
  // interval to 300ms, not to 400ms and not leaving it at 200ms.
  let #(c, polled) = codel.poll(c, at(200), key)

  assert polled == codel.Polled(dropped: [], last_key: key)

  // Fast mode, so this serves rather than shedding a 155ms-old waiter.
  let #(c, second) = codel.dequeue(c, at(250))

  assert second == codel.Serve(item: "b", dropped: [])

  let assert Ok(_) = codel.push(c, "c", at(250))

  // Still before the boundary at 300ms, and still fast. Had the poll left
  // `next` at 200ms, the dequeue above would have been a boundary that
  // entered slow mode on the 105ms recorded delay, and this would shed.
  let #(_c, third) = codel.dequeue(c, at(280))

  assert third == codel.Serve(item: "c", dropped: [])
}

pub fn poll_records_observed_delay_when_entering_slow_mode_test() {
  let c = new_codel()

  // The boundary at 100ms records a 100ms delay; the interval ends at 200ms.
  let assert Ok(_) = codel.push(c, "first", at(0))
  let #(c, _) = codel.dequeue(c, at(100))

  let assert Ok(key) = codel.push(c, "fresh", at(195))

  // The poll at the 200ms boundary enters slow mode on the 100ms on record,
  // sheds nobody - the head has waited only 5ms - and records that 5ms.
  let #(c, polled) = codel.poll(c, at(200), key)

  assert polled == codel.Polled(dropped: [], last_key: key)

  // The boundary at 300ms therefore judges 5ms, within target, and returns
  // to fast mode. Had the poll left the 100ms on record it would have stayed
  // slow and shed the waiter below.
  let #(c, _) = codel.dequeue(c, at(300))

  let assert Ok(_) = codel.push(c, "old", at(300))

  let #(_c, outcome) = codel.dequeue(c, at(350))

  assert outcome == codel.Serve(item: "old", dropped: [])
}

pub fn poll_keeps_entry_at_exactly_two_targets_test() {
  let c = new_codel()

  // Records a 100ms delay, so the poll below enters slow mode and sheds.
  let assert Ok(_) = codel.push(c, "first", at(0))
  let #(c, _) = codel.dequeue(c, at(100))

  let assert Ok(key) = codel.push(c, "edge", at(230))

  let #(c, polled) = codel.poll(c, at(250), key)

  assert polled == codel.Polled(dropped: [], last_key: key)
  assert codel.first(c) == Ok(codel.Entry(sent_at: at(230), item: "edge"))
}

pub fn poll_drops_stale_oldest_first_test() {
  let c = new_codel()

  // Records a 100ms delay, so the poll below enters slow mode.
  let assert Ok(_) = codel.push(c, "first", at(0))
  let #(c, _) = codel.dequeue(c, at(100))

  let assert Ok(key) = codel.push(c, "stale-1", at(100))
  let assert Ok(_) = codel.push(c, "stale-2", at(150))
  let assert Ok(_) = codel.push(c, "fresh", at(245))

  let #(c, polled) = codel.poll(c, at(250), key)

  assert polled == codel.Polled(dropped: ["stale-1", "stale-2"], last_key: key)

  // The first entry that is not stale stopped the shedding, and is still
  // queued.
  assert codel.first(c) == Ok(codel.Entry(sent_at: at(245), item: "fresh"))
}

pub fn poll_drops_whole_queue_oldest_first_test() {
  let c = new_codel()

  let assert Ok(_) = codel.push(c, "first", at(0))
  let #(c, _) = codel.dequeue(c, at(100))

  let assert Ok(key) = codel.push(c, "stale-1", at(100))
  let assert Ok(_) = codel.push(c, "stale-2", at(150))

  let #(c, polled) = codel.poll(c, at(250), key)

  assert polled == codel.Polled(dropped: ["stale-1", "stale-2"], last_key: key)
  assert codel.first(c) == Error(Nil)
}

pub fn poll_with_empty_queue_returns_last_key_test() {
  let c: codel.Codel(String) = new_codel()

  let #(_c, polled) = codel.poll(c, at(500), 42)

  assert polled == codel.Polled(dropped: [], last_key: 42)
}

// --- Queue access ---

pub fn at_and_delete_roundtrip_and_missing_key_test() {
  let c = new_codel()

  let assert Ok(head_key) = codel.push(c, "head", at(5))
  let assert Ok(key) = codel.push(c, "a", at(7))

  assert codel.at(c, head_key) == Ok(codel.Entry(sent_at: at(5), item: "head"))
  assert codel.at(c, key) == Ok(codel.Entry(sent_at: at(7), item: "a"))

  assert codel.delete(c, key) == Ok(Nil)
  assert codel.at(c, key) == Error(Nil)

  // Deleting the second entry left the head where it was.
  assert codel.at(c, head_key) == Ok(codel.Entry(sent_at: at(5), item: "head"))

  // Deleting a key that is no longer queued still succeeds.
  assert codel.delete(c, key) == Ok(Nil)
}

pub fn first_and_pop_all_test() {
  let c: codel.Codel(String) = new_codel()

  assert codel.first(c) == Error(Nil)
  assert codel.pop_all(c) == []

  let assert Ok(_) = codel.push(c, "a", at(1))
  let assert Ok(_) = codel.push(c, "b", at(2))
  let assert Ok(_) = codel.push(c, "c", at(3))

  assert codel.first(c) == Ok(codel.Entry(sent_at: at(1), item: "a"))
  assert codel.pop_all(c) == ["a", "b", "c"]
  assert codel.first(c) == Error(Nil)
}

pub fn push_returns_increasing_keys_test() {
  let c = new_codel()

  let assert Ok(first) = codel.push(c, "a", at(0))
  let assert Ok(second) = codel.push(c, "b", at(0))

  assert second > first
}
