//// The CoDel (Controlled Delay) algorithm used to manage the pool's queue of
//// waiting callers.
////
//// This module owns the queue and the algorithm's state. It returns
//// decisions. Carrying out those decisions is left up to the caller.
//// Time is always passed in, so the algorithm can be driven directly by tests.
////
//// The queue is ETS backed, so a `Codel` is not a value. Every copy shares one
//// queue, and a push or pop through any copy is visible to all of them.

import gleam/bool
import gleam/list
import gleam/result
import rasa/counter
import rasa/queue.{type Queue}
import rasa/table

const ns_per_ms = 1_000_000

pub opaque type Codel(a) {
  Codel(
    queue: Queue(Entry(a)),
    // The maximum acceptable queue delay, in nanoseconds.
    target: Int,
    // The length of a measurement interval, in nanoseconds.
    interval: Int,
    // The minimum delay observed during the current interval.
    delay: Int,
    slow: Bool,
    // The timestamp at which the current interval ends.
    next: Int,
  )
}

/// A queued item paired with the timestamp at which it was pushed.
pub type Entry(a) {
  Entry(sent_at: Int, item: a)
}

/// The result of a `dequeue`. `dropped` holds the items dropped on the way
/// to the decision, starting with the oldest. Dropped items are only populated
/// by the slow dequeue path.
pub type Outcome(a) {
  Serve(item: a, dropped: List(a))
  Empty(dropped: List(a))
}

/// The result of a `poll`. `dropped` holds the items shed, starting with the
/// oldest. Dropped items are only populated by the slow dequeue path.
/// `last_key` is the key the next poll should be armed with.
pub type Polled(a) {
  Polled(dropped: List(a), last_key: Int)
}

/// Returns a `Codel` with an empty queue, in fast mode, whose first
/// measurement interval ends one interval after `now`.
///
/// `target_ms` and `interval_ms` are in milliseconds; every timestamp the
/// module is subsequently passed is in nanoseconds.
pub fn new(target_ms: Int, interval_ms: Int, now: Int) -> Codel(a) {
  let interval = interval_ms * ns_per_ms

  let queue =
    queue.new()
    |> queue.with_access(table.Private)
    // A strictly unique, monotonically increasing counter for queue keys.
    |> queue.with_counter(counter.monotonic())
    |> queue.build

  Codel(
    queue:,
    target: target_ms * ns_per_ms,
    interval:,
    delay: 0,
    slow: False,
    next: now + interval,
  )
}

/// Pushes an item onto the back of the queue, stamped with `now`, and returns
/// its key. Errors only if the key counter hands back a key already in use.
pub fn push(codel: Codel(a), item: a, now: Int) -> Result(Int, Nil) {
  queue.push(codel.queue, Entry(sent_at: now, item:))
}

/// Returns the entry at `key` without removing it.
pub fn at(codel: Codel(a), key: Int) -> Result(Entry(a), Nil) {
  queue.at(codel.queue, key)
}

/// Removes the entry at `key`. Succeeds whether or not the key is present.
pub fn delete(codel: Codel(a), key: Int) -> Result(Nil, Nil) {
  queue.delete(codel.queue, key)
}

/// Returns the oldest entry without removing it.
pub fn first(codel: Codel(a)) -> Result(Entry(a), Nil) {
  queue.first(codel.queue)
  |> result.map(fn(found) { found.1 })
}

/// Empties the queue, returning every item in it, oldest first. Used to drain
/// the queue at shutdown, so the algorithm is deliberately not consulted.
pub fn pop_all(codel: Codel(a)) -> List(a) {
  do_pop_all(codel, [])
}

fn do_pop_all(codel: Codel(a), popped: List(a)) -> List(a) {
  case queue.pop(codel.queue) {
    Ok(entry) -> do_pop_all(codel, [entry.item, ..popped])
    Error(_) -> list.reverse(popped)
  }
}

/// Takes the next item to serve.
///
/// At an interval boundary the interval is rolled over and the mode is
/// recomputed. Otherwise fast mode serves the oldest item immediately, and
/// slow mode sheds items that have waited longer than twice the target before
/// serving the first one that has not.
pub fn dequeue(codel: Codel(a), now: Int) -> #(Codel(a), Outcome(a)) {
  case now >= codel.next, codel.slow {
    True, _ -> dequeue_boundary(codel, now)
    False, False -> dequeue_fast(codel, now)
    False, True -> dequeue_slow(codel, now, [])
  }
}

// At an interval boundary the mode for the interval starting now is decided by
// the delay recorded over the interval that just ended, and the recorded delay
// is reset to whatever this dequeue observes. An empty queue resets it to zero.
fn dequeue_boundary(codel: Codel(a), now: Int) -> #(Codel(a), Outcome(a)) {
  let next = now + codel.interval
  let slow = codel.delay > codel.target

  queue.pop(codel.queue)
  |> result.map(fn(entry) {
    #(
      Codel(..codel, next:, slow:, delay: now - entry.sent_at),
      Serve(item: entry.item, dropped: []),
    )
  })
  |> result.lazy_unwrap(fn() {
    #(Codel(..codel, next:, slow:, delay: 0), Empty(dropped: []))
  })
}

fn dequeue_fast(codel: Codel(a), now: Int) -> #(Codel(a), Outcome(a)) {
  queue.pop(codel.queue)
  |> result.map(fn(entry) {
    #(observe(codel, now - entry.sent_at), Serve(item: entry.item, dropped: []))
  })
  |> result.lazy_unwrap(fn() { #(codel, Empty(dropped: [])) })
}

fn dequeue_slow(
  codel: Codel(a),
  now: Int,
  dropped: List(a),
) -> #(Codel(a), Outcome(a)) {
  queue.pop(codel.queue)
  |> result.map(fn(entry) {
    case now - entry.sent_at > codel.target * 2 {
      True -> dequeue_slow(codel, now, [entry.item, ..dropped])
      False -> #(
        observe(codel, now - entry.sent_at),
        Serve(item: entry.item, dropped: list.reverse(dropped)),
      )
    }
  })
  |> result.lazy_unwrap(fn() { #(codel, Empty(dropped: list.reverse(dropped))) })
}

// Records a delay observed away from an interval boundary. Only a smaller
// delay is kept. The interval's figure is the minimum seen during it, and only
// a boundary may raise or reset it.
fn observe(codel: Codel(a), delay: Int) -> Codel(a) {
  case delay < codel.delay {
    True -> Codel(..codel, delay:)
    False -> codel
  }
}

/// Evaluates the queue without serving anyone, and returns the key to arm the
/// next poll with.
///
/// The interval is only evaluated when the head of the queue is the same entry
/// the previous poll saw (its key has not advanced past `last_key`). A newer
/// head means the queue turned over since that poll, so there is no standing
/// delay to judge.
pub fn poll(
  codel: Codel(a),
  now: Int,
  last_key: Int,
) -> #(Codel(a), Polled(a)) {
  case queue.first(codel.queue) {
    Ok(#(key, entry)) if key <= last_key -> {
      let #(codel, dropped) = interval_elapsed(codel, now, now - entry.sent_at)

      #(codel, Polled(dropped:, last_key: key))
    }
    Ok(#(key, _entry)) -> #(codel, Polled(dropped: [], last_key: key))
    Error(_) -> #(codel, Polled(dropped: [], last_key:))
  }
}

// The poll-side interval rollover. Unlike `dequeue_boundary` this advances
// `next` by one interval rather than restarting it from `now`.
fn interval_elapsed(
  codel: Codel(a),
  now: Int,
  delay: Int,
) -> #(Codel(a), List(a)) {
  use <- bool.guard(when: now < codel.next, return: #(codel, []))

  let next = codel.next + codel.interval

  case codel.delay > codel.target {
    True -> {
      let codel = Codel(..codel, slow: True, delay:, next:)

      #(codel, drop_stale(codel, now, []))
    }
    False -> #(Codel(..codel, slow: False, delay:, next:), [])
  }
}

// Sheds entries from the head of the queue for as long as they have waited
// longer than twice the target.
fn drop_stale(codel: Codel(a), now: Int, dropped: List(a)) -> List(a) {
  queue.first(codel.queue)
  |> result.map(fn(enqueued) {
    let #(key, entry) = enqueued

    case now - entry.sent_at > codel.target * 2 {
      False -> list.reverse(dropped)
      True -> {
        let _ = queue.delete(codel.queue, key)

        drop_stale(codel, now, [entry.item, ..dropped])
      }
    }
  })
  |> result.lazy_unwrap(fn() { list.reverse(dropped) })
}
