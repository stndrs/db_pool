//// Types for the pool's time values.
////
//// Time in this package comes in two kinds. An `Instant` is a point on the
//// monotonic clock; a `Duration` is a span. Subtracting one instant from
//// another gives a duration, advancing an instant by a duration gives another
//// instant, and adding two instants is meaningless and does not compile.
////
//// Durations are `gleam_time`'s. Instants are ours, because `gleam_time`'s
//// `Timestamp` is wall-clock time, which can jump backwards - a pool measuring
//// queue delay needs a reading that only ever increases.
////
//// Millisecond values are what OTP and this package's public API deal in;
//// nanosecond values are what the monotonic clock deals in. Converting between
//// them happens through `gleam/time/duration` and nowhere else.
////
//// Durations in this package are non-negative by construction, which is why
//// the magnitude-ordering of `duration.compare`, `min` and `max` is safe; a
//// duration that could go negative must not be passed through them.

import gleam/int
import gleam/order.{type Order}
import gleam/time/duration.{type Duration}
import rasa/counter
import rasa/monotonic

const ns_per_second = 1_000_000_000

/// A source of monotonic time. Held by the pool actor; reading it is an
/// effect, so modules that must stay pure take `Instant` values as arguments
/// instead of holding a clock.
pub opaque type Clock {
  Clock(counter: counter.Counter)
}

/// A point on the monotonic clock, in nanoseconds. Only meaningful relative to
/// another `Instant` from the same clock.
pub opaque type Instant {
  Instant(nanoseconds: Int)
}

/// Returns a monotonic, nanosecond-resolution clock.
pub fn clock() -> Clock {
  Clock(counter.monotonic_time(monotonic.Nanosecond))
}

/// Reads the current instant from a clock. Successive readings never decrease.
pub fn now(clock: Clock) -> Instant {
  Instant(counter.next(clock.counter))
}

/// Adopts a raw nanosecond reading as an `Instant`. A test-fixture
/// constructor, for building `Instant` values without a real clock.
pub fn from_nanoseconds(nanoseconds: Int) -> Instant {
  Instant(nanoseconds)
}

/// The duration from one instant to another.
///
/// Returns a negative duration when `to` precedes `from`. Note that
/// `duration.compare` orders by magnitude and ignores sign, so a negative
/// duration does not compare as less than a positive one.
pub fn since(from from: Instant, to to: Instant) -> Duration {
  duration.nanoseconds(to.nanoseconds - from.nanoseconds)
}

/// The instant reached by moving forward from `instant` by `span`.
pub fn advance(instant: Instant, by span: Duration) -> Instant {
  Instant(instant.nanoseconds + total_nanoseconds(span))
}

/// Orders two instants on the clock. Unlike `duration.compare`, this is a
/// signed comparison: an earlier instant is always less than a later one.
pub fn compare(left: Instant, right: Instant) -> Order {
  int.compare(left.nanoseconds, right.nanoseconds)
}

/// A duration spanning no time.
pub fn zero() -> Duration {
  duration.nanoseconds(0)
}

/// Twice a duration.
pub fn double(span: Duration) -> Duration {
  duration.add(span, span)
}

/// Half a duration, truncated towards zero.
pub fn halve(span: Duration) -> Duration {
  duration.nanoseconds(total_nanoseconds(span) / 2)
}

/// The shorter of two durations. Orders by magnitude, like
/// `duration.compare`.
pub fn min(left: Duration, right: Duration) -> Duration {
  case duration.compare(left, right) {
    order.Gt -> right
    _ -> left
  }
}

/// The longer of two durations. Orders by magnitude, like `duration.compare`.
pub fn max(left: Duration, right: Duration) -> Duration {
  case duration.compare(left, right) {
    order.Lt -> right
    _ -> left
  }
}

fn total_nanoseconds(span: Duration) -> Int {
  let #(seconds, nanoseconds) = duration.to_seconds_and_nanoseconds(span)

  seconds * ns_per_second + nanoseconds
}
