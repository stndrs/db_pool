import db_pool/internal/time
import gleam/order
import gleam/time/duration

const ms = 1_000_000

// --- Instants ---

pub fn since_and_advance_round_trip_test() {
  let start = time.from_nanoseconds(100 * ms)
  let finish = time.from_nanoseconds(250 * ms)

  let span = time.since(from: start, to: finish)

  assert duration.to_milliseconds(span) == 150
  assert time.advance(start, by: span) == finish
}

pub fn since_backwards_is_negative_test() {
  let start = time.from_nanoseconds(250 * ms)
  let finish = time.from_nanoseconds(100 * ms)

  let span = time.since(from: start, to: finish)

  assert duration.to_milliseconds(span) == -150
  assert duration.compare(span, duration.milliseconds(10)) == order.Gt
}

pub fn compare_orders_instants_test() {
  let earlier = time.from_nanoseconds(100 * ms)
  let later = time.from_nanoseconds(250 * ms)

  assert time.compare(earlier, later) == order.Lt
  assert time.compare(later, earlier) == order.Gt
  assert time.compare(earlier, earlier) == order.Eq
}

pub fn compare_is_signed_test() {
  let before_origin = time.from_nanoseconds(-500 * ms)
  let after_origin = time.from_nanoseconds(500 * ms)

  assert time.compare(before_origin, after_origin) == order.Lt
}

pub fn advance_by_zero_is_identity_test() {
  let instant = time.from_nanoseconds(100 * ms)

  assert time.advance(instant, by: time.zero()) == instant
}

pub fn negative_spans_round_trip_test() {
  let later = time.from_nanoseconds(250 * ms)
  let earlier = time.from_nanoseconds(100 * ms)

  let backwards = time.since(from: later, to: earlier)

  assert time.advance(later, by: backwards) == earlier
  assert duration.to_milliseconds(time.halve(backwards)) == -75
}

pub fn now_never_decreases_test() {
  let clock = time.clock()

  let first = time.now(clock)
  let second = time.now(clock)

  assert time.compare(first, second) != order.Gt
}

// --- Duration helpers ---

pub fn zero_spans_no_time_test() {
  assert duration.to_milliseconds(time.zero()) == 0
}

pub fn double_test() {
  assert duration.to_milliseconds(time.double(duration.milliseconds(1500)))
    == 3000
}

pub fn halve_test() {
  assert duration.to_milliseconds(time.halve(duration.milliseconds(1000)))
    == 500
}

pub fn halve_truncates_test() {
  assert duration.to_milliseconds(time.halve(duration.milliseconds(1001)))
    == 500

  assert duration.to_seconds_and_nanoseconds(
      time.halve(duration.nanoseconds(3)),
    )
    == #(0, 1)
}

pub fn min_and_max_test() {
  let short = duration.milliseconds(1000)
  let long = duration.milliseconds(30_000)

  assert time.min(short, long) == short
  assert time.min(long, short) == short
  assert time.max(short, long) == long
  assert time.max(long, short) == long
}

pub fn min_and_max_of_equal_durations_test() {
  let span = duration.milliseconds(1000)

  assert time.min(span, span) == span
  assert time.max(span, span) == span
}

pub fn min_and_max_order_by_magnitude_test() {
  let backwards =
    time.since(
      from: time.from_nanoseconds(30_000 * ms),
      to: time.from_nanoseconds(0),
    )
  let forwards = duration.milliseconds(1000)

  assert duration.to_milliseconds(backwards) == -30_000

  assert time.min(backwards, forwards) == forwards
  assert time.max(backwards, forwards) == backwards
}

// --- gleam_time behaviour this package depends on ---

pub fn to_milliseconds_truncates_test() {
  assert duration.to_milliseconds(duration.nanoseconds(1_500_000)) == 1
  assert duration.to_milliseconds(duration.nanoseconds(999_999)) == 0
  assert duration.to_milliseconds(duration.milliseconds(1000)) == 1000
}
