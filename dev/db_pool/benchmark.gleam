import db_pool
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/string
import rasa/counter
import rasa/monotonic

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type Scenario {
  Scenario(
    name: String,
    pool_size: Int,
    workers: Int,
    hold_ms: Int,
    checkout_timeout: Int,
    deadline: Int,
    duration_ms: Int,
  )
}

type Sample {
  Sample(latency_us: Int, ok: Bool)
}

type Stats {
  Stats(
    total_ops: Int,
    successes: Int,
    errors: Int,
    throughput: Float,
    p50: Int,
    p95: Int,
    p99: Int,
    max: Int,
  )
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn main() -> Nil {
  io.println("db_pool benchmark")
  io.println(string.repeat("=", 60))
  io.println("")

  let scenarios = [
    Scenario(
      name: "Uncontended",
      pool_size: 10,
      workers: 10,
      hold_ms: 0,
      checkout_timeout: 200,
      deadline: 30_000,
      duration_ms: 5000,
    ),
    Scenario(
      name: "Contended",
      pool_size: 10,
      workers: 50,
      hold_ms: 2,
      checkout_timeout: 200,
      deadline: 30_000,
      duration_ms: 5000,
    ),
    Scenario(
      name: "Overloaded",
      pool_size: 5,
      workers: 100,
      hold_ms: 5,
      checkout_timeout: 100,
      deadline: 30_000,
      duration_ms: 5000,
    ),
    Scenario(
      name: "Deadline pressure",
      pool_size: 5,
      workers: 20,
      hold_ms: 50,
      checkout_timeout: 500,
      deadline: 10,
      duration_ms: 5000,
    ),
  ]

  list.each(scenarios, run_scenario)
}

// ---------------------------------------------------------------------------
// Scenario runner
// ---------------------------------------------------------------------------

fn run_scenario(scenario: Scenario) -> Nil {
  let params =
    "pool="
    <> int.to_string(scenario.pool_size)
    <> ", workers="
    <> int.to_string(scenario.workers)
    <> ", hold="
    <> int.to_string(scenario.hold_ms)
    <> "ms"

  io.println("Scenario: " <> scenario.name <> " (" <> params <> ")")

  let name = process.new_name("bench_pool")

  let pool =
    db_pool.new()
    |> db_pool.size(scenario.pool_size)
    |> db_pool.on_open(fn() { Ok(Nil) })
    |> db_pool.on_close(fn(_) { Ok(Nil) })
    |> db_pool.on_interval(fn(_) { Nil })

  let assert Ok(pool) = db_pool.start(pool, name, 5000)

  // Subject to collect samples from workers
  let collector = process.new_subject()

  // Microsecond counter for latency measurement
  let timer = counter.monotonic_time(monotonic.Microsecond)

  // Compute the absolute stop time so workers can check autonomously
  let now = counter.next(timer)
  let stop_time = now + scenario.duration_ms * 1000

  // Spawn workers
  int.range(from: 0, to: scenario.workers, with: "", run: fn(_, _) {
    spawn_worker(pool, collector, stop_time, timer, scenario)

    ""
  })

  // Wait for the benchmark duration plus a drain buffer
  process.sleep(scenario.duration_ms + 500)

  // Collect all samples
  let samples = collect_samples(collector, [])

  // Compute and print stats
  let stats = compute_stats(samples, scenario.duration_ms)
  print_stats(stats)

  // Pool cleanup is not needed for benchmarks — the actor will be
  // garbage collected when the process exits. With ~3M ops and 2 messages
  // each, the actor mailbox has millions of pending messages that would
  // take too long to drain.

  io.println("")
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

fn spawn_worker(
  pool: process.Subject(db_pool.Message(Nil, err)),
  collector: process.Subject(Sample),
  stop_time: Int,
  timer: counter.Counter,
  scenario: Scenario,
) -> process.Pid {
  process.spawn_unlinked(fn() {
    worker_loop(pool, collector, stop_time, timer, scenario)
  })
}

fn worker_loop(
  pool: process.Subject(db_pool.Message(Nil, err)),
  collector: process.Subject(Sample),
  stop_time: Int,
  timer: counter.Counter,
  scenario: Scenario,
) -> Nil {
  // Check if we've exceeded the stop time
  let now = counter.next(timer)
  case now >= stop_time {
    True -> Nil
    False -> {
      let self = process.self()

      let result =
        db_pool.checkout(
          pool,
          self,
          scenario.checkout_timeout,
          scenario.deadline,
        )

      case result {
        Ok(conn) -> {
          // Measure only checkout wait time, not hold time or checkin
          let checkout_end = counter.next(timer)

          // Simulate work
          case scenario.hold_ms > 0 {
            True -> process.sleep(scenario.hold_ms)
            False -> Nil
          }
          db_pool.checkin(pool, conn, self)

          process.send(
            collector,
            Sample(latency_us: checkout_end - now, ok: True),
          )
        }
        Error(_) -> {
          let t1 = counter.next(timer)
          process.send(collector, Sample(latency_us: t1 - now, ok: False))
        }
      }

      worker_loop(pool, collector, stop_time, timer, scenario)
    }
  }
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

fn collect_samples(
  collector: process.Subject(Sample),
  acc: List(Sample),
) -> List(Sample) {
  case process.receive(collector, 0) {
    Ok(sample) -> collect_samples(collector, [sample, ..acc])
    Error(Nil) -> acc
  }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

fn compute_stats(samples: List(Sample), duration_ms: Int) -> Stats {
  let total_ops = list.length(samples)
  let successes =
    list.filter(samples, fn(s) { s.ok })
    |> list.length

  let errors = total_ops - successes

  let duration_s = int.to_float(duration_ms) /. 1000.0
  let throughput = int.to_float(total_ops) /. duration_s

  // Sort success latencies for percentile computation
  let latencies =
    list.filter(samples, fn(s) { s.ok })
    |> list.map(fn(s) { s.latency_us })
    |> list.sort(int.compare)

  let #(p50, p95, p99, max) = case latencies {
    [] -> #(0, 0, 0, 0)
    _ -> {
      let len = list.length(latencies)
      let p50 = percentile(latencies, len, 50)
      let p95 = percentile(latencies, len, 95)
      let p99 = percentile(latencies, len, 99)
      let max = percentile(latencies, len, 100)
      #(p50, p95, p99, max)
    }
  }

  Stats(total_ops:, successes:, errors:, throughput:, p50:, p95:, p99:, max:)
}

fn percentile(sorted: List(Int), len: Int, pct: Int) -> Int {
  let idx = case pct >= 100 {
    True -> len - 1
    False -> {
      let raw = {
        int.to_float(len) *. int.to_float(pct) /. 100.0
      }
      let i = float.truncate(raw)
      int.min(i, len - 1)
    }
  }
  nth(sorted, idx)
}

fn nth(list: List(Int), idx: Int) -> Int {
  case list, idx {
    [x, ..], 0 -> x
    [_, ..rest], n -> nth(rest, n - 1)
    [], _ -> 0
  }
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

fn print_stats(stats: Stats) -> Nil {
  let error_pct = case stats.total_ops > 0 {
    True -> int.to_float(stats.errors) *. 100.0 /. int.to_float(stats.total_ops)
    False -> 0.0
  }

  io.println(
    "  Throughput: "
    <> format_number(float.truncate(stats.throughput))
    <> " ops/sec",
  )
  io.println(
    "  Total ops:  "
    <> format_number(stats.total_ops)
    <> " ("
    <> format_number(stats.successes)
    <> " ok, "
    <> format_number(stats.errors)
    <> " errors)",
  )
  io.println("  Error rate: " <> format_pct(error_pct))
  io.println(
    "  Latency:    "
    <> "p50="
    <> format_us(stats.p50)
    <> "  p95="
    <> format_us(stats.p95)
    <> "  p99="
    <> format_us(stats.p99)
    <> "  max="
    <> format_us(stats.max),
  )
}

fn format_us(us: Int) -> String {
  case us >= 1_000_000 {
    True -> {
      let ms = us / 1000
      format_number(ms) <> "ms"
    }
    False ->
      case us >= 1000 {
        True -> {
          let ms = us / 1000
          let frac = { us % 1000 } / 100
          int.to_string(ms) <> "." <> int.to_string(frac) <> "ms"
        }
        False -> int.to_string(us) <> "us"
      }
  }
}

fn format_pct(pct: Float) -> String {
  // Format to 1 decimal place
  let whole = float.truncate(pct)
  let frac = float.truncate({ pct -. int.to_float(whole) } *. 10.0)
  let frac_abs = int.absolute_value(frac)
  int.to_string(whole) <> "." <> int.to_string(frac_abs) <> "%"
}

fn format_number(n: Int) -> String {
  case n < 0 {
    True -> "-" <> format_number_pos(int.absolute_value(n))
    False -> format_number_pos(n)
  }
}

fn format_number_pos(n: Int) -> String {
  case n < 1000 {
    True -> int.to_string(n)
    False -> {
      let rest = n / 1000
      let last3 =
        int.to_string(n % 1000)
        |> string.pad_start(to: 3, with: "0")
      format_number_pos(rest) <> "," <> last3
    }
  }
}
