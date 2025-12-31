type TimeUnit {
  Millisecond
}

@external(erlang, "db_pool_ffi", "unique_int")
pub fn unique_int() -> Int

pub fn now_in_ms() -> Int {
  monotonic_time(Millisecond)
}

@external(erlang, "erlang", "monotonic_time")
fn monotonic_time(unit: TimeUnit) -> Int
