import gleam/erlang/atom.{type Atom}
import gleam/int
import gleam/option.{type Option}

pub type Queue(a, b)

/// Creates a new ordered set table.
pub fn new(name: String) -> Queue(a, b) {
  let table_name = name <> int.to_string(unique_int())

  atom.create(table_name) |> ets_queue
}

/// Returns the queue's first key and value. Returns `None` if the table
/// is empty.
@external(erlang, "db_pool_ffi", "ets_first_lookup")
pub fn first_lookup(queue: Queue(a, b)) -> Option(#(a, b))

/// Inserts a value into the queue. If the key already exists in the
/// queue, the old value is replaced.
@external(erlang, "db_pool_ffi", "ets_insert")
pub fn insert(queue: Queue(a, b), key: a, value: b) -> Result(Nil, Nil)

/// Deletes the key from the queue.
@external(erlang, "db_pool_ffi", "ets_delete")
pub fn delete_key(queue: Queue(a, b), key: a) -> Result(Nil, Nil)

@external(erlang, "db_pool_ffi", "ets_queue")
fn ets_queue(name: Atom) -> Queue(a, b)

@external(erlang, "db_pool_ffi", "unique_int")
fn unique_int() -> Int
