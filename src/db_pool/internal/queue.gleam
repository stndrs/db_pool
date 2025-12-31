import db_pool/internal
import gleam/erlang/atom.{type Atom}
import gleam/int

pub opaque type Queue(a, b) {
  Queue(name: Atom)
}

/// Creates a new ordered set table.
pub fn new(name: String) -> Queue(a, b) {
  let table_name = name <> int.to_string(internal.unique_int())

  atom.create(table_name)
  |> ets_queue
  |> Queue
}

/// Returns the queue's first key and value. Returns `None` if the table
/// is empty.
pub fn first_lookup(queue: Queue(a, b)) -> Result(#(a, b), Nil) {
  first_lookup_(queue.name)
}

pub fn lookup(queue: Queue(a, b), key: a) -> Result(b, Nil) {
  lookup_(queue.name, key)
}

/// Inserts a value into the queue. If the key already exists in the
/// queue, the old value is replaced.
pub fn insert(queue: Queue(a, b), key: a, value: b) -> Result(Nil, Nil) {
  insert_(queue.name, key, value)
}

/// Deletes the key from the queue.
pub fn delete_key(queue: Queue(a, b), key: a) -> Result(Nil, Nil) {
  delete_key_(queue.name, key)
}

pub fn size(queue: Queue(a, b)) -> Result(Int, Nil) {
  info(queue.name, Size)
}

type InfoItem {
  Size
}

@external(erlang, "db_pool_ffi", "ets_info")
fn info(name: Atom, item: InfoItem) -> a

@external(erlang, "db_pool_ffi", "ets_first_lookup")
fn first_lookup_(name: Atom) -> Result(#(a, b), Nil)

@external(erlang, "db_pool_ffi", "ets_lookup")
fn lookup_(name: Atom, key: a) -> Result(b, Nil)

@external(erlang, "db_pool_ffi", "ets_insert")
fn insert_(name: Atom, key: a, value: b) -> Result(Nil, Nil)

@external(erlang, "db_pool_ffi", "ets_delete")
fn delete_key_(name: Atom, key: a) -> Result(Nil, Nil)

@external(erlang, "db_pool_ffi", "ets_queue")
fn ets_queue(name: Atom) -> Atom
