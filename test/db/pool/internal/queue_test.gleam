import db/pool/internal/queue
import gleam/option.{None, Some}

pub fn queue_test() {
  let data = queue.new("test_queue")

  let assert Ok(Nil) = queue.insert(data, "first", 10)
  let assert Ok(Nil) = queue.insert(data, "second", 20)

  let assert Some(#(key, value)) = queue.first_lookup(data)

  assert key == "first"
  assert value == 10

  let assert Ok(Nil) = queue.delete_key(data, "first")

  let assert Some(#(key, value)) = queue.first_lookup(data)

  assert key == "second"
  assert value == 20
}

pub fn first_lookup_none_test() {
  let data = queue.new("test_queue")

  let assert None = queue.first_lookup(data)
}
