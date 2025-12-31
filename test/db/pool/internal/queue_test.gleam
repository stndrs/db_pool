import db/pool/internal/queue

pub fn queue_test() {
  let data = queue.new("test_queue")

  let assert Ok(Nil) = queue.insert(data, "first", 10)
  let assert Ok(Nil) = queue.insert(data, "second", 20)

  let assert Ok(#(key, value)) = queue.first_lookup(data)

  assert key == "first"
  assert value == 10

  let assert Ok(Nil) = queue.delete_key(data, "first")

  let assert Ok(#(key, value)) = queue.first_lookup(data)

  assert key == "second"
  assert value == 20
}

pub fn first_lookup_none_test() {
  let data = queue.new("test_queue")

  let assert Error(Nil) = queue.first_lookup(data)
}

pub fn lookup_test() {
  let data = queue.new("test_queue")

  let assert Ok(Nil) = queue.insert(data, "first", 10)

  let assert Ok(value) = queue.lookup(data, "first")

  assert value == 10
}

pub fn size_test() {
  let data = queue.new("test_queue")

  let assert Ok(0) = queue.size(data)

  let assert Ok(Nil) = queue.insert(data, "first", 10)

  let assert Ok(1) = queue.size(data)
}
