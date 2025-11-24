-module(db_pool_ffi).

-export([
  ets_queue/1,
  ets_first_lookup/1,
  ets_new/1,
  ets_insert/3,
  ets_queue_insert/3,
  ets_queue_lookup/2,
  ets_lookup/2,
  unique_int/0
]).

ets_queue(Name) ->
  ets:new(Name, [protected, ordered_set, {decentralized_counters, true}]).

ets_first_lookup(Table) ->
  case ets:first_lookup(Table) of
    '$end_of_table' -> none;
    {Key, [{_Key, Value}]} -> {some, {Key, Value}}
  end.

ets_new(Name) ->
  ets:new(Name, [named_table, {read_concurrency, true}]).

ets_insert(Name, Key, Value) ->
  try
    ets:insert(Name, {Key, Value}),

    {ok, {Key, Value}}
  catch
    error:badarg ->
      {error, nil}
  end.

ets_queue_insert(Name, Key, Value) ->
  try
    ets:insert(Name, {Key, Value}),

    {ok, nil}
  catch
    error:badarg ->
      {error, nil}
  end.

ets_queue_lookup(Table, Key) ->
  try ets:lookup(Table, Key) of
    [{_, Value}] -> {ok, Value};
    _ -> {error, nil}
  catch error:badarg -> {error, nil}
  end.

ets_lookup(Name, Key) ->
  try
    Objects = ets:lookup(Name, Key),

    {ok, Objects}
  catch
    error:badarg ->
      {error, nil}
  end.

unique_int() -> erlang:unique_integer([positive]).
