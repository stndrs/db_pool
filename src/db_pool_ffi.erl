-module(db_pool_ffi).

-export([
  ets_queue/1,
  ets_first_lookup/1,
  ets_insert/3,
  ets_delete/2,
  unique_int/0
]).

ets_queue(Name) ->
  ets:new(Name, [protected, ordered_set, {decentralized_counters, true}]).

ets_first_lookup(Table) ->
  case ets:first_lookup(Table) of
    '$end_of_table' -> none;
    {Key, [{_Key, Value}]} -> {some, {Key, Value}}
  end.

ets_delete(Name, Key) ->
  try
    ets:delete(Name, Key),
    {ok, nil}
  catch error:badarg -> {error, nil}
  end.

ets_insert(Name, Key, Value) ->
  try
    ets:insert(Name, {Key, Value}),

    {ok, nil}
  catch
    error:badarg ->
      {error, nil}
  end.

unique_int() -> erlang:unique_integer([positive]).
