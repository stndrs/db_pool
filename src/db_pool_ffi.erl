-module(db_pool_ffi).

-export([
  ets_queue/1,
  ets_first_lookup/1,
  ets_info/2,
  ets_lookup/2,
  ets_insert/3,
  ets_delete/2,
  unique_int/0
]).

ets_queue(Name) ->
  ets:new(Name, [named_table, private, ordered_set]).

ets_first_lookup(Name) ->
  try
    case ets:first_lookup(Name) of
      '$end_of_table' -> {error, nil};
      {Key, [{_Key, Value}]} -> {ok, {Key, Value}}
    end
  catch error:badarg -> {error, nil}
  end.

ets_lookup(Name, Key) ->
  try
    case ets:lookup(Name, Key) of
      '$end_of_table' -> {error, nil};
      [{_Key, Value}] -> {ok, Value};
      [] -> {error, nil}
    end
  catch error:badarg -> {error, nil}
  end.

ets_info(Name, Item) ->
  try
    case ets:info(Name, Item) of
      undefined -> {error, nil};
      Value -> {ok, Value}
    end
  catch error:badarg -> {error, nil}
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
    error:badarg -> {error, nil}
  end.

unique_int() -> erlang:unique_integer([positive]).
