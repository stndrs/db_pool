-module(db_pool_ffi).

-export([
    raw_send/2
]).

%%% Raw messaging %%%

%% Send a raw Erlang message to a process, bypassing Subject/selector.
raw_send(Pid, Message) ->
    erlang:send(Pid, Message),
    nil.
