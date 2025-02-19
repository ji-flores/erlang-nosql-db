-module(db).
-include_lib("eunit/include/eunit.hrl").
% API
-export([start/2, stop/1]).

%=============================================
% API
%=============================================

start(Name, NumberOfReplicas) when is_integer(NumberOfReplicas) ->
    case global:whereis_name(Name) of
        undefined -> {ok, spawn(fun() -> init(Name, NumberOfReplicas) end)};
        _Pid      -> {error, module_already_running}
    end.

stop(Name) ->
    case global:whereis_name(Name) of
        undefined   -> {error, name_not_registered};
        Pid         -> Pid ! stop
    end.

%=============================================
% Funciones internas
%=============================================

init(Name, NumberOfReplicas) ->
    global:register_name(Name, self()),
    Replicas = startReplicas(Name, NumberOfReplicas),
    idle(Replicas).

idle(Replicas) ->
    receive
        stop -> stopReplicas(Replicas)
    end.

startReplicas(Name, NumberOfReplicas) ->
    ReplicaNames = [generateReplicaName(Name, N) || N <- lists:seq(1, NumberOfReplicas)],
    lists:foreach(
        fun(Replica) -> db_replica:start(Replica, lists:delete(Replica,ReplicaNames)) end,
        ReplicaNames
    ),
    ReplicaNames.

generateReplicaName(Name, N) ->
    NameString = lists:flatten(io_lib:format("~w-~w",[Name, N])),
    list_to_atom(NameString).

stopReplicas(Replicas) ->
    lists:foreach(fun(Replica) -> db_replica:stop(Replica) end, Replicas).
