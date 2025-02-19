-module(replicated_server).
-export([start/3, stop/1]).
-export([call/3, cast/2]).

%=======================================================================================================================================
% API
%=======================================================================================================================================

start(Mod, Name, Replicas) when is_list(Replicas) ->
    case global:whereis_name(Name) of
        undefined -> {ok, spawn(fun() -> init(Mod, Name, Replicas) end)};
        _Pid      -> {error, module_already_running}
    end.

stop(Name) ->
    case global:whereis_name(Name) of
        undefined ->
            {error, name_not_registered};
        Pid ->
            Pid ! stop
    end.

call(Name, Req, Consistency)
when Consistency == all; Consistency == quorum; Consistency == one -> 
    case global:whereis_name(Name) of
        undefined -> {error, name_not_registered};
        Pid ->
            Pid ! {call, self(), Req, Consistency},
            receive
                {Name, Res} ->
                    Res
            after
                5000 -> timeout
			end
    end;

call(_Name, _Req, _Consistency) ->
    {error, invalid_consistency_level}.


cast(Name, Req) ->
    case global:whereis_name(Name) of
        undefined -> {error, name_not_registered};
        Pid ->
            Pid ! {cast, Req},
            ok
    end.

%=======================================================================================================================================
% FUNCIONES INTERNAS
%=======================================================================================================================================

init(Mod, Name, Replicas) ->
    global:register_name(Name, self()),
    State = Mod:init(),
    PendingReplications = {#{}, 0},
    loop(Mod, Name, State, Replicas, PendingReplications).

loop(Mod, Name, State, Replicas, {PendingReplications, NextId}) ->
    receive
        {call, From, Req, Consistency} ->
            % Resuelve el requerimiento
            {LocalRes, State2} = Mod:handle_call(Req, State),
            % Guarda el timestamp de respuesta
            PendingNew = case Consistency of
                one ->
					% Selecciona la respuesta (en este caso es solo una, asi que siempre será la misma)
                    Reply = Mod:select_reply([LocalRes]),
					% Llama a las replicas para que repliquen la operacion
                    replicate_call(Req, Replicas, NextId, one),
					% Envia la respuesta al cliente
					From ! {Name, Reply},
					PendingReplications;
                quorum ->
					% Guarda en su mapa de replicaciones pendientes un nuevo proceso de replicacion
					% Cuando comience a recibir mensajes de confirmación de replicacion, los reconocera como
					% pertenecientes a este proceso por una Id.
					% Para "quorum" se necesita la confirmación de al menos la mitad de las replicas
                    replicate_call(Req, Replicas, NextId, quorum),
					put_replication(NextId, {[LocalRes], 0, length(Replicas) div 2, From}, PendingReplications);
                all ->
					% Casi igual que "quorum", pero para "all" se necesita la confirmación todas las replicas
                    replicate_call(Req, Replicas, NextId, all),
					put_replication(NextId, {[LocalRes], 0, length(Replicas), From}, PendingReplications)
            end,
			% Vuelve a esperar, con el estado actualizado y con una nueva Id para la proxima replicacion
			loop(Mod, Name, State2, Replicas, {PendingNew, NextId + 1});

        {replicate, From, Req, ReplicationId} ->
            % Replica el requerimiento indicado
            {Res, State2} = Mod:handle_call(Req, State),
            % Avisa a la replica que la operacion fue realizada
            From ! {replicated, Name, ReplicationId, Res},
            % Recibe el proximo requerimiento
            loop(Mod, Name, State2, Replicas, {PendingReplications, NextId});
        
        {replicate_async, Req} ->
            % Resuelve el requerimiento
            {_, State2} = Mod:handle_call(Req, State),
            % Recibe el proximo requerimiento, actualizando el estado
            loop(Mod, Name, State2, Replicas, {PendingReplications, NextId});

        {replicated, From, ReplicationId, Res} ->
            case handle_replicated(PendingReplications, ReplicationId, Res, From, Replicas) of
                {done, ReplicaRes, ReturnTo, Pending2} ->
                    Reply = Mod:select_reply(ReplicaRes),
                    ReturnTo ! {Name, Reply},
                    loop(Mod, Name, State, Replicas, {Pending2, NextId});
                {wait, Pending2} ->
                    loop(Mod, Name, State, Replicas, {Pending2, NextId});
                {error, _Error} ->
                    loop(Mod, Name, State, Replicas, {PendingReplications, NextId})
            end;
            
        stop ->
            % Termina su ejecución
            ok
    end.

handle_replicated(PendingReplications, ReplicationId, Res, From, Replicas) ->
    case find_replication(ReplicationId, PendingReplications) of
        {ok, Entry} ->
            IsKnownReplica = lists:member(From, Replicas),
            if IsKnownReplica ->
                handle_valid_replicated(Entry, PendingReplications, ReplicationId, Res);
			true ->
                {error, unknown_replica}
            end;
        error ->
            {error, expired_confirmation}
    end.

handle_valid_replicated({Answers, Answered, MinAnswers, ReturnTo}, PendingReplications, ReplicationId, Res)
when Answered + 1 == MinAnswers ->
    Pending2 = remove_replication(ReplicationId, PendingReplications),
    {done, [Res | Answers], ReturnTo, Pending2};

handle_valid_replicated({Answers, Answered, MinAnswers, ReturnTo}, PendingReplications, ReplicationId, Res)
when Answered + 1 < MinAnswers ->
    Pending2 = put_replication(ReplicationId, {[Res | Answers], Answered + 1, MinAnswers, ReturnTo}, PendingReplications), 
    {wait, Pending2}.

replicate_call(Req, Replicas, _ReplicationId, one) when is_list(Replicas) ->
    lists:foreach(
        fun(R) ->
            case global:whereis_name(R) of
                undefined -> notfound;
                Pid -> Pid ! {replicate_async, Req}
            end
        end,
        Replicas
    );

replicate_call(Req, Replicas, ReplicationId, _Consistency) when is_list(Replicas) ->
    lists:foreach(
        fun(R) ->
            case global:whereis_name(R) of
                undefined -> notfound;
                Pid -> Pid ! {replicate, self(), Req, ReplicationId}
            end
        end,
        Replicas
    ).

put_replication(Id, {Answers, Answered, MinAnswers, From}, ReplicationMap) ->
    ReplicationMap#{Id => {Answers, Answered, MinAnswers, From}}.

remove_replication(Id, ReplicationMap) ->
    maps:remove(Id, ReplicationMap).

find_replication(Id, ReplicationMap) ->
    maps:find(Id, ReplicationMap).