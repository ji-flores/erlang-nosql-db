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
                {Name, Result} ->
                    Result
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
            {LocalResult, State2} = Mod:handle_call(Req, State),
            % Actualiza el mapa de replicaciones
            PendingNew = case Consistency of
                one ->
					% Selecciona la respuesta (en este caso es solo una, asi que siempre será la misma)
                    Reply = Mod:select_reply([LocalResult]),
					% Llama a las replicas para que repliquen la operacion
                    replicate_call(Req, Replicas, NextId, one),
					% Envia la respuesta al cliente
					From ! {Name, Reply},
                    % Retorna el mapa de replicaciones sin actualizar
					PendingReplications;
                quorum ->
					% Guarda en su mapa de replicaciones pendientes un nuevo proceso de replicacion
					% Cuando comience a recibir mensajes de confirmación de replicacion, los reconocera como
					% pertenecientes a este proceso por una Id.
					% Para "quorum" se necesita la confirmación de al menos la mitad de las replicas
                    replicate_call(Req, Replicas, NextId, quorum),
                    % Retorna el mapa de replicaciones actualizado
					put_replication(NextId, {[LocalResult], 0, length(Replicas) div 2, From}, PendingReplications);
                all ->
					% Casi igual que "quorum", pero para "all" se necesita la confirmación todas las replicas
                    replicate_call(Req, Replicas, NextId, all),
                    % Retorna el mapa de replicaciones actualizado
					put_replication(NextId, {[LocalResult], 0, length(Replicas), From}, PendingReplications)
            end,
			% Vuelve a esperar, con el estado actualizado y con una nueva Id para la proxima replicacion
			loop(Mod, Name, State2, Replicas, {PendingNew, NextId + 1});

        {replicate, From, Req, ReplicationId} ->
            % Replica el requerimiento indicado
            {Result, State2} = Mod:handle_call(Req, State),
            % Avisa a la replica que la operacion fue realizada
            From ! {replicated, Name, ReplicationId, Result},
            % Recibe el proximo requerimiento
            loop(Mod, Name, State2, Replicas, {PendingReplications, NextId});
        
        {replicate_async, Req} ->
            % Resuelve el requerimiento
            {_, State2} = Mod:handle_call(Req, State),
            % Recibe el proximo requerimiento, actualizando el estado
            loop(Mod, Name, State2, Replicas, {PendingReplications, NextId});

        {replicated, From, ReplicationId, Result} ->
            case handle_replicated(PendingReplications, ReplicationId, Result, From, Replicas) of
                % Si se recibieron todas las respuestas, devuelve al cliente
                {done, ReplicaResult, ReturnTo, Pending2} ->
                    Reply = Mod:select_reply(ReplicaResult),
                    ReturnTo ! {Name, Reply},
                    loop(Mod, Name, State, Replicas, {Pending2, NextId});
                % Si faltan respuestas por recibir, actualiza el mapa de replicaciones y vuelve a loop
                {wait, Pending2} ->
                    loop(Mod, Name, State, Replicas, {Pending2, NextId});
                % Si hay un error, vuelve a loop sin ningun cambio
                {error, _Error} ->
                    loop(Mod, Name, State, Replicas, {PendingReplications, NextId})
            end;
            
        stop ->
            % Termina su ejecución
            ok
    end.

handle_replicated(PendingReplications, ReplicationId, Result, From, Replicas) ->
    case find_replication(ReplicationId, PendingReplications) of
        % Si encuentra un proceso de replicacion con esa Id
        {ok, Entry} ->
            IsKnownReplica = lists:member(From, Replicas),
            % Y el mensaje viene de una replica conocida
            if IsKnownReplica ->
                % Actualiza el mapa de replicaciones
                handle_valid_replicated(Entry, PendingReplications, ReplicationId, Result);
			true ->
                % Si no conoce la replica, error.
                {error, unknown_replica}
            end;
        % Si no lo encuentra, el proceso ya expiro (ya se contesto al cliente)
        error ->
            {error, expired_confirmation}
    end.

% Si la respuesta recibida es la ultima, elimina el proceso de replicacion del mapa y avisa que ya termino
handle_valid_replicated({Results, Answered, MinAnswers, ReturnTo}, PendingReplications, ReplicationId, Result)
when Answered + 1 == MinAnswers ->
    Pending2 = remove_replication(ReplicationId, PendingReplications),
    {done, [Result | Results], ReturnTo, Pending2};

% Si todavia faltan respuestas, actualiza el mapa de replicaciones y avisa que todavia faltan respuestas
handle_valid_replicated({Results, Answered, MinAnswers, ReturnTo}, PendingReplications, ReplicationId, Result)
when Answered + 1 < MinAnswers ->
    Pending2 = put_replication(ReplicationId, {[Result | Results], Answered + 1, MinAnswers, ReturnTo}, PendingReplications), 
    {wait, Pending2}.

% Si el nivel de consistencia es one no hace falta que las replicas contesten
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

% Si el nivel de consistencia es cualquier otro, se espera una confirmacion de las replicas
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

% Interfaz simple para manejar el mapa/diccionario de replicaciones

put_replication(Id, {Results, Answered, MinAnswers, From}, ReplicationMap) ->
    ReplicationMap#{Id => {Results, Answered, MinAnswers, From}}.

remove_replication(Id, ReplicationMap) ->
    maps:remove(Id, ReplicationMap).

find_replication(Id, ReplicationMap) ->
    maps:find(Id, ReplicationMap).