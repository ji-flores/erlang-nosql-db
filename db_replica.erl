-module(db_replica).
-export([start/2, stop/1, put/4, remove/3, get/3]).
-export([init/0, handle_call/2, select_reply/1]).
-include_lib("eunit/include/eunit.hrl").

%==========================================================================================
% API
%==========================================================================================

start(Name, Replicas) ->
    replicated_server:start(db_replica, Name, Replicas).

stop(Name) ->
    replicated_server:stop(Name).

put(Key, Value, Consistency, Name) ->
    replicated_server:call(Name, {put, {Key, Value, erlang:timestamp()}}, Consistency).

remove(Key, Consistency, Name) ->
    replicated_server:call(Name, {remove, {Key, erlang:timestamp()}}, Consistency).

get(Key, Consistency, Name) ->
    replicated_server:call(Name, {get, Key}, Consistency).

%==========================================================================================
% Callbacks
%==========================================================================================

init() ->
    db_map:create_database().

handle_call({put, {Key, Value, Timestamp}}, Map1) ->
    Entry = db_map:create_entry(Key, Value, Timestamp),
    {Reply, ReplyTimestamp, Map2} = db_map:put_request(Entry, Map1),
    {{Reply, ReplyTimestamp}, Map2};

handle_call({remove, {Key, Timestamp}}, Map1) ->
    RemovalRequest = db_map:create_removal(Key, Timestamp),
    {Reply, ReplyTimestamp, Map2} = db_map:remove_request(RemovalRequest, Map1),
    {{Reply, ReplyTimestamp}, Map2};

handle_call({get, Key}, Map) ->
    {Reply, ReplyTimestamp} = db_map:get_request(Key, Map),
    {{Reply, ReplyTimestamp}, Map};

handle_call(Message, State) ->
    {{error, {unexpected_message, Message}}, State}.

% Selecciona el resultado con el mayor timestamp
select_reply([R | Rs]) ->
    Fun =
		fun({Result, Timestamp}, {MaxResult, MaxTimestamp}) ->
			if
				Timestamp > MaxTimestamp -> {Result, Timestamp};
				true -> {MaxResult, MaxTimestamp}
			end
		end,
	{MaxResult, _MaxTime} = lists:foldl(Fun, R, Rs),
    MaxResult.