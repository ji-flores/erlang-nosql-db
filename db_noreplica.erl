-module(db_noreplica).
-behaviour(gen_server).

%% Imports
-import(db_map, [create_database/0, create_entry/3, create_removal/2,
    put_request/2, remove_request/2, get_request/2]).

%% API
-export([start/0, stop/0, put/2, remove/1, get/1]).
-export([init/1, handle_call/3, handle_cast/2]).

%==============================
%   API
%==============================

start() ->
    case global:whereis_name(db_noreplica) of
        undefined   -> gen_server:start_link({global,db_noreplica}, db_noreplica, [], []);
        _           -> {error, name_already_registered}
    end.

stop() ->
    gen_server:stop({global, db_noreplica}).

put(Key, Value) ->
    gen_server:call({global, db_noreplica}, {put, Key, Value}).

remove(Key) ->
    gen_server:call({global, db_noreplica}, {remove, Key}).

get(Key) ->
    gen_server:call({global, db_noreplica}, {get, Key}).

%==============================
%   INNER FUNCTIONS
%==============================

init(_Args) ->
    {ok, create_database()}.

handle_call({put, Key, Value}, _From, Map1) ->
        Entry = create_entry(Key, Value, erlang:timestamp()),
        {Reply, _Timestamp ,Map2} = put_request(Entry, Map1),
        {reply, Reply, Map2};

handle_call({remove, Key}, _From, Map1) ->
    RemovalRequest = create_removal(Key, erlang:timestamp()),
    {Reply, _Timestamp , Map2} = remove_request(RemovalRequest, Map1),
    {reply, Reply, Map2};

handle_call({get, Key}, _From, Map) ->
    {Reply, _Timestamp} = get_request(Key, Map),
    {reply, Reply, Map};

handle_call(Message, _From, State) ->
    {reply, {error, {unexpected_message, Message}}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.