-module(db_map).
-export([create_database/0, create_entry/3, create_removal/2,
    put_request/2, remove_request/2, get_request/2]).

create_database() ->
    #{}.

create_entry(Key, Value, Timestamp) ->
    {Key, {Value, Timestamp, false}}.

create_removal(Key, Timestamp) ->
    {Key, Timestamp}.

put_request({Key, {Value, Timestamp, _IsDeleted}}, Map1) ->
    case maps:find(Key, Map1) of
        {ok, {_, PresentTimestamp, _}} when PresentTimestamp > Timestamp ->
            {ko, PresentTimestamp, Map1};
        _ ->
            % error (todavia no existe una entrada con esa key), o una entrada, presente o eliminada
            % De cualquiera manera sera sobreescrita por su version mas actual.
            Map2 = Map1#{Key => {Value, Timestamp, false}},
            {ok, Timestamp, Map2}
    end.

remove_request({Key, Timestamp}, Map1) ->
    case maps:find(Key, Map1) of
        {ok, {_Key, PresentTimestamp, _IsDeleted}} when PresentTimestamp > Timestamp ->
            {ko, PresentTimestamp, Map1};
        {ok, {_Key, _Timestamp, false}} ->
            Map2 = Map1#{Key => {ok, Timestamp, true}}, 
            {ok, Timestamp, Map2};
        _ ->
            % error (todavia no existe una entrada con esa key)
            %   -> debemos registrar que se elimino por si llegan inserciones pasadas.
            % una entrada presente
            %   -> debe ser eliminada, y marcar la key como eliminada con el tiempo presente.
            Map2 = Map1#{Key => {null, Timestamp, true}}, 
            {notfound, {0,0,0}, Map2}
    end.

get_request(Key, Map) ->
    case maps:find(Key, Map) of
        {ok, {Value, Timestamp, false}} -> 
            {{ok, Value, format_timestamp(Timestamp)}, Timestamp};
        {ok, {_, Timestamp, true}} ->
            {{ko, format_timestamp(Timestamp)}, Timestamp};
        error ->
            {notfound, {0,0,0}} % Retorna el minimo timestamp
    end.

format_timestamp(Timestamp) ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:now_to_local_time(Timestamp),
    IOList = io_lib:format("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B",
                  [Year, Month, Day, Hour, Minute, Second]),
    lists:flatten(IOList).