-module(db_test).
-include_lib("eunit/include/eunit.hrl").

standard_test_() ->
    {
        "Test simples de funcionamiento de db_replica:put, db_replica:remove y db_replica:get",
        {
            setup,
            fun() -> db:start(db, 5), timer:sleep(1000) end,
            fun(_) -> db:stop(db) end,
            fun(_) ->
                [
                    ?_assertEqual(ok, db_replica:put("dns.google.com", "8.8.8.8", all, 'db-4')), 
                    ?_assertEqual(ok, db_replica:put("yahoo.com", "3.69.58.11", all, 'db-2')),
                    ?_assertEqual(ok, db_replica:put("facebook.com", "5.55.93.122", quorum, 'db-1')),
                    ?_assertEqual(ok, db_replica:put("last.fm", "43.54.198.88", one, 'db-3')),
                    ?_assertEqual(ok, db_replica:put("rateyourmusic.com", "129.9.1.22", quorum, 'db-5')),
                    ?_assertMatch({ok, "8.8.8.8", _}, db_replica:get("dns.google.com", quorum, 'db-5')),
                    ?_assertMatch({ok, "3.69.58.11", _}, db_replica:get("yahoo.com", all, 'db-2')),
                    ?_assertMatch({ok, "5.55.93.122", _}, db_replica:get("facebook.com", quorum, 'db-3')),
                    ?_assertMatch({ok, "43.54.198.88", _}, db_replica:get("last.fm", all, 'db-4')),
                    ?_assertMatch({ok, "129.9.1.22", _}, db_replica:get("rateyourmusic.com", quorum, 'db-1')),
                    ?_assertEqual(ok, db_replica:remove("dns.google.com", all, 'db-1')),
                    ?_assertEqual(ok, db_replica:remove("rateyourmusic.com", one, 'db-2')),
                    ?_assertEqual(ok, db_replica:remove("facebook.com", one, 'db-4')),
                    ?_assertEqual(ok, db_replica:remove("last.fm", quorum, 'db-5')),
                    ?_assertEqual(ok, db_replica:remove("yahoo.com", all, 'db-3')),
                    ?_assertMatch({ko, _}, db_replica:get("dns.google.com", quorum, 'db-5')),
                    ?_assertMatch({ko, _}, db_replica:get("yahoo.com", all, 'db-2')),
                    ?_assertMatch({ko, _}, db_replica:get("facebook.com", quorum, 'db-3')),
                    ?_assertMatch({ko, _}, db_replica:get("last.fm", all, 'db-4')),
                    ?_assertMatch({ko, _}, db_replica:get("rateyourmusic.com", quorum, 'db-1'))
                ]
            end
        }
    }.

expired_request_test_() ->
    {
        "Test para verificar el comportamiento si llegan requerimientos desactualizados",
        {
            setup,
            fun() ->
                db:start(db, 5),
                OldTimestamp = erlang:timestamp(),
                timer:sleep(1000),
                OldTimestamp
            end,
            fun(_) -> db:stop(db) end,
            fun(OldTimestamp) ->
                [
                    ?_assertEqual(ok, db_replica:put("dns.google.com", "8.8.8.8", all, 'db-4')),
                    ?_assertEqual(ko, replicated_server:call('db-2', {put, {"dns.google.com", "8.8.4.4", OldTimestamp}}, all)),
                    ?_assertEqual(ok, db_replica:remove("dns.google.com", all, 'db-4')),
                    ?_assertEqual(ko, replicated_server:call('db-2', {remove, {"dns.google.com", OldTimestamp}}, all))
                ]
            end
        }
    }.

seleccionar_mas_reciente_test_() ->
	{
		"Testea si el resultado devuelto es el mas actualizado entre todas las replicas",
		{
			setup,
			fun() ->
				db:start(db, 5),
				timer:sleep(1000),
				db_replica:put("dns.google.com", "8.8.8.8", all, 'db-1'),
				db_replica:stop('db-2'),
				db_replica:start('db-2', ['db-1','db-3','db-4','db-5']),
				timer:sleep(1000)
            end,
			fun(_) -> db:stop(db) end,
			fun(_) ->
				[
					?_assertEqual(notfound, db_replica:get("dns.google.com", one, 'db-2')),
					?_assertMatch({ok, "8.8.8.8", _}, db_replica:get("dns.google.com", all, 'db-2'))
				]
			end
		}
	}.



timeout_test_() ->
% Este test demora en terminar, ya que testea (dos veces) el timeout cuando no se obtiene respuesta que es a los 5 segundos
    {
		"Testea la diferencia de comportamiento entre los niveles de consistencia cuando se caen replicas",
		{
			setup,
			fun() -> db:start(db, 3), timer:sleep(1000) end,
			fun(_) -> db:stop(db) end,
			fun(_) ->
				[
					?_assertEqual(ok, db_replica:put("dns.google.com", "8.8.8.8", all, 'db-1')),
                    {
                        setup,
                        fun() -> db_replica:stop('db-3') end,
                        fun(_) -> ok end,
                        fun(_) -> ?_assertMatch({ok, "8.8.8.8", _}, db_replica:get("dns.google.com", one, 'db-1')) end
                    },
					?_assertMatch({ok, "8.8.8.8", _}, db_replica:get("dns.google.com", quorum, 'db-1')),
					{
                        timeout,
                        10, % segundos
                        ?_assertEqual(timeout, db_replica:get("dns.google.com", all, 'db-1'))
                    },
                    {
                        setup,
                        fun() -> db_replica:stop('db-2') end,
                        fun(_) -> ok end,
                        fun(_) ->
                            {
                                timeout,
                                10, %segundos
                                ?_assertEqual(timeout, db_replica:get("dns.google.com", quorum, 'db-1'))
                            }
                        end
                    },
					?_assertMatch({ok, "8.8.8.8", _}, db_replica:get("dns.google.com", one, 'db-1'))
				]
            end
		}
	}.

start_stop_test_() ->
    {
        "Testea la correcta inicializacion y terminación de las replicas",
        {
            setup,
            fun() -> db:start(db, 3), timer:sleep(1000) end,
            fun(_) -> db:stop(db) end,
            fun(_) ->
                [
                    ?_assertEqual({error, module_already_running}, db:start(db, 3)),
                    ?_assertEqual({error, module_already_running}, db_replica:start('db-1', [])),
                    ?_assertEqual({error, module_already_running}, db_replica:start('db-2', [])),
                    ?_assertEqual({error, module_already_running}, db_replica:start('db-3', [])),
                    ?_assertEqual(stop, db:stop(db)),
                    ?_assertEqual({error, name_not_registered}, db:stop(db)),
                    ?_assertEqual({error, name_not_registered}, db_replica:stop('db-1')),
                    ?_assertEqual({error, name_not_registered}, db_replica:stop('db-2')),
                    ?_assertEqual({error, name_not_registered}, db_replica:stop('db-3'))
                ]
            end
        }
    }.