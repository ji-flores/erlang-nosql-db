# Erlang NoSQL database

In-memory NoSQL database made with Erlang 27.2.3. Started as an assignment for a concurrent programming class.

It allows you to spawn `N` instances of your database and perform `put`, `remove` and `get` operations with varying levels of consistency (`one`, `quorum`, `all`).

## Usage

```erl
% Start replicated database with db:start(Name, N), where N is the number of replicas.
% Replicas are automatically named using the format Name-1..N.
db:start(db, 3).

% Run operations.

% db_replica:put(Key, Value, ConsistencyLevel, ReplicaName).
db_replica:put("dns.google.com", "8.8.8.8", all, 'db-2').
% db_replica:put(Key, ConsistencyLevel, ReplicaName).
db_replica:get("dns.google.com", quorum, 'db-1').
% db_replica:put(Key, ConsistencyLevel, ReplicaName).
db_replica:remove("dns.google.com", one, 'db-3').

% Stop all replicas of the "Name" database with db:stop(Name). 
db:stop(db).
```

## Testing
```erl
% For verbose output (recommended)
eunit:test(db_test, [verbose]).

% For standard output
db_test:test().
```
