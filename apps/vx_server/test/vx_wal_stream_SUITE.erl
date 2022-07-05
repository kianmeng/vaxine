-module(vx_wal_stream_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(APP, vx_server).
-define(comment(A), [{userdata,
                      [{description, A}]
                     }]).

all() ->
    [ %%single_node_test,
      {group, setup_node}
    ].

groups() ->
    [{setup_node, [],
      [ sanity_check,
        single_txn,
        single_txn_from_history,
        single_txn_from_history2
      ]}].

init_per_suite(_, Config) ->
    Config.

end_per_suite(_, Config) ->
    Config.

init_per_group(setup_node, Config) ->
    vx_test_utils:init_testsuite(),
    vx_test_utils:init_clean_node(node(), 0,
                                  [{ riak_core, ring_creation_size, 1 },
                                   { antidote, sync_log, true}
                                  ]),
    Config.
    %%[{nodes, Nodes}, {cookie, erlang:get_cookie()}| Config].

end_per_group(setup_node, Config) ->
    Config.

%%------------------------------------------------------------------------------

sanity_check() ->
    ?comment("Sanity check for replication").
sanity_check(_Config) ->
    {ok, Pid} = vx_wal_stream:start_replication([]),
    ok = vx_wal_stream:stop_replication(Pid),

    {ok, Pid2} = vx_wal_stream:start_replication([]),
    ok = vx_wal_stream:stop_replication(Pid2),
    ok.

single_txn() ->
    ?comment("Single transaction is replicated").

single_txn(_Config) ->
    {Key, Bucket, Type, Value} = {key_a, <<"my_bucket">>, antidote_crdt_counter_pn, 5},
    {ok, SN} = simple_update_value([{Key, Value}]),
    assert_receive(100),

    {ok, Pid} = vx_wal_stream:start_replication([]),

    [Msg] = assert_count(1, 1000),
    ct:log("message: ~p~n", [Msg]),

    K = key_format(Key, Bucket),
    ?assertMatch({_Txn, [{K, Type, Value}]}, Msg),

    ok = vx_wal_stream:stop_replication(Pid).

single_txn_from_history() ->
    ?comment("When database has single transaction we get it when starting replication").
single_txn_from_history(_Config) ->
    {Key, Bucket, Type, Value} = {key_a, <<"my_bucket">>, antidote_crdt_counter_pn, 5},

    {ok, Pid} = vx_wal_stream:start_replication([]),
    [Msg] = assert_count(1, 1000),
    ct:log("message: ~p~n", [Msg]),

    K = key_format(Key, Bucket),
    ?assertMatch({_Txn, [{K, Type, Value}]}, Msg),

    assert_receive(100),

    ok = vx_wal_stream:stop_replication(Pid).

single_txn_from_history2() ->
    ?comment("When database has single transaction we get it when starting replication").
single_txn_from_history2(_Config) ->
    {Key, Bucket, Type, Value} = {key_a, <<"my_bucket">>, antidote_crdt_counter_pn, 5},

    {ok, Pid} = vx_wal_stream:start_replication([]),
    [Msg] = assert_count(1, 1000),
    ct:log("message: ~p~n", [Msg]),

    K = key_format(Key, Bucket),
    ?assertMatch({_Txn, [{K, Type, Value}]}, Msg),

    assert_receive(100),

    {ok, SN1} = simple_update_value([{Key, 6}]),
    [Msg1] = assert_count(1, 1000),
    ct:log("message: ~p~n", [Msg1]),

    ?assertMatch({_Txn, [{K, Type, 11}]}, Msg1),

    assert_receive(100),

    ok = vx_wal_stream:stop_replication(Pid).


%------------------------------------------------------------------------------

key_format(Key, Bucket) ->
    {erlang:atom_to_binary(Key, latin1), Bucket}.

simple_update_value(KVList) ->
    {ok, Pid} = antidotec_pb_socket:start_link("127.0.0.1",
                                               vx_test_utils:port(antidote, pb_port, 0)),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{static, false}]),

    UpdateOps =
        lists:map(
          fun({Key, Value}) ->
                  Identity = {erlang:atom_to_binary(Key, latin1),
                              antidote_crdt_counter_pn, <<"my_bucket">>},
                  Operation = antidotec_counter:increment(
                                Value, antidotec_counter:new()),
                  antidotec_counter:to_ops(Identity, Operation)
          end, KVList),

    antidotec_pb:update_objects(Pid, lists:flatten(UpdateOps), TxId),
    {ok, SnapshotTime}= antidotec_pb:commit_transaction(Pid, TxId),
    ct:log("committed txn ~p:~n ~p~n", [binary_to_term(SnapshotTime), UpdateOps]),
    {ok, binary_to_term(SnapshotTime)}.

assert_receive(Timeout) ->
    receive
        M ->
            erlang:error({unexpected_msg, M})
    after Timeout ->
            ok
    end.

assert_count(0, _Timeout) ->
    [];
assert_count(N, Timeout) ->
    receive
        M ->
            [M | assert_count(N-1, Timeout)]
    after Timeout ->
              erlang:error({not_sufficient_msg_count, N})
    end.
