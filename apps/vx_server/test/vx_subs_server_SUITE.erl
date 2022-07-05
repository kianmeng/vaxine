-module(vx_subs_server_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(APP, vx_server).
-define(comment(A), [{userdata,
                      [{description, A}]
                     }]).

-define(rpc(Node, Block),
        rpc:call(Node, erlang, apply, [fun() -> Block end, []])).
-define(rpc_async(Node, Block),
        rpc:async_call(Node, erlang, apply, [fun() -> Block end, []])).
-define(rpc_await(Key), rpc:yield(Key)).

all() ->
    [ %%single_node_test,
      {group, setup_cluster}
    ].

groups() ->
    [{setup_cluster, [], [sanity_check]}].

init_per_suite(_, Config) ->
    Config.

end_per_suite(_, Config) ->
    Config.

init_per_group(setup_cluster, Config) ->
    init_testsuite(),
    {ok, _} = application:ensure_all_started(vx_client),
    Nodes = start_dc(3, Config),
    [{nodes, Nodes}, {cookie, erlang:get_cookie()}| Config].

end_per_group(setup_cluster, Config) ->
    NodesInfo = ?config(nodes, Config),
    [ct_slave:stop(Node) || {_, Node} <- NodesInfo].


init_node_meck() ->
    application:ensure_all_started(meck).

    %%meck:new(simple_crdt_store_memtable, [no_link, passthrough]).

reset_node() ->
    meck:unload().

%%------------------------------------------------------------------------------

sanity_check() ->
    ?comment("Sanity check for the cluster").
sanity_check(Config) ->
    Nodes = ?config(nodes, Config),

    {ok, _} = application:ensure_all_started(vx_client),

    lists:map(
      fun({N, _Node}) ->
              {ok, Pid} = vx_client:connect("localhost", port(?APP, pb_port, N), []),
              ok = vx_client:stop(Pid)
      end, Nodes),
    ok.

port(A, B, C) ->
    vx_test_utils:port(A, B, C).

%%------------------------------------------------------------------------------

start_dc(NodesNum, Config) ->
    NodeNameFun = fun(N) ->
                          erlang:list_to_atom(
                            lists:flatten(
                              io_lib:format("node-~p",[N])))
                  end,
    Nodes = [{N, NodeNameFun(N)} || N <- lists:seq(1, NodesNum)],

    lists:foreach(fun({connect, _Node}) -> ok end,
                  rpc:pmap({?MODULE, start_node},
                           [Config], Nodes)),
    Nodes.


%% Copied from antidote/test/utils/test_utils
-spec start_node({integer(), atom()}, list()) ->
          {connect, node()} | {ready, node()}.
start_node({PortModifier, Name}, Config) ->
    %% have the slave nodes monitor the runner node, so they can't outlive it
    NodeConfig = [{monitor_master, true}],
    case ct_slave:start(Name, NodeConfig) of
        {ok, Node} ->
            %% code path for compiled dependencies
            CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()) ,
            lists:foreach(fun(P) -> rpc:call(Node, code, add_patha, [P]) end, CodePath),

            {ok, _} = vx_test_utils:init_clean_node(Node, PortModifier),
            ct:pal("Node ~p started~n", [Node]),

            {connect, Node};
        {error, already_started, Node} ->
            ct:log("Node ~p already started, reusing node", [Node]),
            {error, Node};
        {error, Reason, Node} ->
            ct:pal("Error starting node ~w, reason ~w, will retry", [Node, Reason]),
            ct_slave:stop(Name),
            %% FIXME: Probably make sense to reuse functionality from antidote as well
            %% time_utils:wait_until_offline(Node),
            timer:sleep(2000),
            start_node(Name, Config)
    end.


init_testsuite() ->
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, {{already_started, _}, _}} -> ok
    end.
