%% @doc Nofitication service for logging events. Handler are supposed to be
%% light-weight and should not spend too much time in M:F/2 calls.

-module(logging_notification_server).
-behaviour(gen_event).

-export([ start_link/0,
          add_sup_handler/3,
          notify_commit/4
        ]).
-export([ init/1,
          handle_event/2,
          handle_call/2,
          terminate/2
        ]).

-record( state, { handler :: handler() } ).
-type state() :: #state{}.
-type handler() :: {module(), atom(), term()}.

start_link() ->
    gen_event:start_link({local, ?MODULE}, []).

%% @doc Add subscribers handler. Handler should be as light-weight as possible,
%% as it affects the flow of committed transactions.
-spec add_sup_handler(module(), atom(), term()) -> ok.
add_sup_handler(M, F, HandlerState) ->
    gen_event:add_sup_handler({local, ?MODULE}, ?MODULE,
                             {M, F, HandlerState}).

%% @doc Notify subscribers about new committed txn on the specific partition.
-spec notify_commit(antidote:partition_id(),
                    antidote:txid(),
                    {antidote:dcid(), antidote:clock_time()},
                    antidote:snapshot_time()) ->
          ok.
notify_commit(Partition, TxId, CommitTime, SnapshotTime) ->
    gen_event:sync_notify({local, ?MODULE},
                          {commit, Partition, TxId, CommitTime, SnapshotTime}).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

init({M, F, A}) ->
    {ok, #state{ handler = {M, F, A} }}.

handle_call(Msg, State) ->
    State1 = apply_handler(Msg, State),
    {ok, _Reply = ok, State1}.

handle_event(Msg, State) ->
    try
        State1 = apply_handler(Msg, State),
        {ok, State1}
    catch T:E:S ->
            logger:error("Handler crashed: ~p:~p Stack: ~p~n", [T, E, S]),
            remove_handler
    end.

terminate(_Arg, _State) ->
    ok.

apply_handler({commit, Info}, State = #state{handler = {M, F, HandlerState0}}) ->
    {ok, HandlerState1} = apply(M, F, [Info, HandlerState0]),
    State#state{handler = {M, F, HandlerState1}}.
