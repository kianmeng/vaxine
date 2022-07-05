-module(vx_wal_stream).
-behaviour(poolboy_worker).
-behaviour(gen_statem).

-export([ start_link/1,
          start_replication/1,
          stop_replication/1,
          notify_commit/2
        ]).

-export([ init/1,
          init_stream/3,
          stream_data/3,
          await_data/3,
          callback_mode/0,
          terminate/3,
          code_change/4
        ]).

%% -include_lib("kernel/src/disk_log.hrl").
-record(continuation,         %% Chunk continuation.
        {pid = self() :: pid(),
         pos          :: non_neg_integer() | {integer(), non_neg_integer()},
         b            :: binary() | [] | pos_integer()
        }).
-include_lib("stdlib/include/ms_transform.hrl").

-include_lib("antidote/include/antidote.hrl").
-include("vx_wal_stream.hrl").

-define(POLL_RETRY_MIN, 10).
-define(POLL_RETRY_MAX, timer:seconds(10)).

-record(data, {client :: pid() | undefined,
               mon_ref:: reference() | undefined,
               file_desc :: file:fd() | undefined,
               file_name :: file:filename_all(),
               file_buff = [] :: term(),
               file_pos = 0 :: non_neg_integer(),
               txns_buffer :: txns_noncomitted_map(),
               %% Last read TxId. May be either comitted or aborted
               last_read_txid :: antidote:txid() | undefined,
               %% Last txid we have been notified about
               last_notif_txid :: antidote:txid() | undefined,
               poll_tref :: reference() | undefined,
               poll_backoff :: backoff:backoff(),
               partition :: antidote:partition_id()
              }).

-record(commit, { partition :: antidote:partition_id(),
                  txid :: antidote:txid(),
                  snapshot :: snapshot_time()
                }).

start_link(Args) ->
    gen_statem:start_link(?MODULE, Args, []).

%% @doc
start_replication(_DiskLogPos) ->
    case poolboy:checkout(?MODULE, false) of
        full ->
            {error, pool_is_full};
        Pid ->
            gen_statem:call(Pid, {start_replication}, infinity)
    end.

stop_replication(Pid) ->
    gen_statem:call(Pid, {stop_replication}, infinity).

%% @doc Callback for logging_vnode to notify us about new transactions
-spec notify_commit(term(), pid()) -> ok.
notify_commit({commit, Partition, TxId, CommitTime, SnapshotTime} = V, Pid) ->
    try
        logger:info("commit notification ~p~n~p~n", [TxId, V]),
        true = ets:update_element(wal_replication_status, {Partition, Pid},
                           { #wal_replication_status.txdata,
                             {TxId, CommitTime, SnapshotTime}
                           }),
        RFun = ets:fun2ms(
                 fun(#wal_replication_status{key = {Partition0, Pid0},
                                             notification = ready
                                            } = W)
                    when Partition0 == Partition,
                         Pid0 == Pid->
                         W#wal_replication_status{notification = sent}
                 end),

        case ets:select_replace(wal_replication_status, RFun) of
            0 -> ok;
            1 ->
                Pid ! #commit{ partition = Partition, txid = TxId,
                               snapshot = SnapshotTime
                             }
        end,
        ok
    catch T:E:_ ->
            logger:error("Notification failed ~p~n", [{T, E}]),
            ok
    end.

init_notification_slot(Partition) ->
    logger:info("Init notification slot for partition ~p~n", [Partition]),
    ets:insert(wal_replication_status,
               #wal_replication_status{key = {Partition, self()},
                                       notification = ready
                                      }).

-spec mark_as_ready_for_notification(antidote:partition_id()) -> ok.
mark_as_ready_for_notification(Partition) ->
    logger:debug("Mark as ready to continue wal streaming: ~p~n", [Partition]),
    true = ets:update_element(wal_replication_status, {Partition, self()},
                              [{#wal_replication_status.notification, ready}]),
    ok.

callback_mode() ->
    state_functions.

init(_Args) ->
    %% FIXME: Only work with a single partition at the moment
    %% FIXME: Move this initialization back to start_replication
    [Partition | _] = dc_utilities:get_all_partitions(),
    %% We would like to know where the file is located
    InfoList = disk_log:info(log_path(Partition)),
    LogFile = proplists:get_value(file, InfoList),
    halt    = proplists:get_value(type, InfoList), %% Only handle halt type of the logs

       {ok, init_stream, #data{file_name = LogFile,
                            txns_buffer = dict:new(),
                            partition = Partition
                           }}.

%% Copied from logging_vnode and simplified for our case
log_path(Partition) ->
    LogFile = integer_to_list(Partition),
    {ok, DataDir} = application:get_env(antidote, data_dir),
    LogId = LogFile ++ "--" ++ LogFile,
    filename:join(DataDir, LogId).


init_stream({call, {Sender, _} = F}, {start_replication}, Data) ->
    %% FIXME: We support only single partition for now
    {ok, FD} = open_log(Data#data.file_name),
    MonRef = erlang:monitor(process, Sender),
    Backoff0 = backoff:init(?POLL_RETRY_MIN, ?POLL_RETRY_MAX,
                           self(), poll_retry),
    Backoff1 = backoff:type(Backoff0, jitter),

    ok = logging_notification_server:add_handler(
           ?MODULE, notify_commit, [self()]),
    true = init_notification_slot(Data#data.partition),

    {next_state, stream_data, Data#data{client = Sender,
                                        mon_ref = MonRef,
                                        file_desc = FD,
                                        poll_backoff = Backoff1
                                       },
     [{state_timeout, 0, wtf}, {reply, F, ok}]}.

stream_data(_, wtf, Data) ->
   continue_streaming(Data);

stream_data(_, state_timeout, Data) ->
    continue_streaming(Data).

await_data(_, #commit{txid = TxId}, #data{poll_tref = undefined} = Data) ->
    continue_streaming(Data#data{last_notif_txid = TxId});
await_data(_, #commit{}, Data) ->
    {keep_state, Data};
await_data(_, {timeout, TRef, poll_retry}, Data = #data{poll_tref = TRef}) ->
    %% FIXME: set to debug
    logger:info("poll retry: ~p~n", [Data#data.poll_backoff]),
    continue_streaming(Data#data{poll_tref = undefined});
await_data(_, Msg, Data) ->
    %% FIXME: handle {stop_replication} message here
    logger:info("Ignored message: ~p~n", [Msg]),
    {keep_state, Data}.

%%------------------------------------------------------------------------------

continue_streaming(#data{partition = Partition} = Data) ->
    logger:info("Continue wal streaming for client ~p on partition ~p"
                " at position ~p~n",
                 [Data#data.client, Data#data.partition, Data#data.file_pos]),

    case read_ops_from_log(Data) of
        {eof, N, Data1} ->
            {_, Backoff} = case N of
                          0 -> backoff:fail(Data#data.poll_backoff);
                          N when N > 0 -> backoff:succeed(Data#data.poll_backoff)
                      end,
            Data2 = Data1#data{poll_backoff = Backoff},

            ok = mark_as_ready_for_notification(Partition),
            case fetch_latest_position(Partition) of
                undefined ->
                    {next_state, await_data, Data2};
                {TxId, _, _} when TxId == Data2#data.last_read_txid ->
                    {next_state, await_data, Data2};
                {TxId, _, _} when TxId == Data2#data.last_notif_txid ->
                    %% No matter whether or not we read something new,
                    %% we didn't read TxId transaction, so set the poll timer
                    %% based on backoff value
                    {next_state, await_data, set_poll_timer(Data2)};
                {TxId, _, _} ->
                    %% New TxId since we last checked. For now I would like
                    %% to keep it as a separate case here.
                    {next_state, await_data,
                     set_poll_timer(Data2#data{last_notif_txid = TxId})}
            end;
        {error, _} = Error ->
            {stop, Error}
    end.

set_poll_timer(Data) ->
    Data#data{poll_tref = backoff:fire(Data#data.poll_backoff)}.

-spec fetch_latest_position(antidote:partition_id()) -> undefined |
          {antidote:txid(), antidote:clock_time(), antidote:snapshot_time()}.
fetch_latest_position(Partition) ->
    ets:lookup_element(wal_replication_status,
                       {Partition, self()}, #wal_replication_status.txdata).

materialize(Key, Type, ST, TxId) ->
    {Partition, _} = log_utilities:get_key_partition(Key),
    %% FIXME: Yeah, we do not expect this to fail for now
    {ok, Snapshot} =
        materializer_vnode:read(
          Key, Type, ST, TxId, _PropertyList = [], Partition),
    Snapshot.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%% only initial position is supported at the moment.
open_log(LogFile) ->
    {ok, FD} = file:open(LogFile, [raw, binary, read]),
    {ok, _Head} = file:read(FD, _Header = 8),
    {ok, FD}.

read_ops_from_log(Data) ->
    read_ops_from_log(Data, 0).

read_ops_from_log(#data{txns_buffer = TxnBuff,
                        file_pos = FPos,
                        file_buff = FBuff,
                        file_desc = Fd
                       } = Data, N) ->
    case read_ops_from_log(Fd, Data#data.file_name, FPos, FBuff, TxnBuff) of
        {eof, LogPosition, {NonComittedMap, ComittedData}} ->
            %% FIXME: What is the position here?
            {FPos1, FBuff1} = LogPosition,
            {ok, Data1} =
                notify_client(ComittedData, Data#data{txns_buffer = NonComittedMap,
                                                      file_pos = FPos1,
                                                      file_buff = FBuff1
                                                     }),
            {eof, N, Data1};
        {error, _} = Error ->
            Error;
        {ok, LogPosition, {NonComittedMap, ComittedData}} ->
            {FPos1, FBuff1} = LogPosition,
            {ok, Data1} =
                notify_client(ComittedData, Data#data{file_pos = FPos1,
                                                      file_buff = FBuff1,
                                                      txns_buffer = NonComittedMap
                                                     }),
            read_ops_from_log(Data1, N+1)
    end.

-type txns_noncomitted_map() :: dict:dict(antidote:txid(), [any_log_payload()]).
-type txns_comitted()   :: [ { antidote:txid(), term() } ].

-spec read_ops_from_log(file:fd(), file:filename_all(), non_neg_integer(), term(),
                        txns_noncomitted_map()) ->
          {error, term()} |
          {ok | eof, log_position(),
           { txns_noncomitted_map(), txns_comitted() }
          }.
read_ops_from_log(Fd, FileName, FPos, FBuffer, RemainingOps) ->
    case read_chunk(Fd, FileName, FPos, FBuffer, 100) of
        {eof, LogPosition, []} ->
            {eof, LogPosition, {RemainingOps, []}};
        {error, _} = Error ->
            Error;
        {ok, LogPosition, NewTerms}->
            {ok, LogPosition, process_txns(NewTerms, RemainingOps, [])}
    end.

-type log_position() :: {non_neg_integer(), Buffer :: term()}.
-spec read_chunk(file:fd(), file:filename_all(), non_neg_integer(), term(), non_neg_integer()) ->
          {ok, log_position(), [ term() ]} |
          {error, term()} |
          {eof, log_position(), [ term() ]}.
read_chunk(Fd, FileName, FPos, FBuff, Amount) ->
    R = disk_log_1:chunk_read_only(Fd, FileName, FPos, FBuff, Amount),
    %% Create terms from the binaries returned from chunk_read_only/5.
    %% 'foo' will do here since Log is not used in read-only mode.
    case disk_log:ichunk_end(R, _Log = foo) of
        {#continuation{pos = FPos1, b = Buffer1}, Terms}
          when FPos == FPos1 ->
            %% The same block but different term position
            {ok, {FPos1, Buffer1}, Terms};
        {#continuation{pos = FPos1, b = Buffer1}, Terms} ->
            {ok, {FPos1, Buffer1}, Terms};
        {error, _} = Error ->
            Error;
        eof ->
            %% That's ok, just need to keep previous position
            {eof, {FPos, FBuff}, []}
    end.

process_txns([], RemainingOps, FinalizedTxns) ->
    {RemainingOps, preprocess_comitted(lists:reverse(FinalizedTxns))};
process_txns([{_, LogRecord} | Rest], RemainingOps, FinalizedTxns0) ->
    #log_record{log_operation = LogOperation} = log_utilities:check_log_record_version(LogRecord),

    {RemainingOps1, FinalizedTxns1} =
        process_op(LogOperation, RemainingOps, FinalizedTxns0),
    process_txns(Rest, RemainingOps1, FinalizedTxns1).

process_op(#log_operation{op_type = update, tx_id = TxId, log_payload = Payload},
           RemainingOps, FinalizedTxns) ->
    {dict:append(TxId, Payload, RemainingOps), FinalizedTxns};
process_op(#log_operation{op_type = prepare}, RemainingOps, FinalizedTxns) ->
    {RemainingOps, FinalizedTxns};
process_op(#log_operation{op_type = abort, tx_id = TxId}, RemainingOps, FinalizedTxns) ->
     case dict:take(TxId, RemainingOps) of
        {_, RemainingOps1} ->
             %% NOTE: We still want to know about this transaction to not loose
             %% track of last transaction id.
            {RemainingOps1,
             [prepare_txn_operations(TxId, aborted) |FinalizedTxns]};
        error ->
            logger:warning("Empty transaction: ~p~n", [TxId]),
            {RemainingOps, FinalizedTxns}
    end;
process_op(#log_operation{op_type = commit, tx_id = TxId, log_payload = Payload},
           RemainingOps, FinalizedTxns) ->
    #commit_log_payload{commit_time = {DcId, TxCommitTime},
                        snapshot_time = ST
                       } = Payload,
    TxST = vectorclock:set(DcId, TxCommitTime, ST),

    case dict:take(TxId, RemainingOps) of
        {TxOpsList, RemainingOps1} ->
            {RemainingOps1,
             [prepare_txn_operations(TxId, TxST, TxOpsList)
             | FinalizedTxns]};
        error ->
            logger:warning("Empty transaction: ~p~n", [TxId]),
            {RemainingOps, FinalizedTxns}
    end.

prepare_txn_operations(TxId, ST, TxOpsList0) ->
    logger:info("preprocess txn:~n ~p ~p ~p~n", [TxId, ST, TxOpsList0]),

    %% FIXME: Do we do materialization here?
    TxOpsList1 =
        lists:map(fun(#update_log_payload{key = K, type = T}) ->
                          {K, T, materialize(K, T, ST, TxId)}
                  end, TxOpsList0),

    logger:info("processed txn:~n ~p ~p~n", [TxId, TxOpsList1]),

    {TxId, TxOpsList1}.

prepare_txn_operations(TxId, aborted) ->
    {TxId, aborted}.

preprocess_comitted(L) ->
    L.

notify_client([], Data) ->
    {ok, Data};
notify_client(FinalyzedTxns, #data{client = ClientPid} = Data) ->
    {ok, LastTxId} = notify_client0(FinalyzedTxns, undefined, ClientPid),
    {ok, Data#data{last_read_txid = LastTxId}}.

notify_client0([{TxId, TxOpsList} | FinalyzedTxns], _LastTxn, ClientPid)
 when is_list(TxOpsList)->
    ClientPid ! {TxId, TxOpsList},
    notify_client0(FinalyzedTxns, TxId, ClientPid);
notify_client0([{TxId, aborted} | FinalyzedTxns], _LastTxn, ClientPid) ->
    notify_client0(FinalyzedTxns, TxId, ClientPid);
notify_client0([], LastTxn, _) ->
    {ok, LastTxn}.

-ifdef(TEST).

-endif.
