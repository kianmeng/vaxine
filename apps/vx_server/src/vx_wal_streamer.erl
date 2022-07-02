-module(vx_wal_streamer).
-behaviour(poolboy_worker).
-behaviour(gen_statem).

-export([ start_link/1,
          start_replication/1,
          notify_commit/2
        ]).

-export([ init/1,
          callback_mode/0,
          terminate/3,
          code_change/4
        ]).
-export([ initial_position/3 ]).

%% -include_lib("kernel/src/disk_log.hrl").
-record(continuation,         %% Chunk continuation.
	{pid = self() :: pid(),
	 pos          :: non_neg_integer() | {integer(), non_neg_integer()},
	 b            :: binary() | [] | pos_integer()}
	).
-include_lib("antidote/include/antidote.hrl").

-record(start_replication,
        { pos
        }).

-record(data, {client :: pid() | undefined,
               mon_ref:: reference() | undefined,
               file_desc :: file:fd() | undefined,
               file_name :: file:filename_all(),
               file_buff = [] :: term(),
               file_pos = 0 :: non_neg_integer(),
               txns_buffer :: txns_noncomitted_map()
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
            gen_statem:call(Pid, #start_replication{}, infinity)
    end.

%% @doc Callback for logging_vnode to notify us about new transactions
notify_commit({commit, Partition, TxId, _CommitTime, SnapshotTime}, Pid) ->
   Pid ! #commit{ partition = Partition, txid = TxId, snapshot = SnapshotTime }.

callback_mode() ->
    state_functions.

init(_Args) ->
    [Partition] = dc_utilities:get_all_partitions(),
    %% We would like to know where the file is located
    InfoList = disk_log:info(Partition),
    LogFile = proplists:get_value(file, InfoList),
    halt    = proplists:get_value(type, InfoList), %% Only handle halt type of the logs

    {ok, initial_position, #data{file_name = LogFile,
                                 txns_buffer = dict:new()
                                }}.

initial_position({call, Sender}, #start_replication{pos = Pos}, Data) ->
    %% FIXME: We support only single partition for now
    {ok, FD} = open_log(Data#data.file_name),
    MonRef = erlang:monitor(process, Sender),
    case read_ops_from_log(Data#data{client = Sender,
                                     mon_ref = MonRef,
                                     file_desc = FD,
                                     txns_buffer = dict:new()
                                    })
    of
        %% {ok, Data1} ->
        %%     {next_state, process_file, Data1};
        {eof, Data1} ->
            ok = logging_notification_server:add_sup_handler(
                   ?MODULE, notify_commit, self()),
            case fetch_latest_position(Data1) of
                {eof, Data2} ->
                    %% Ask logging_vnode to wake us up next chunk arrives
                    {next_state, await_data, Data2}
                %% {continue, Data2} ->
                %%     {next_state, process_file, Data2}
            end
    end.

fetch_latest_position(Data) ->
    {eof, Data}.

materialize(#clocksi_payload{key = Key, type = Type, snapshot_time = SN, txid = TxId}) ->
    Partition = log_utilities:get_key_partition(Key),
    %% FIXME: Yeah, we do not expect this to fail, as we have read that from the log.
    {ok, Snapshot} =
        materializer_vnode:read(Key, Type, SN, TxId, _PropertyList = [], Partition),
    Snapshot.

update_position(_, _, _) ->
    {error, not_implemented}.

get_continuation(_) ->
    {error, not_implemented}.


code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%% only initial position is supported at the moment.
open_log(LogFile) ->
    {ok, FD} = file:open(LogFile, [raw, binary, read]),
    {ok, _Head} = file:read(FD, _Header = 8),
    {ok, FD}.

read_ops_from_log(#data{txns_buffer = TxnBuff,
                        file_pos = FPos,
                        file_buff = FBuff,
                        file_desc = Fd
                       } = Data) ->
    case read_ops_from_log(Fd, Data#data.file_name, FPos, FBuff, TxnBuff) of
        {eof, LogPosition, {NonComittedMap, ComittedData}} ->
            %% FIXME: What is the position here?
            {FPos1, FBuff1} = LogPosition,
            {ok, Data1} =
                notify_client(ComittedData, Data#data{txns_buffer = NonComittedMap,
                                                  file_pos = FPos1,
                                                  file_buff = FBuff1
                                                 }),
            {eof, Data1};
        {error, _} = Error ->
            Error;
        {ok, LogPosition, {NonComittedMap, ComittedData}} ->
            {FPos1, FBuff1} = LogPosition,
            {ok, Data1} =
                notify_client(ComittedData, Data#data{file_pos = FPos1,
                                                      file_buff = FBuff1,
                                                      txns_buffer = NonComittedMap
                                                     }),
            read_ops_from_log(Data1)
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

process_txns([], RemainingOps, ComittedTxns) ->
    {RemainingOps, preprocess_comitted(lists:reverse(ComittedTxns))};
process_txns([{_, LogRecord} | Rest], RemainingOps, ComittedTxns0) ->
    #log_record{log_operation = LogOperation} = log_utilities:check_log_record_version(LogRecord),

    {RemainingOps1, ComittedTxns1} =
        process_op(LogOperation, RemainingOps, ComittedTxns0),
    process_txns(Rest, RemainingOps1, ComittedTxns1).

process_op(#log_operation{op_type = update, tx_id = TxId, log_payload = Payload},
           RemainingOps, ComittedTxns) ->
    {dict:append(TxId, Payload, RemainingOps), ComittedTxns};
process_op(#log_operation{op_type = commit, tx_id = TxId, log_payload = Payload},
           RemainingOps, ComittedTxns) ->
    #commit_log_payload{commit_time = {_DcId, _TxCommitTime},
                        snapshot_time = _SnapshotTime
                       } = Payload,
    case dict:take(TxId, RemainingOps) of
        {TxOpsList, RemainingOps1} ->
            {RemainingOps1,
             [prepare_txn_operations(TxId, TxOpsList) | ComittedTxns]};
        error ->
            logger:warning("Empty transaction: ~p~n", [TxId]),
            {RemainingOps, ComittedTxns}
    end.

prepare_txn_operations(TxId, TxOpsList) ->
    %% FIXME: Do we do materialization here?
    {TxId, TxOpsList}.

preprocess_comitted(L) ->
    L.

notify_client([], Data) ->
    {ok, Data};
notify_client(ComittedData, #data{client = Pid} = Data) ->
    Pid ! ComittedData,
    {ok, Data}.

-ifdef(TEST).

-endif.
