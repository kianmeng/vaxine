%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(eiger_vnode).
-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(EIGER_MASTER, eiger_vnode_master).

-export([start_vnode/1,
         read_key/4,
         read_key_time/5,
         prepare/4,
         remote_prepare/4,
         commit/7,
         coordinate_tx/4,
         check_deps/2,
         notify_tx/2,
         propagated_group_txs/1,
         clean_propagated_tx_fsm/3,
         get_clock/1,
         update_clock/2,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         eiger_ts_lt/2,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-ignore_xref([start_vnode/1]).

-record(state, {partition,
                prop_txs :: cache_id(),
                idle_fsms=queue:new() :: queue(),
                queue_fsm=queue:new() :: queue(),
                min_pendings :: cache_id(),
                buffered_reads :: cache_id(),
                pending :: cache_id(),
                fsm_deps :: cache_id(),
                deps_keys :: cache_id(),
                clock=0 :: integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


check_deps(Node, Deps) ->
    riak_core_vnode_master:command(Node,
                                   {check_deps, Deps},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

propagated_group_txs(Transactions) ->
    lists:foreach(fun(Txn) ->
                    {Txid,_Commitime,_ST, _Deps, _Ops, _TOps} = Txn,
                    Preflist = log_utilities:get_preflist_from_key(Txid),
                    Indexnode = hd(Preflist),
                    notify_tx(Indexnode, Txn)
                  end, Transactions),
    ok.

notify_tx(Node, Transaction)->
    riak_core_vnode_master:command(Node,
                                   {notify_tx, Transaction},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER). 
clean_propagated_tx_fsm(Node, TxId, FsmRef)->
    riak_core_vnode_master:command(Node,
                                   {clean_propagated_tx_fsm, TxId, FsmRef},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER). 

read_key(Node, Key, Type, TxId) ->
    riak_core_vnode_master:command(Node,
                                   {read_key, Key, Type, TxId},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

read_key_time(Node, Key, Type, TxId, Clock) ->
    riak_core_vnode_master:command(Node,
                                   {read_key_time, Key, Type, TxId, Clock},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

prepare(Node, Transaction, Clock, Keys) ->
    riak_core_vnode_master:command(Node,
                                   {prepare, Transaction, Clock, Keys},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

remote_prepare(Node, TxId, TimeStamp, Keys) ->
    riak_core_vnode_master:command(Node,
                                   {remote_prepare, TxId, TimeStamp, Keys},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).
commit(Node, Transaction, Updates, Deps, TimeStamp, Clock, TotalOps) ->
    riak_core_vnode_master:command(Node,
                                   {commit, Transaction, Updates, Deps, TimeStamp, Clock, TotalOps},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

coordinate_tx(Node, Updates, Deps, Debug) ->
    riak_core_vnode_master:sync_command(Node,
                                        {coordinate_tx, Updates, Deps, Debug},
                                        ?EIGER_MASTER,
                                        infinity).

get_clock(Node) ->
    riak_core_vnode_master:sync_command(Node,
                                        get_clock,
                                        ?EIGER_MASTER,
                                        infinity).

update_clock(Node, Clock) ->
    riak_core_vnode_master:sync_command(Node,
                                        {update_clock, Clock},
                                        ?EIGER_MASTER,
                                        infinity).

init_prop_fsms(_Vnode, Queue0, 0) ->
    {ok, Queue0};

init_prop_fsms(Vnode, Queue0, Rest) ->
    {ok, FsmRef} = eiger_propagatedtx_coord_fsm:start_link(Vnode),
    Queue1 = queue:in(FsmRef, Queue0),
    init_prop_fsms(Vnode, Queue1, Rest-1).
    
%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    {ok, Queue} = init_prop_fsms({Partition, node()}, queue:new(), ?NPROP_FSMS), 
    PropTxs = open_table(prop_txs, Partition),
    MinPendings = open_table(min_pendings, Partition),
    BufferedReads = open_table(buffered_reads, Partition),
    Pending = open_table(pending, Partition),
    FsmDeps = open_table(fsm_deps, Partition),
    DepsKeys = open_table(deps_keys, Partition),
    {ok, #state{partition=Partition, min_pendings=MinPendings, buffered_reads=BufferedReads, pending=Pending, fsm_deps=FsmDeps, deps_keys=DepsKeys, prop_txs=PropTxs, idle_fsms=Queue}}.

handle_command({check_deps, Deps}, Sender, S0=#state{fsm_deps=FsmDeps, deps_keys=DepsKeys, partition=_Partition}) ->
    RestDeps = lists:foldl(fun({Key, TimeStamp}=Dep, Acc) ->
                            {Key, _Value, _EVT, _Clock, TS2} = do_read(Key, ?EIGER_DATATYPE, latest, latest, S0),
                            case eiger_ts_lt(TS2, TimeStamp) of
                                true ->
                                    Acc ++ [Dep];
                                false ->
                                    Acc
                            end
                        end, [], Deps),
    case length(RestDeps) of
        0 ->
            {reply, deps_checked, S0};
        Other ->
            ets:insert(FsmDeps, {Sender, Other}),
            lists:foreach(fun({Key, TimeStamp}=_Dep) ->
                        %dict:append(Key, {TimeStamp, Sender}, Acc)
                        case ets:lookup(DepsKeys, Key) of
                            [] ->  ets:insert(DepsKeys, {Key, [TimeStamp, Sender]});
                            [{Key, L}] -> ets:insert(DepsKeys, {Key, [{TimeStamp, Sender}|L]})
                        end end, RestDeps),
            {noreply, S0}
    end;

handle_command({clean_propagated_tx_fsm, TxId, FsmRef}, _Sender, S0=#state{prop_txs=PropTxs, queue_fsm=Queue0, idle_fsms=IdleFsms0}) ->
    true = ets:delete(PropTxs, TxId),
    case queue:out(Queue0) of
        {{value, {TxIdPending, CommitTime, Deps}}, Queue1} ->
            [{TxIdPending, {queue, ListOps}}] = ets:lookup(PropTxs, TxIdPending),
            gen_fsm:send_event(FsmRef, {new_tx, TxId, CommitTime, Deps, ListOps}),
            true = ets:insert(PropTxs, {TxId, {running, FsmRef}}),
            {noreply, S0#state{queue_fsm=Queue1}};
        {empty, _Queue1} ->
            IdleFsms1 = queue:in(FsmRef, IdleFsms0),
            {noreply, S0#state{idle_fsms=IdleFsms1}}
    end;

handle_command({notify_tx, Transaction}, _Sender, State0=#state{prop_txs=PropTxs, partition=Partition, queue_fsm=Queue0, idle_fsms=IdleFsms0}) ->
    {TxId, CommitTime, _ST, Deps, Ops, _TOps} = Transaction,
    case ets:lookup(PropTxs, TxId) of
        [{TxId, {running, FsmRef}}] ->
            gen_fsm:send_event(FsmRef, {notify, Ops, {Partition, node()}}),
            {noreply, State0};
        [{TxId, {queue, OpsList}}] ->
            true = ets:insert(PropTxs, {TxId, {queue, [Ops|OpsList]}}),
            {noreply, State0};
        [] ->
            case queue:out(IdleFsms0) of
                {{value, FsmRef}, IdleFsms1} ->
                    gen_fsm:send_event(FsmRef, {new_tx, TxId, CommitTime, Deps, [Ops]}),
                    true = ets:insert(PropTxs, {TxId, {running, FsmRef}}),
                    {noreply, State0#state{idle_fsms=IdleFsms1}};
                true ->
                    Queue1 = queue:in({TxId, CommitTime, Deps}, Queue0),
                    true = ets:insert(PropTxs, {TxId, {queue, [Ops]}}),
                    {noreply, State0#state{queue_fsm=Queue1}}
            end
    end;

%% @doc starts a read_fsm to handle a read operation.
handle_command({read_key, Key, Type, TxId}, _Sender,
               #state{clock=Clock, min_pendings=MinPendings}=State) ->
    case ets:lookup(MinPendings, Key) of
        [{Key, _Min}] ->
            {reply, {Key, empty, empty, Clock, empty}, State};
        [] ->
            Reply = do_read(Key, Type, TxId, latest, State),
            {reply, Reply, State}
    end;

handle_command({read_key_time, Key, Type, TxId, Time}, Sender,
               #state{clock=Clock0, buffered_reads=BufferedReads, min_pendings=MinPendings}=State) ->
    Clock = max(Clock0, Time),
    case ets:lookup(MinPendings, Key) of
        [{Key, Min}] ->
            case Min =< Time of
                true ->
                    Orddict = case ets:lookup(BufferedReads, Key) of
                                [{Key, Orddict0}] ->
                                    orddict:store(Time, {Sender, Type, TxId}, Orddict0);
                                [] ->
                                    Orddict0 = orddict:new(),
                                    orddict:store(Time, {Sender, Type, TxId}, Orddict0)
                              end,
                    ets:insert(BufferedReads, {Key, Orddict}),
                    {noreply, State#state{clock=Clock, buffered_reads=BufferedReads}};
                false ->
                    Reply = do_read(Key, Type, TxId, Time, State#state{clock=Clock}),
                    {reply, Reply, State#state{clock=Clock}} 
            end;
        [] ->
            Reply = do_read(Key, Type, TxId, Time, State#state{clock=Clock}),
            {reply, Reply, State#state{clock=Clock}} 
    end;

handle_command({coordinate_tx, Updates, Deps, Debug}, Sender, #state{partition=Partition}=State) ->
    Vnode = {Partition, node()},
    {ok, _Pid} = eiger_updatetx_coord_fsm:start_link(Vnode, Sender, Updates, Deps, Debug),
    {noreply, State};

handle_command({remote_prepare, TxId, TimeStamp, Keys0}, _Sender, #state{clock=Clock0, partition=Partition}=S0) ->
    {_DC1, C1} = TimeStamp,
    Clock = max(Clock0, C1) + 1,
    Keys1 = lists:foldl(fun(Key, Acc) ->
                            {Key, _Value, _EVT, _Clock, TS2} = do_read(Key, ?EIGER_DATATYPE, TxId, latest, S0#state{clock=Clock}),
                            case eiger_ts_lt(TS2, TimeStamp) of
                                true ->
                                    Acc ++ [Key];
                                false ->
                                    Acc
                            end
                        end, [], Keys0),
    %lager:info("Keys ~p to be prepared. TimeStamp: ~p", [Keys1, TimeStamp]),
    S1 = do_prepare(TxId, Clock, Keys1, S0),
    {reply, {prepared, Clock, Keys1, {Partition, node()}}, S1#state{clock=Clock}};

handle_command({prepare, Transaction, CoordClock, Keys}, _Sender, S0=#state{clock=Clock0}) ->
    Clock = max(Clock0, CoordClock) + 1,
    S1 = do_prepare(Transaction#transaction.txn_id, Clock, Keys, S0),
    {reply, {prepared, Clock}, S1#state{clock=Clock}};

handle_command({commit, Transaction, Updates, Deps, TimeStamp, CommitClock, TotalOps}, _Sender, State0=#state{clock=Clock0}) ->
    Clock = max(Clock0, CommitClock),
    case update_keys(Updates, Deps, Transaction, TimeStamp, CommitClock, TotalOps, State0) of
        {ok, State} ->
            {reply, {committed, CommitClock}, State#state{clock=Clock}};
        {error, Reason} ->
            {reply, {error, Reason}, State0#state{clock=Clock}}
    end;

handle_command(get_clock, _Sender, S0=#state{clock=Clock}) ->
    {reply, {ok, Clock}, S0};

handle_command({update_clock, NewClock}, _Sender, S0=#state{clock=Clock0}) ->
    Clock =  max(Clock0, NewClock),
    {reply, ok, S0#state{clock=Clock}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

update_keys(Ups, Deps, Transaction, {_DcId, _TimeStampClock}=TimeStamp, CommitTime, TotalOps, State0) ->
    Payloads = lists:foldl(fun(Update, Acc) ->
                    {Key, Type, _} = Update,
                    TxId = Transaction#transaction.txn_id,
                    LogRecord = #log_record{tx_id=TxId, op_type=update, op_payload={Key, Type, Update}},
                    LogId = log_utilities:get_logid_from_key(Key),
                    [Node] = log_utilities:get_preflist_from_key(Key),
                    case Deps of
                        nodeps ->
                            {ok, _} = logging_vnode:nonlocal_append(Node,LogId,LogRecord);
                        _ ->    
                            {ok, _} = logging_vnode:append(Node,LogId,LogRecord)
                    end,
                    CommittedOp = #clocksi_payload{
                                            key = Key,
                                            type = Type,
                                            op_param = Update,
                                            snapshot_time = Transaction#transaction.vec_snapshot_time,
                                            commit_time = TimeStamp,
                                            evt = CommitTime,
                                            txid = Transaction#transaction.txn_id},
                    [CommittedOp|Acc] end, [], Ups),
    FirstOp = hd(Ups),
    {Key, _, _} = FirstOp,
    LogId = log_utilities:get_logid_from_key(Key),
    [Node] = log_utilities:get_preflist_from_key(Key),
    TxId = Transaction#transaction.txn_id,
    PreparedLogRecord = #log_record{tx_id=TxId,
                                    op_type=prepare,
                                    op_payload=CommitTime},
    case Deps of
        nodeps ->
            {ok, _} = logging_vnode:nonlocal_append(Node,LogId,PreparedLogRecord);
        _ ->    
            {ok, _} = logging_vnode:append(Node,LogId,PreparedLogRecord)
    end,
    CommitLogRecord=#log_record{tx_id=TxId,
                                op_type=commit,
                                op_payload={TimeStamp, Transaction#transaction.vec_snapshot_time, Deps, TotalOps}},
    case Deps of
        nodeps ->
            ResultCommit = logging_vnode:nonlocal_append(Node,LogId,CommitLogRecord);
        _ ->    
            ResultCommit = logging_vnode:append(Node,LogId,CommitLogRecord)
    end,
    case ResultCommit of
        {ok, _} ->
            State = lists:foldl(fun(Op, S0) ->
                                    LKey = Op#clocksi_payload.key,
                                    %% This can only return ok, it is therefore pointless to check the return value.
                                    eiger_materializer_vnode:update(LKey, Op),
                                    S1 = post_commit_dependencies(LKey, TimeStamp, S0),
                                    post_commit_update(LKey, TxId, CommitTime, S1)
                                end, State0, Payloads),
            {ok, State};
        {error, timeout} ->
            error
    end.
    
post_commit_update(Key, TxId, CommitTime, State0=#state{pending=Pending, min_pendings=MinPendings, buffered_reads=BufferedReads, clock=Clock}) ->
    %lager:info("Key to post commit : ~p", [Key]),
    [{Key, List0}] = ets:lookup(Pending, Key),
    {List, PrepareTime} = delete_pending_entry(List0, TxId, []),
    case List of
        [] ->
            ets:delete(Pending, Key),
            ets:delete(MinPendings, Key),
            case ets:lookup(BufferedReads, Key) of
                [{Key, Orddict0}] ->
                    lists:foreach(fun({Time, {Client, TypeB, TxIdB}}) ->
                                    Reply = do_read(Key, TypeB, TxIdB, Time, State0),
                                    riak_core_vnode:reply(Client, Reply)
                                  end, Orddict0),
                    ets:delete(BufferedReads, Key),
                    State0;
                [] ->
                    State0
            end;
        _ ->
            ets:insert(Pending, {Key, List}),
            case ets:lookup(MinPendings, Key) < PrepareTime of
                true ->
                    State0#state{pending=Pending};
                false ->
                    Times = [PT || {_TxId, PT} <- List],
                    Min = lists:min(Times),
                    ets:insert(MinPendings, {Key, Min}),
                    case ets:lookup(BufferedReads, Key) of
                        [{Key, Orddict0}] ->
                            case handle_pending_reads(Orddict0, CommitTime, Key, Clock) of
                                [] ->
                                    true = ets:delete(BufferedReads, Key);
                                Orddict ->
                                    true = ets:insert(BufferedReads, {Key, Orddict})
                            end,
                            State0;
                        [] ->
                            State0
                    end
            end
    end.

post_commit_dependencies(Key, TimeStamp, S0=#state{deps_keys=DepsKeys, fsm_deps=FsmDeps, partition=_Partition}) ->
    case ets:lookup(DepsKeys, Key) of
        [{Key, List0}] ->
            List1 = lists:foldl(fun({TS2, Fsm}, Acc) ->
                                                case eiger_ts_lt(TimeStamp, TS2) of
                                                    true ->
                                                        Acc ++ [{TS2, Fsm}];
                                                    false ->
                                                        case ets:lookup(Fsm, FsmDeps) of
                                                            [{Fsm, 1}] ->
                                                                riak_core_vnode:reply(Fsm, deps_checked),
                                                                ets:delete(FsmDeps, Fsm),
                                                                Acc;
                                                            [{Fsm, Rest}] ->
                                                                ets:insert(FsmDeps, {Fsm, Rest - 1}),
                                                                Acc
                                                        end
                                                end
                                            end, [], List0),
            ets:insert(DepsKeys, {Key, List1}),
            S0;
        [] ->
            S0
    end.

delete_pending_entry([], _TxId, List) ->
    {List, not_found};

delete_pending_entry([Element|Rest], TxId, List) ->
    case Element of
        {PrepareTime, TxId} ->
            {List ++ Rest, PrepareTime};
        _ ->
            delete_pending_entry(Rest, TxId, List ++ [Element])
    end.

handle_pending_reads([], _CommitTime, _Key, _Clock) ->
    [];
handle_pending_reads([Element|Rest], CommitTime, Key, Clock) ->
    {Time, {Client, Type, TxId}} = Element,
    case Time < CommitTime of
        true ->
            Reply = do_read(Key, Type, TxId, Time, #state{clock=Clock}),
            riak_core_vnode:reply(Client, Reply),
            handle_pending_reads(Rest, CommitTime, Key, Clock);
        false ->
            [Element|Rest]
    end.

do_read(Key, Type, TxId, Time, #state{clock=Clock}) -> 
    %lager:info("Do read ~w, ~w, ~w", [Key, Type, Time]),
    case eiger_materializer_vnode:read(Key, Type, Time, TxId) of
    %case eiger_materializer_vnode:read(Key, Time) of
        {ok, Snapshot, EVT, Timestamp} ->
            Value = Type:value(Snapshot),
            %lager:info("Snapshot is ~w, EVT is ~w", [Snapshot, EVT]),
            case Time of
                latest -> 
                    {Key, Value, EVT, Clock, Timestamp};
                _ -> 
                    {Key, Value, Timestamp}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

do_prepare(TxId, Clock, Keys, S0=#state{pending=Pending, min_pendings=MinPendings}) ->
    lists:foreach(fun(Key) ->
        case ets:lookup(Pending, Key) of
            [] -> ets:insert(Pending, {Key, [{Clock, TxId}]});
            [{Key, L}] -> ets:insert(Pending, {Key, L++[{Clock, TxId}]})
        end,
        case ets:lookup(MinPendings, Key) of
            [] -> ets:insert(MinPendings, {Key, Clock});
            _ -> ok
        end
    end, Keys),
    S0.

eiger_ts_lt(TS1, TS2) ->
   %lager:info("ts1: ~p, ts2: ~p",[TS1, TS2]),
    {DC1, C1} = TS1,
    {DC2, C2} = TS2,
    case (C1 < C2) of
        true -> true;
        false ->
            case (C1 == C2) of
                true ->
                    DC1<DC2;
                false -> false
            end
    end.

open_table(Name, Partition) ->
    case ets:info(get_cache_name(Partition, Name)) of
    undefined ->
        ets:new(get_cache_name(Partition, Name),
            [set, private, named_table]);
    _ ->
        %% Other vnode hasn't finished closing tables
        timer:sleep(100),
        open_table(Name, Partition)
    end.

get_cache_name(Partition, Base) ->
    list_to_atom(atom_to_list(node()) ++ atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).
