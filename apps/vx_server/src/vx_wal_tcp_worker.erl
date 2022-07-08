-module(vx_wal_tcp_worker).
-behaviour(ranch_protocol).
-behaviour(gen_server).

-export([ start_link/3,
          send/2
        ]).
-export([ init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2
        ]).

-record(state, { socket :: port(),
                 transport :: module(),
                 wal_stream :: pid() | undefined
               }).

-spec start_link(ranch:ref(), module(), any()) -> {ok, ConnPid :: pid()}.
start_link(Ref, Transport, ProtoOpts) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [{Ref, Transport, ProtoOpts}]),
    {ok, Pid}.

-spec send(port(), term()) -> boolean() | {error, term()}.
send(Port, Data) ->
    Binary = term_to_binary(Data),
    try
        erlang:port_command(Port, Binary, [nosuspend])
    catch _:_ ->
            {error, port_closed}
    end.

init({Ref, Transport, _Opts}) ->
    {ok, Socket} = ranch:handshake(Ref),
    ok = Transport:setopts(Socket, [{packet, 4}, {active, once}]),

    gen_server:enter_loop(?MODULE, _Opts,
                          #state{socket = Socket,
                                 transport = Transport
                                }).

handle_call(_, _, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Data}, #state{transport = Transport} = State) ->
    try
        ok = Transport:setopts(Socket, [{active, once}]),
        handle_vx_wal_protocol(Data, State)
    of
        {ok, Req} ->
            case handle_request(Req, State) of
                {reply, Req1, State1} ->
                    ok = Transport:send(Socket, term_to_binary(Req1)),
                    {noreply, State1};
                {error, Error} ->
                    ok = Transport:send(Socket, mk_error(Error)),
                    {stop, {shutdown, Error}, State}
            end;
        {error, Error} ->
            ok = Transport:send(Socket, mk_error(Error)),
            {noreply, State}
    catch
        Type:Error:Stack ->
            logger:error("~p: ~p~n~p~n When handling request ~p~n",
                         [Type, Error, Stack, Data]),
            ok = Transport:send(Socket, mk_error(bad_proto)),
            {stop, {error, bad_proto}, State}
    end;
handle_info({tcp_error, _, Reason}, #state{socket = Socket,
                                           transport = Transport,
                                           wal_stream = Pid
                                          } = State) ->
    logger:error("Socket error: ~p", [Reason]),
    Transport:close(Socket),
    ok = vx_wal_stream:stop_replication(Pid),
    {stop, {error, Reason}, State};
handle_info({tcp_closed, _}, State) ->
    {stop, normal, State};

handle_info({'EXIT', _, process, Pid, Reason}, #state{wal_stream = Pid} = State) ->
    case Reason of
        normal ->
            {noreply, State#state{wal_stream = undefined}};
        _ ->
            {stop, {error, {wal_stream_crashed, Reason}}, State}
    end;
handle_info(Msg, State) ->
    logger:info("Ignored message tcp_worker: ~p~n", [Msg]),
    {noreply, State}.

terminate(_, _) ->
    ok.

%%------------------------------------------------------------------------------

handle_vx_wal_protocol(BinaryReq, _State) ->
    {ok, binary_to_term(BinaryReq)}.

handle_request({start_replication, _Opts}, State) ->
    {ok, Pid} = vx_wal_stream:start_link([]),
    case vx_wal_stream:start_replication(Pid, State#state.socket, []) of
        {ok, _} ->
            {reply, ok, State#state{wal_stream = Pid}};
        {error, Reason} ->
            {reply, mk_error(Reason), State}
    end;
handle_request({stop_replication}, #state{wal_stream = Pid} = State)
  when is_pid(Pid) ->
    case vx_wal_stream:stop_replication(Pid) of
        ok ->
            {reply, ok, State#state{wal_stream = undefined}};
        {error, Reason} ->
            {reply, mk_error(Reason), State}
    end.

mk_error(bad_proto) ->
    term_to_binary({error, bad_proto});
mk_error(_) ->
    term_to_binary({error, unknown_error}).
