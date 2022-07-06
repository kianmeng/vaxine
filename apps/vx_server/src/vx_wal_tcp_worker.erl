-module(vx_wal_tcp_worker).
-behaviour(ranch_protocol).
-behaviour(gen_server).

-export([ start_link/3 ]).
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
        handle_vx_wal_protocol(binary_to_term(Data), State)
    of
        {ok, Req} ->
            handle_request(Req, State);
        {error, Error} ->
            ok = Transport:send(Socket, mk_error(Error)),
            {noreply, State}
    catch
        Type:Error:Stack ->
            logger:error("~p: ~p~n~p~n When handling request ~p~n",
                         [Type, Error, Stack, Data]),
            ok = Transport:send(Socket, mk_error(bad_proto)),
            {stop, {error, bad_proto}}
    end;
handle_info({tcp_error, _, Reason}, #state{socket = Socket,
                                           transport = Transport,
                                           wal_stream = Pid
                                          }) ->
    logger:error("Socket error: ~p", [Reason]),
    Transport:close(Socket),
    ok = vx_wal_stream:stop_replication(Pid),
    {stop, {error, Reason}};
handle_info({tcp_closed, _}, #state{}) ->
    {stop, normal}.

terminate(_, _) ->
    ok.

%%------------------------------------------------------------------------------

handle_vx_wal_protocol(Req, _State) ->
    Req.

handle_request({start_replication, _Opts}, State) ->
    case vx_wal_stream:start_replication([]) of
        {ok, Pid} ->
            {reply, ok, State#state{wal_stream = Pid}};
        {error, Reason} ->
            {reply, mk_error(Reason), State}
    end;
handle_request({ack_replication}, #state{wal_stream = Pid} = State) ->
    case vx_wal_stream:ack_replication(Pid) of
        ok ->
            {reply, ok, State};
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
    {error, bad_proto};
mk_error(_) ->
    {error, unknown_error}.
