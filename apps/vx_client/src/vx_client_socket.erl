-module(vx_client_socket).
-behaviour(gen_server).

-export([start_link/2, start_link/3,
         disconnect/1, subscribe/4, unsubscribe/2,
         stop/1]).

-export([start_replication/2,
         get_next_stream_bulk/2,
         stop_replication/1
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(CONN_TIMEOUT, 1000).
-define(RECONN_MIN, 500).
-define(RECONN_MAX, 5000). %% 5 seconds

-type address() :: inet:socket_address() | inet:hostname().
-type sub_id() :: vx_client:sub_id().

-record(state, { address :: address(),
                 port :: inet:port_number(),
                 opts :: [ gen_tcp:connect_option()
                        % | inet:inet_backend()
                         ],
                 socket :: gen_tcp:socket() | undefined,
                 socket_status = disconnected :: disconnected | connected,
                 connect_timeout = ?CONN_TIMEOUT :: non_neg_integer(),
                 reconnect_backoff = backoff:backoff(),
                 reconnect_tref :: reference() | undefined,
                 wal_replication = none :: active | none,
                 owner :: pid()
               }).

-spec start_link(address(), inet:port_number()) ->
          {ok, pid()} | {error, term()}.
start_link(Address, Port) ->
    start_link(Address, Port, []).

-spec start_link(address(), inet:port_number(), list()) ->
          {ok, pid()} | {error, term()}.
start_link(Address, Port, Options) when is_list(Options) ->
    gen_server:start_link(?MODULE, [Address, Port, Options, self()], []).

-spec disconnect(pid()) -> ok | {error, term()}.
disconnect(_Pid) ->
    {error, not_implemented}.

-spec subscribe(pid(), [binary()], term(), boolean()) ->
          {ok, sub_id()} | {error, term()}.
subscribe(_Pid, _Keys, _Snapshot, _SnapshotFlag) ->
    {error, not_implemented}.

-spec unsubscribe(pid(), sub_id()) -> ok | {error, term()}.
unsubscribe(_Pid, _SubId) ->
    {error, not_implemented}.

-spec stop(pid()) -> ok.
stop(Pid) ->
    call_infinity(Pid, stop).

-spec start_replication(pid(), list()) -> ok | {error, term()}.
start_replication(Pid, Opts) ->
    call_request(Pid, {start_replication, Opts}).

get_next_stream_bulk(Pid, N) ->
    gen_server:cast(Pid, {get_next_bulk, N}).

-spec stop_replication(pid()) -> ok | {error, term()}.
stop_replication(Pid) ->
    call_request(Pid, {stop_replication}).

call_request(Pid, Msg) ->
    call_infinity(Pid, {request, Msg}).

call_infinity(Pid, Msg) ->
    gen_server:call(Pid, Msg, infinity).


%%------------------------------------------------------------------------------

init([Address, Port, Options, CallingProcess]) ->
    Backoff0 = backoff:init(?RECONN_MIN, ?RECONN_MAX, self(), reconnect),
    Backoff1 = backoff:type(Backoff0, jitter),

    _ = erlang:monitor(process, CallingProcess),
    State = #state{ address = Address,
                    port = Port,
                    opts = Options,
                    reconnect_backoff = Backoff1,
                    owner = CallingProcess
                  },

    case connect_int(Address, Port, Options, State) of
        {ok, State1} ->
            {ok, State1};
        {error, Reason} ->
            case maybe_reconnect(Reason, State) of
                {noreply, State1} ->
                    {ok, State1};
                Ret ->
                    Ret
            end
    end.

handle_call({request, Msg}, {Pid, _}, #state{owner = Pid} = State) ->
    case State#state.socket_status of
        connected ->
            handle_client_msg(Msg, State);
        disconnected ->
            {reply, {error, disconnected}, State}
    end;
handle_call(stop, _From, State) ->
    {stop, normal, ok, disconnect_int(State)};
handle_call(_, _, State) ->
    {noreply, State}.

handle_cast({get_next_bulk, N}, State) ->
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Data}, State) ->
    logger:debug("tcp_data: ~p~n", [binary_to_term(Data)]),
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, State};
handle_info({tcp_error, _, Reason}, State) ->
    maybe_reconnect(Reason, State);
handle_info({tcp_closed, _}, State) ->
    maybe_reconnect(tcp_closed, State);
handle_info({timeout, Tref, reconnect}, #state{reconnect_tref = Tref} = State) ->
    Backoff = State#state.reconnect_backoff,
    case
        connect_int(State#state.address, State#state.port,
                     State#state.opts, State)
    of
        {ok, State1} ->
            {noreply, State1#state{reconnect_backoff = backoff:succeed(Backoff),
                                   reconnect_tref = undefined
                                  }};
        {error, Reason} ->
            maybe_reconnect(Reason, State)
    end;
handle_info({'EXIT', _, process, Pid, Reason}, State = #state{owner = Pid}) ->
    %% Controling process terminated, do the cleanup.
    {stop, Reason, disconnect_int(State)};
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.

%%------------------------------------------------------------------------------

handle_client_msg({start_replication, Opts}, State) ->
    case State#state.wal_replication of
        none ->
            send_request({start_replication, Opts}, State);
        active ->
            {reply, {error, already_started}, State}
    end;
handle_client_msg({stop_replication}, State) ->
    case State#state.wal_replication of
        none ->
            {reply, {error, no_active_wal_replication}, State};
        active ->
            send_request({stop_replication}, State)
    end.

send_request(Request, State) ->
    case gen_tcp:send(State#state.socket, term_to_binary(Request)) of
        ok ->
            {reply, ok, State};
        {error, Reason} = Error ->
            case maybe_reconnect(Reason, State) of
                {noreply, State1} ->
                    {reply, Error, State1};
                {stop, _, State1} ->
                    {stop, Error, State1}
            end
    end.

maybe_reconnect(Error, State = #state{reconnect_backoff = Backoff0, opts = Options}) ->
    catch gen_tcp:close(State#state.socket),
    case proplists:get_bool(auto_reconnect, Options) of
        true ->
            {_, Backoff1} = backoff:fail(Backoff0),
            {noreply, schedule_reconnect(State#state{reconnect_backoff = Backoff1,
                                                     socket = undefined,
                                                     socket_status = disconnected
                                                    })};
        false ->
            {stop, {shutdown, Error}, State}
    end.

connect_int(Address, Port, Options, State) ->
    ConnectionTimeout = proplists:get_value(connection_timeout, Options,
                                           State#state.connect_timeout),
    KeepAlive = proplists:get_value(keepalive, Options, true),
    case gen_tcp:connect(Address, Port, [{packet, 4}, binary, {active, once},
                                         {keepalive, KeepAlive}
                                        ], ConnectionTimeout)
    of
        {ok, Socket} ->
            {ok, State#state{socket = Socket,
                             socket_status = connected
                            }};
        {error, _} = Error ->
            Error
    end.

disconnect_int(State = #state{socket = Socket}) ->
    catch gen_tcp:close(Socket),
    State#state{socket = undefined}.

schedule_reconnect(State = #state{reconnect_backoff = Backoff}) ->
    State#state{reconnect_tref = backoff:fire(Backoff)}.
