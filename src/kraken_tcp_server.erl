%% @doc Generic TCP Server that delegates protocol handling to a callback module
%% that is expected to implement the kraken_tcp_connection behavior.

-module(kraken_tcp_server).

%%%-----------------------------------------------------------------
%%% Exports
%%%-----------------------------------------------------------------

%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% API
-export([start_link/4]).

%%%-----------------------------------------------------------------
%%% Definitions
%%%-----------------------------------------------------------------

-record(state, {
    port,           % The port the listen socket accepts connections on
    listen_socket,  % The listen socket
    acceptor_pid,   % Pid of the current acceptor process
    client_count=0, % Count of active clients
    max_clients,    % Maximum number of active clients
    clients,        % ETS table of active clients
    module          % Callback module
    }).

-define(SERVER, ?MODULE).
-define(TABLE_PREFIX, atom_to_list(?MODULE) ++ "_").

% TODO: look into optimizing
% ideal backlog size
% recbuf setting
% nodelay setting
-define(TCP_OPTIONS, [
    binary,
    {packet, line},
    {backlog, 1024},
    {active, false},
    {reuseaddr, true}]).

%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------

start_link(Module, Ip, Port, MaxClients) ->
  gen_server:start_link(
    {local, ?SERVER}, ?MODULE, [Module, Ip, Port, MaxClients], []).

%%%-----------------------------------------------------------------
%%% Callbacks
%%%-----------------------------------------------------------------

init([Module, Ip, Port, MaxClients]) ->
  % Trap exit so we know when acceptor processes die.
  process_flag(trap_exit, true),
  TcpOpts = case Ip of
    any -> ?TCP_OPTIONS;
    Ip when is_tuple(Ip) ->
      [inet, {ip, Ip} | ?TCP_OPTIONS];
    Ip when is_list(Ip) ->
      {ok, IpTuple} = inet_parse:address(Ip),
      [inet, {ip, IpTuple} | ?TCP_OPTIONS]
  end,
  case gen_tcp:listen(Port, TcpOpts) of
    {ok, ListenSocket} ->
      % Create the first accepting process (consider a pool of them?)
      AcceptorPid = kraken_tcp_acceptor:start_link(self(), ListenSocket, Module),
      {ok, #state{
          port=Port,
          listen_socket=ListenSocket,
          acceptor_pid=AcceptorPid,
          max_clients=MaxClients,
          module=Module,
          clients=ets:new(list_to_atom(?TABLE_PREFIX ++ "clients"), [set])}};
    {error, Reason} ->
      {stop, Reason};
    Other ->
      {stop, Other}
  end.

handle_call({accepted, AcceptorPid}, _From,
            State=#state{
        clients=Clients,
        client_count=ClientCount,
        max_clients=MaxClients}) ->
  % Regardless of wether or not we have too many connections we still need
  % to create a new acceptor.
  NewState = recycle_acceptor(AcceptorPid, State),
  % Add the client process to the set of current connections even if there
  % are too many connections because then we don't need to do anything
  % special when we handle its EXIT info message.
  NewState1 = NewState#state{client_count=ClientCount+1},
  ets:insert(Clients, {AcceptorPid, AcceptorPid}),
  if
    ClientCount >= MaxClients ->
      log4erl:debug("Too many connections in kraken tcp server"),
      {reply, too_many_connections, NewState1};
    true ->
      {reply, ok, NewState1}
  end;

handle_call(_Request, _From, State) ->
  {reply, ignored, State}.

handle_cast(stop, State) ->
  {stop, normal, State}.

handle_info({'EXIT', AcceptorPid, Reason},
            State=#state{acceptor_pid=AcceptorPid}) ->
  error_logger:error_report([{'Accept EXIT', Reason}, {'State', State}]),
  {noreply, recycle_acceptor(AcceptorPid, State)};

handle_info({'EXIT', Pid, Reason},
            State=#state{
        clients=Clients,
        client_count=ClientCount}) ->
  case ets:member(Clients, Pid) of
    true ->
      ets:delete(Clients, Pid),
      log4erl:debug("Acceptor exited ~p", [Pid]),
      {noreply, State#state{client_count=ClientCount-1}};
    false ->
      error_logger:error_report([{'Unknown EXIT', {Pid, Reason}}]),
      {noreply, State}
  end;

handle_info(Info, State) ->
  error_logger:error_report([{'INFO', Info}, {'State', State}]),
  {stop, {unhandled_info, Info}, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%%-----------------------------------------------------------------
%%% Private Utility
%%%-----------------------------------------------------------------

recycle_acceptor(_AcceptorPid,
                 State=#state{
        module=Module,
        listen_socket=ListenSocket}) ->
  State#state{
    acceptor_pid=kraken_tcp_acceptor:start_link(self(), ListenSocket, Module)}.

%%%-----------------------------------------------------------------
%%% Tests
%%%-----------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).

-endif.
