%% @doc TCP Acceptor process. The actually handling of the data is delegated
%% to a callback module so that this can be shared between different API
%% protocol implementations.

-module(kraken_tcp_acceptor).

%%%-----------------------------------------------------------------
%%% Exports
%%%-----------------------------------------------------------------

%% API
-export([start_link/3, init/3]).

%%%-----------------------------------------------------------------
%%% Definitions
%%%-----------------------------------------------------------------

%% Time idle clients out after 1 hour by default.
-define(IDLE_TIMEOUT_MS, 1000 * 60 * 60).

%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------

start_link(TcpServer, ListenSocket, Module) ->
  proc_lib:spawn_link(?MODULE, init, [TcpServer, ListenSocket, Module]).

init(TcpServer, ListenSocket, Module) ->
  case gen_tcp:accept(ListenSocket) of
    {ok, Socket} ->
      % Notify the TCP server that this acceptor is no longer accepting
      % connections so it can start a new one as needed.
      case gen_server:call(TcpServer, {accepted, self()}) of
        ok ->
          case Module:init(Socket) of
            {ok, State} ->
              relay(Module, Socket, State),
              case catch Module:handle_client_disconnect(Socket, State) of
                {stop, _State} ->
                  exit(normal);
                Other ->
                  error_logger:error_report(
                    [{atom_to_list(Module) ++ ":handle_client_disconnect/3 failed", Other}]),
                  exit(Other)
              end;
            Other ->
              error_logger:error_report([
                  {atom_to_list(Module) ++ ":init/0 failed",
                   Other}]),
              gen_tcp:close(Socket),
              exit({error, init_failed})
          end;
        too_many_connections ->
          Module:handle_server_busy(Socket),
          gen_tcp:close(Socket),
          exit(normal)
      end;
    {error, closed} ->
      log4erl:warn("Connection closed while accepting connection in tcp acceptor"),
      exit(normal);
    {error, timeout} ->
      log4erl:warn("Timeout reached in tcp acceptor"),
      exit(normal);
    Other ->
      error_logger:error_report(
        [{application, kraken},
         "Accept failed error",
         lists:flatten(io_lib:format("~p", [Other]))]),
      exit({error, accept_failed})
  end.

%%%-----------------------------------------------------------------
%%% Private Utility
%%%-----------------------------------------------------------------

relay(Module, Socket, State) ->
  case gen_tcp:recv(Socket, 0, ?IDLE_TIMEOUT_MS) of
    {ok, Data} ->
      case catch Module:handle_data(Data, Socket, State) of
        {ok, NewState} ->
          relay(Module, Socket, NewState);
        {stop, _NewState} ->
          gen_tcp:close(Socket);
        {'EXIT', ok} ->
          gen_tcp:close(Socket);
        Other ->
          gen_tcp:close(Socket),
          error_logger:error_report(
            [{atom_to_list(Module) ++ ":handle_data/3 failed", Other}])
      end;
    {error, closed} ->
      log4erl:warn("Connection closed while receiving data in tcp acceptor"),
      gen_tcp:close(Socket);
    {error, timeout} ->
      Module:handle_client_timeout(Socket, State),
      log4erl:error("Client timed out after ~p milliseconds", [?IDLE_TIMEOUT_MS]),
      gen_tcp:close(Socket);
    Other ->
      error_logger:error_report([{"gen_tcp:recv/2", Other}]),
      gen_tcp:close(Socket)
  end.

%%%-----------------------------------------------------------------
%%% Tests
%%%-----------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).
-endif.
