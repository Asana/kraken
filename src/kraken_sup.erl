%% @doc Supervisor for the kraken application.

-module(kraken_sup).

-behaviour(supervisor).

%% External exports
-export([start_link/0]).

%% supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
  {ok, ListenIp} = application:get_env(listen_ip),
  {ok, TcpServerPort} = application:get_env(tcp_server_port),
  {ok, MaxTcpClients} = application:get_env(max_tcp_clients),
  {ok, NumRouterShards} = application:get_env(num_router_shards),

  PubsubRouter = {
      kraken_router,
      {kraken_router, start_link, [self(), NumRouterShards]},
      permanent, 5000, worker, [kraken_router]},

  PubsubTcpServer = {
      kraken_tcp_server,
      {kraken_tcp_server, start_link,
       [kraken_memcached, ListenIp, TcpServerPort, MaxTcpClients]},
      permanent, 5000, worker, dynamic},

  {ok, {{one_for_all, 10, 10}, [PubsubRouter, PubsubTcpServer]}}.

%%
%% Tests
%%
-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).
-endif.
