%%%-------------------------------------------------------------------
%%% Stores (and keeps up to date) a horizon from the router shards
%%%-------------------------------------------------------------------
-module(kraken_horizon_updater).

-behaviour(gen_server).


%% Callbacks
-export([init/1, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).
%% API
-export([start_link/1, get_horizon/1]).

-define(SERVER, ?MODULE).

%% Refresh interval (in microseconds) is 1 second.
-define(REFRESH_INTERVAL, 1000000).

-record(state, {
  router_shards
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(RouterShards) ->
  gen_server:start_link(?MODULE, [RouterShards], []).

%% Non-blocking way to get the horizon. ReplyPid will be cast back with a
%% {get_horizon_reply, Horizon} message at some point.
%% This causes the kraken_horizon_updater process to do potentially slow work.
get_horizon(UpdaterPid) ->
  gen_server:cast(UpdaterPid, get_horizon).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([RouterShards]) ->
  {ok, #state{router_shards=RouterShards}}.

handle_cast(get_horizon, State=#state{router_shards=RouterShards}) ->
  % Get the horizon by synchronously looping through the shards, doing
  % synchronous get calls. This could theoretically  be done in parallel, but
  % that would make the API ugly and there's no need for it to be fast.
  NewHorizon = lists:map(fun(RPid) ->
    kraken_router_shard:get_serial(RPid)
  end, RouterShards),

  % Ick, circular. There's probably some kind of interface, can't be bothered.
  kraken_router:get_horizon_reply(NewHorizon),

  {noreply, State};

handle_cast(stop, State) ->
  {stop, normal, State}.

handle_info(Info, State) ->
  error_logger:error_report([{'INFO', Info}, {'State', State}]),
  {stop, {unhandled_info, Info}, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
