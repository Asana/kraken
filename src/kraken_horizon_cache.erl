%%%-------------------------------------------------------------------
%%% Stores (and keeps up to date) a horizon from the router shards
%%%-------------------------------------------------------------------
-module(kraken_horizon_cache).

-behaviour(gen_server).


%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).
%% API
-export([start_link/1]).

-define(SERVER, ?MODULE).

%% Refresh interval (in microseconds) is 1 second.
-define(REFRESH_INTERVAL, 1000000).

-record(state, {
  router_shards,
  latest_cached_horizon,
  cached_horizon_time %% erlang:timestamp()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(RouterShards) ->
  gen_server:start_link(?MODULE, [RouterShards], []).

get_horizon(CachePid) ->
  gen_server:call(CachePid, {get_horizon}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([RouterShards]) ->
  % Create a zero horizon in case someone asks before we have one
  Horizon = lists:map(fun(_) -> 0 end, RouterShards),
  {ok, #state{router_shards=RouterShards, latest_cached_horizon=Horizon}}.

%% Returns a recent horizon immediately and, if it was relatively old, queues
%% an operation to get a new horizon.
handle_call({get_horizon}, _From,
    State#state{
      cached_horizon_time=CachedHorizonTime,
      latest_cached_horizon=CachedHorizon
    }) ->

  RefreshNow = timer:now_diff(CachedHorizonTime)
  if

  {reply, CachedHorizon, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({refresh_horizon}, State) ->
  {noreply, State}.

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
