%% @doc Routes topic invalidation messages to the appropriate clients.
%% Uses multiple kraken_router_shard processes to manage the topic space and get
%% more concurrency between cores.
%%
%% At the moment, the kraken_router itself is a bottleneck since any router
%% operation must first request the list of Router shards from the kraken_router
%% process. In practice, this doesn't seem to affect performance however we could
%% request that information once when we launch the kraken TCP server and pass it
%% through to the various kraken_router functions if necessary.

-module(kraken_router).

-behavior(gen_server).

%%%-----------------------------------------------------------------
%%% Exports
%%%-----------------------------------------------------------------

%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
%% API
-export([start_link/2, start_waitress_link/1, retro_subscribe/3, subscribe/2,
         unsubscribe/2, publish/3, topics/1, topic_status/0, status/0,
         waitress_pids/0, get_horizon/0]).

%% Internal callbacks
-export([get_horizon_reply/1]).

%%%-----------------------------------------------------------------
%%% Definitions
%%%-----------------------------------------------------------------

-define(SERVER, ?MODULE).

-record(state, {
    % Array of routers
    routers,
    num_routers=0,
    % Cache of a recent horizon, so we don't have to wait for the router shards
    % to do slow things to register.
    latest_cached_horizon,
    % The pid of a kraken_horizon_updater
    horizon_updater,
    % Whether we are currently waiting for the kraken_horizon_updater to give
    % us an update (so we shouldn't ask it again).
    waiting_for_updated_horizon=false
    }).

%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------

start_link(Sup, NumRouters) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Sup, NumRouters], []).

%% @doc Creates a new kraken_waitress, links it to the calling process, and
%% registers it with the router so that the router will know when it exits
%% and can clean up the appropriate routing information.
%%
%% @spec start_waitress_link(Name) -> {ok, WPid :: pid()}
start_waitress_link(Name) ->
  % TODO: Consider using another supervisor to monitor these in addition to monitoring
  % them ourself to be more OTP compliant.
  {ok, WPid} = kraken_waitress:start_link(Name),
  router_map(fun(Router) ->
        gen_server:cast(Router, {register_waitress, WPid})
    end),
  {ok, WPid}.

%% @doc Returns the current Horizon
%% Gets the current serial from each router_shard, and returns them as a list.
%% @spec get_horizon() -> [Serials]
get_horizon() ->
  #state{latest_cached_horizon=LatestCachedHorizon} = state(),
  % Return something old, but hopefully not too old
  LatestCachedHorizon.

get_horizon_reply(Horizon) ->
  gen_server:call(?SERVER, {store_horizon, Horizon}).

%% @doc Subscribes WPid to a list of topics in a similar way to subscribe/2
%% below, but do it retroactively so that any messages in between times.
%% @spec retro_subscribe(
%%    WPid :: pid(),
%%    Horizon :: [int],
%%    Topics :: [string()],
%% ) -> (ok | horizon_too_old)
retro_subscribe(WPid, Horizon, RequestedTopics) ->
  classical_subscribe(WPid, RequestedTopics),
  enqueue_buffered_messages(WPid, Horizon, RequestedTopics).


%% @doc Subscribes WPid to a list of topics so that they will receive messages
%% whenever another client publishes to the topic. This is a synchronous call.
%% Subscribers will not receive their own messages.
%% Note that this code is run in the callers process, not the router process
%% @spec subscribe(WPid :: pid(), Topics :: [string()]) -> (ok | horizon_too_old)
subscribe(WPid, RequestedTopics) ->
  %% log4erl:debug("In router:subscribe, : ~p ~p", [WPid, RequestedTopics]),

  % Irrespective of whether there is a horizon to get buffered messages, do the
  % subscribes.
  classical_subscribe(WPid, RequestedTopics),

  HorizonInfo = kraken_waitress:get_horizon(WPid),

  %% Clear the horizon because after the first subscribe its no longer relevant
  kraken_waitress:clear_horizon(WPid),

  case HorizonInfo of
    none ->
      % The waitress didn't have an existing horizon, no need to get buffered
      % messages.
      ok;
    {exists, Horizons} ->
      % There is a horizon, go through the shards and ask them for buffered
      % messages.
      enqueue_buffered_messages(WPid, Horizons, RequestedTopics)
  end.

classical_subscribe(WPid, RequestedTopics) ->
  router_topics_fold(fun(RPid, ShardTopics, _Acc) ->
    kraken_router_shard:subscribe(RPid, WPid, ShardTopics)
  end, [], RequestedTopics).

enqueue_buffered_messages(WPid, Horizon, RequestedTopics) ->
  TopicsByRouter = topics_by_router(RequestedTopics),

  State = state(),
  Shards = array:to_list(State#state.routers),

  % Loop through all the shards and the corresponding horizon simultaneously
  BufferedMessages = lists:foldl(fun({RPid, ShardHorizon}, MsgAcc) ->
    MaybeShardTopics = dict:find(RPid, TopicsByRouter),

    case MaybeShardTopics of
      error ->
        % This shard didn't have any topics, no need to get buffered
        % messages.
        MsgAcc;
      {ok, ShardTopics} ->
        % This is it! We have some topics and a horizon for this shard, we
        % can look up buffered messages!
        Messages = kraken_router_shard:get_buffered_msgs(
          RPid, ShardHorizon, ShardTopics),
        if
          (Messages == failure) ->
            % The horizon was too long ago, remember that.
            log4erl:warn("Horizon too long ago during subscribe. " ++
                "Requested horizon ~p Requested shard serial ~p Shard ~p " ++
                "Cached horizon ~p",
                [Horizon, ShardHorizon, RPid,
                  State#state.latest_cached_horizon]),
            [failure | MsgAcc];
          true ->
            % Concat the messages to ones from other shards
            lists:append(Messages, MsgAcc)
        end
    end
  end, [], lists:zip(Shards, Horizon)),


  Failure = lists:member(failure, BufferedMessages),
  if Failure ->
    % One of the shards couldn't give us messages because the horizon was too
    % long ago.
    registration_too_old;
    true ->
      lists:foreach(fun(MessagePack) ->
        {Message, Topics, _Serial} = MessagePack,
        kraken_waitress:enqueue_message(WPid, Topics, Message)
      end, BufferedMessages),
      ok
  end.

%% @doc Unsubscribes WPid from a list of the topics they were previously
%% subscribed to. If there is a topic in the list that the caller was not
%% previously subscribed to it will be ignored.
%%
%% TODO#Performance:
%% This should probably become an asynchronous call to improve client
%% performance. We could have a dedicated publisher process, or just spawn
%% a new process for each publish operation. Note that if we make this
%% async we must still ensure that unsubscribes and subscribes from the same
%% client happen in order!
%%
%% @spec unsubscribe(WPid :: pid(), Topics :: [string()]) -> ok
unsubscribe(WPid, Topics) ->
  router_topics_fold(fun(Router, RouterTopics, _Acc) ->
        kraken_router_shard:unsubscribe(Router, WPid, RouterTopics)
    end, undefined, Topics),
  ok.

%% @doc Publishes a messages to all subscribers of Topics except the publisher
%% themself. This is a asynchronous call because the publisher should not need
%% to wait for it to complete before it can move on to other processing.
%%
%% @spec publish(PublisherWPid :: pid(), Topics :: [string()], Message :: string()) -> ok
publish(PublisherWPid, Topics, Message) ->
  case application:get_env(router_min_publish_to_topics_to_warn) of
    {ok, MinTopicsToWarn} ->
      TopicCount = length(Topics),
      if
        TopicCount >= MinTopicsToWarn ->
          log4erl:warn(
            "Publish topic fanout of ~p, publisher ~p, message ~p",
            [TopicCount, PublisherWPid, Message]);
        true -> ok
      end;
    undefined -> ok
  end,
  router_topics_fold(fun(Router, RouterTopics, _Acc) ->
        kraken_router_shard:publish(Router, PublisherWPid, RouterTopics, Message)
    end, undefined, Topics),

  % Make sure we are getting a new horizon at some point in the future
  gen_server:call(?SERVER, ensure_updating_horizon),
  ok.

%% @doc Returns the list of waitress pids.
%%
%% @spec waitress_pids() -> [Pid :: pid()]
waitress_pids() ->
  State = state(),
  % Any router should be able to return the complete list of WPids so we will just
  % return the first one.
  Router = array:get(0, State#state.routers),
  kraken_router_shard:waitress_pids(Router).

%% @doc Lists the topics that WPid is subscribed to.
%%
%% @spec topics(WPid :: pid()) -> {ok, [Topics :: string()]}
topics(WPid) ->
  router_aggregate(fun(Router) ->
        kraken_router_shard:topics(Router, WPid)
    end).

%% @doc Lists all topics, with the count of subscribers
%%
%% @spec topic_status() -> [Topics:: {string(), integer()}]
topic_status() ->
  router_fold(fun(Router, Dict) ->
        dict:merge(fun (K, V1, V2) ->
              erlang:error({topic_in_two_routers, K, V1, V2})
          end, Dict, kraken_router_shard:topic_status(Router))
    end, dict:new()).

%% @doc Prints the status of each waitress currently referenced by the router.
%%
%% @spec status() -> ok
status() ->
  Wpids = waitress_pids(),
  WpidsLength = length(Wpids),
  io:format("~s:~n~n", [kraken_util:pluralize("Waitress", WpidsLength)]),
  lists:foreach(fun(Wpid) ->
        try
          {ok, Status} = kraken_waitress:status(Wpid),
          NTopics = length(topics(Wpid)),
          io:format("Waitress ~p, subscriptions: ~p, status: ~p~n", [
              Wpid, NTopics, Status])
        catch E:R ->
            % This is expected. We will octionally try to ask a process for its
            % status that has already exited.
            io:format("Error ~p ~p~n", [E, R])
        end
    end, Wpids),
  ok.

%%%-----------------------------------------------------------------
%%% Callbacks
%%%-----------------------------------------------------------------

init([Sup, NumRouters]) ->
  self() ! {start_routers, Sup, NumRouters},
  {ok, #state{routers=array:new(NumRouters)}}.

handle_call(state, _From, State) ->
  {reply, State, State};

handle_call({store_horizon, Horizon}, _From, State) ->
  {reply, ok, State#state{
    latest_cached_horizon=Horizon,
    waiting_for_updated_horizon = false}};

handle_call(ensure_updating_horizon, _From,
    State=#state{waiting_for_updated_horizon = false,
      horizon_updater = HorizonUpdater}) ->
  % We're not already updating the horizon, start doing it.
  kraken_horizon_updater:get_horizon(HorizonUpdater),
  {reply, ok, State#state{waiting_for_updated_horizon = true}};

handle_call(ensure_updating_horizon, _From,
    State=#state{waiting_for_updated_horizon = true}) ->
  % We're already updating the horizon, it may be out of date by the time it
  % gets back to us, but if we keep doing this repeatedly, we won't get far
  % behind.
  {reply, ok, State}.

% Cast is ok for register because it's ok if we are not notified immediatly
% when a WPid process dies.
handle_cast({register_waitress, WPid}, State=#state{routers=Routers}) ->
  array:map(fun(Router) ->
        gen_server:cast(Router, {register_waitress, WPid})
    end, Routers),
  {noreply, State}.

handle_info({start_routers, Sup, NumRouters}, State) ->
  {noreply, start_routers(Sup, NumRouters, State)};

handle_info({'DOWN', _MonitorRef, process, Pid, Info}, State) ->
  % Don't need to log here since OTP will already log for us.
  {stop, {router_exited, Pid, Info}, State};

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

start_routers(Sup, 0, State=#state{routers=RouterShards}) ->
  % Now there are no more routers to start, start the horizon updater
  {ok, HorizonUpdater} = supervisor:start_child(Sup, {
    {kraken_horizon_updater, self()},
    {kraken_horizon_updater, start_link, [array:to_list(RouterShards)]},
    % Temporary because we share this supervisor, so the next router will make
    % a new one
    temporary,
    brutal_kill,
    worker,
    [kraken_horizon_updater]
  }),
  State#state{horizon_updater=HorizonUpdater};
start_routers(Sup, Count, State=#state{num_routers=NumRouters, routers=Routers}) ->
  {ok, NewRouter} = supervisor:start_child(Sup, {
        {kraken_router_shard, self(), Count},
        {kraken_router_shard, start_link, []},
        % Temporary because we DO not want the supervisor to restart them. The router
        % will spawn new router shards whenever it restarts.
        temporary,
        brutal_kill,
        worker,
        [kraken_router_shard]
        }),
  % Monitor the router so that we can detect when it exits and stop the router,
  % which will cause the supervisor to reboot the entire system.
  monitor(process, NewRouter),

  % Create a zero horizon in case someone asks before we have one
  Horizon = lists:map(fun(_) -> 0 end, array:to_list(Routers)),

  start_routers(
    Sup,
    Count-1,
    State#state{
      num_routers=NumRouters+1,
      routers=array:set(Count-1, NewRouter, Routers),
      latest_cached_horizon=Horizon}).

router_topics_fold(Fun, Acc, Topics) ->
  dict:fold(Fun, Acc, topics_by_router(Topics)).

router_fold(Fun, Acc) ->
  State = state(),
  array:foldl(fun(_Idx, Router, Acc2) ->
        Fun(Router, Acc2)
    end, Acc, State#state.routers).

router_aggregate(Fun) ->
  router_fold(fun(Router, Acc) ->
        R = Fun(Router),
        lists:append(Acc, R)
    end, []).

router_map(Fun) ->
  State = state(),
  lists:map(Fun, array:to_list(State#state.routers)).

%% Returns a mapping from RPid to topic
%% {RPid => Topic}
topics_by_router(Topics) ->
  State = state(),
  lists:foldl(fun(Topic, Dict) ->
        dict:append(router_for_topic(Topic, State), Topic, Dict)
    end, dict:new(), Topics).

%% Returns the RouterShard Pid that is reponsible for this Topic
router_for_topic(Topic, _State=#state{num_routers=NumRouters, routers=Routers}) ->
  array:get(erlang:phash2(Topic, NumRouters), Routers).

state() -> gen_server:call(?SERVER, state).

%%%-----------------------------------------------------------------
%%% Tests
%%%-----------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).
-include_lib("kraken_test.hrl").

start_kraken_test() ->
  % Ideally we would mock the supervisor and just call start_link
  % directly, but this is what we do for now.
  ok = kraken:start().

subscribe_unsubscribe_topics_test() ->
  {ok, WPid} = start_waitress_link("Foo"),
  ?assertMatch(ok, subscribe(WPid, ["topic1", "topic2"])),
  ?assertMatch(["topic1", "topic2"], lists:sort(topics(WPid))),
  ?assertMatch(ok, subscribe(WPid, ["topic3"])),
  ?assertMatch(ok, subscribe(WPid, ["topic3"])),
  ?assertMatch(["topic1", "topic2", "topic3"], lists:sort(topics(WPid))),
  ?assertMatch(ok, unsubscribe(WPid, ["topic1", "topic3"])),
  ?assertMatch(["topic2"], topics(WPid)).

publish_publish_test() ->
  {ok, WPid} = start_waitress_link("Foo"),
  ?assertMatch(ok, subscribe(WPid, ["topic1", "topic2", "topic3"])),
  publish(self(), ["topic1", "topic3"], <<"m1">>),
  publish(self(), ["topic3"], <<"m2">>),
  % "sleep" a tiny bit to allow the messages to propagate back to the waitress.
  receive after 1 -> ok end,
  assert_received_messages(
    kraken_waitress:receive_messages(WPid),
    [{"topic1", <<"m1">>},
     {"topic3", <<"m1">>},
     {"topic3", <<"m2">>}]),
  % Ensure WPid does not receive its own messages
  publish(WPid, ["topic2"], <<"m3">>),
  publish(self(), ["topic2"], <<"m4">>),
  receive after 1 -> ok end,
  assert_received_messages(
    kraken_waitress:receive_messages(WPid),
    [{"topic2", <<"m4">>}]).

topic_status_test() ->
  {ok, WPid1} = start_waitress_link("Foo"),
  {ok, WPid2} = start_waitress_link("Bar"),
  ?assertMatch(ok, subscribe(WPid1, ["_topic1", "_topic2"])),
  ?assertMatch(ok, subscribe(WPid2, ["_topic1"])),
  TopicStatus = topic_status(),
  ?assertMatch({ok, 2}, dict:find("_topic1", TopicStatus)),
  ?assertMatch({ok, 1}, dict:find("_topic2", TopicStatus)),
  ok.

stop_kraken_test() ->
  ok = kraken:stop().

% TODO: Write tests that ensure routing information is cleaned up when waitress
% processes exit.

-endif.
