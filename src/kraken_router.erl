%% @doc Routes topic invalidation messages to the appropriate clients.
%% Uses multiple kraken_router_shard processes to manage the topic space and get
%% more concurrency between cores.
%%
%% At the moment, the kraken_router itself is a bottleneck since any router
%% operation must first requests the list of Router shards from the kraken_router
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
-export([start_link/2, start_queue_link/1, subscribe/2, unsubscribe/2, publish/3,
         topics/1, topic_status/0, status/0, queue_pids/0]).

-compile(export_all).
%%%-----------------------------------------------------------------
%%% Definitions
%%%-----------------------------------------------------------------

-define(SERVER, ?MODULE).

-record(state, {
    % Array of routers
    routers,
    num_routers=0
    }).

%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------

start_link(Sup, NumRouters) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Sup, NumRouters], []).

%% @doc Creates a new kraken_queue, links it to the calling process, and
%% registers it with the router so that the router will know when it exits
%% and can clean up the appropriate routing information.
%%
%% @spec start_queue_link(Name) -> {ok, QPid :: pid()}
start_queue_link(Name) ->
  % TODO: Consider using another supervisor to monitor these in addition to monitoring
  % them ourself to be more OTP compliant.
  {ok, QPid} = kraken_queue:start_link(Name),
  router_map(fun(Router) ->
        gen_server:cast(Router, {register, QPid})
    end),
  {ok, QPid}.

%% @doc registers QPid so the retroction has a place to start from.
%% The serial_number from each Router Shard gets stored in the waitresses horizon
%%
%% @spec register(QPid :: pid(), Topics :: [string()]) -> ok
register(QPid) ->
  %% router_topics_fold(fun(Router, RouterTopics, _Acc) ->
  %%   % TODO: Consider doing this and unregister in parallel to improve performance
  %%   kraken_router_shard:register(Router, QPid, RouterTopics)
  %% end, undefined, Topics),
  log4erl:debug("IN KRAKEN ROUTER REGISTER !!!!"),
  ok.

%% @doc Subscribes QPid to a list of topics so that they will receive messages
%% whenever another client publishes to the topic. This is a synchronous call.
%% Subscribers will not receive their own messages.
%%
%% @spec subscribe(QPid :: pid(), Topics :: [string()]) -> ok
subscribe(QPid, Topics) ->
  router_topics_fold(fun(Router, RouterTopics, _Acc) ->
        % TODO: Consider doing this and unsubscribe in parallel to improve performance
        kraken_router_shard:subscribe(Router, QPid, RouterTopics)
    end, undefined, Topics),
  ok.

%% @doc Unsubscribes QPid from a list of the topics they were previously
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
%% @spec unsubscribe(QPid :: pid(), Topics :: [string()]) -> ok
unsubscribe(QPid, Topics) ->
  router_topics_fold(fun(Router, RouterTopics, _Acc) ->
        kraken_router_shard:unsubscribe(Router, QPid, RouterTopics)
    end, undefined, Topics),
  ok.

%% @doc Publishes a messages to all subscribers of Topics except the publisher
%% themself. This is a asynchronous call because the publisher should not need
%% to wait for it to complete before it can move on to other processing.
%%
%% @spec publish(PublisherQPid :: pid(), Topics :: [string()], Message :: string()) -> ok
publish(PublisherQPid, Topics, Message) ->
  case application:get_env(router_min_publish_to_topics_to_warn) of
    {ok, MinTopicsToWarn} ->
      TopicCount = length(Topics),
      if
        TopicCount >= MinTopicsToWarn ->
          log4erl:warn(
            "Publish topic fanout of ~p, publisher ~p, message ~p",
            [TopicCount, PublisherQPid, Message]);
        true -> ok
      end;
    undefined -> ok
  end,
  router_topics_fold(fun(Router, RouterTopics, _Acc) ->
        kraken_router_shard:publish(Router, PublisherQPid, RouterTopics, Message)
    end, undefined, Topics),
  ok.

%% @doc Returns the list of queue pids.
%%
%% @spec queue_pids() -> [Pid :: pid()]
queue_pids() ->
  State = state(),
  % Any router should be able to return the complete list of QPids so we will just
  % return the first one.
  Router = array:get(0, State#state.routers),
  kraken_router_shard:queue_pids(Router).

%% @doc Lists the topics that QPid is subscribed to.
%%
%% @spec topics(QPid :: pid()) -> {ok, [Topics :: string()]}
topics(QPid) ->
  router_aggregate(fun(Router) ->
        kraken_router_shard:topics(Router, QPid)
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

%% @doc Prints the status of each queue currently referenced by the router.
%%
%% @spec status() -> ok
status() ->
  Qpids = queue_pids(),
  QpidsLength = length(Qpids),
  io:format("~s:~n~n", [kraken_util:pluralize("Queue", QpidsLength)]),
  lists:foreach(fun(Qpid) ->
        try
          {ok, Status} = kraken_queue:status(Qpid),
          NTopics = length(topics(Qpid)),
          io:format("Queue ~p, subscriptions: ~p, status: ~p~n", [
              Qpid, NTopics, Status])
        catch E:R ->
            % This is expected. We will octionally try to ask a process for its
            % status that has already exited.
            io:format("Error ~p ~p~n", [E, R])
        end
    end, Qpids),
  ok.

%%%-----------------------------------------------------------------
%%% Callbacks
%%%-----------------------------------------------------------------

init([Sup, NumRouters]) ->
  self() ! {start_routers, Sup, NumRouters},
  {ok, #state{routers=array:new(NumRouters)}}.

handle_call(state, _From, State) ->
  {reply, State, State}.

% Cast is ok for register because it's ok if we are not notified immediatly
% when a QPid process dies.
handle_cast({register, QPid}, State=#state{routers=Routers}) ->
  array:map(fun(Router) ->
        gen_server:cast(Router, {register, QPid})
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

start_routers(_Sup, 0, State) ->
  State;
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
  start_routers(
    Sup,
    Count-1,
    State#state{
      num_routers=NumRouters+1,
      routers=array:set(Count-1, NewRouter, Routers)}).

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

topics_by_router(Topics) ->
  State = state(),
  lists:foldl(fun(Topic, Dict) ->
        dict:append(router_for_topic(Topic, State), Topic, Dict)
    end, dict:new(), Topics).

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
  {ok, QPid} = start_queue_link("Foo"),
  ?assertMatch(ok, subscribe(QPid, ["topic1", "topic2"])),
  ?assertMatch(["topic1", "topic2"], lists:sort(topics(QPid))),
  ?assertMatch(ok, subscribe(QPid, ["topic3"])),
  ?assertMatch(ok, subscribe(QPid, ["topic3"])),
  ?assertMatch(["topic1", "topic2", "topic3"], lists:sort(topics(QPid))),
  ?assertMatch(ok, unsubscribe(QPid, ["topic1", "topic3"])),
  ?assertMatch(["topic2"], topics(QPid)).

publish_publish_test() ->
  {ok, QPid} = start_queue_link("Foo"),
  ?assertMatch(ok, subscribe(QPid, ["topic1", "topic2", "topic3"])),
  publish(self(), ["topic1", "topic3"], <<"m1">>),
  publish(self(), ["topic3"], <<"m2">>),
  % "sleep" a tiny bit to allow the messages to propagate back to the queue.
  receive after 1 -> ok end,
  assert_received_messages(
    kraken_queue:receive_messages(QPid),
    [{"topic1", <<"m1">>},
     {"topic3", <<"m1">>},
     {"topic3", <<"m2">>}]),
  % Ensure QPid does not receive its own messages
  publish(QPid, ["topic2"], <<"m3">>),
  publish(self(), ["topic2"], <<"m4">>),
  receive after 1 -> ok end,
  assert_received_messages(
    kraken_queue:receive_messages(QPid),
    [{"topic2", <<"m4">>}]).

topic_status_test() ->
  {ok, QPid1} = start_queue_link("Foo"),
  {ok, QPid2} = start_queue_link("Bar"),
  ?assertMatch(ok, subscribe(QPid1, ["_topic1", "_topic2"])),
  ?assertMatch(ok, subscribe(QPid2, ["_topic1"])),
  TopicStatus = topic_status(),
  ?assertMatch({ok, 2}, dict:find("_topic1", TopicStatus)),
  ?assertMatch({ok, 1}, dict:find("_topic2", TopicStatus)),
  ok.

stop_kraken_test() ->
  ok = kraken:stop().

% TODO: Write tests that ensure routing information is cleaned up when queue
% processes exit.

-endif.
