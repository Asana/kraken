%% @doc Responsible for routing messages through a partition of the topic space
%% for the kraken_router that manages it.

-module(kraken_router_shard).

-behavior(gen_server).

%%%-----------------------------------------------------------------
%%% Exports
%%%-----------------------------------------------------------------

%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
%% API
-export([start_link/1, subscribe/3, unsubscribe/3, publish/4,
         topics/2, topic_status/1, queue_pids/1]).

%%%-----------------------------------------------------------------
%%% Definitions
%%%-----------------------------------------------------------------

-define(SERVER, ?MODULE).
-define(TABLE_PREFIX, atom_to_list(?MODULE) ++ "_").
-define(NODEDOWN_RETRY_DELAY_MS, 1000).

-record(state, {
    % Total count of topics in the system
    topic_count=0,
    proxy_to_node,
    pid_to_topics,
    topic_to_pids
    }).

%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------

start_link(ProxyToNode) ->
  gen_server:start_link(?MODULE, [ProxyToNode], []).

%% @doc Subscribes QPid to a list of topics so that they will receive messages
%% whenever another client publishes to the topic. This is a synchronous call.
%% Subscribers will not receive their own messages.
%%
%% @spec subscribe(RPid :: pid(), QPid :: pid(), Topics :: [string()]) -> ok
subscribe(RPid, QPid, Topics) ->
  gen_server:call(RPid, {subscribe, QPid, Topics}).

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
%% @spec unsubscribe(RPid :: pid(), QPid :: pid(), Topics :: [string()]) -> ok
unsubscribe(RPid, QPid, Topics) ->
  gen_server:call(RPid, {unsubscribe, QPid, Topics}).

%% @doc Publishes a messages to all subscribers of Topics except the publisher
%% themself. This is a asynchronous call because the publisher should not need
%% to wait for it to complete before it can move on to other processing.
%%
%% @spec publish(RPid :: pid(), PublisherQPid :: pid(), Topics :: [string()], Message :: string()) -> ok
publish(RPid, PublisherQPid, Topics, Message) ->
  gen_server:cast(RPid, {publish, PublisherQPid, Topics, Message}),
  ok.

%% @doc Returns the list of queue pids.
%%
%% @spec queue_pids(RPid :: pid()) -> [Pid :: pid()]
queue_pids(RPid) ->
  gen_server:call(RPid, queue_pids).

%% @doc Lists the topics that QPid is subscribed to.
%%
%% @spec topics(RPid :: pid(), QPid :: pid()) -> {ok, [Topics :: string()]}
topics(RPid, QPid) ->
  gen_server:call(RPid, {topics, QPid}).

%% @doc Lists all topics, with the count of subscribers
%%
%% @spec topic_status(RPid :: pid()) -> [Topics :: {string(), integer()}]
topic_status(RPid) ->
  gen_server:call(RPid, topic_status).

%%%-----------------------------------------------------------------
%%% Callbacks
%%%-----------------------------------------------------------------

init([ProxyToNode]) ->
  case ProxyToNode of
    undefined -> ok;
    _ ->
      log4erl:info(
        "(~p) Registering proxy with remote router ~p", [self(), ProxyToNode]),
      ok = rpc:call(ProxyToNode, kraken_router, register, [self()], 5000)
  end,
  {ok, #state{
      proxy_to_node=ProxyToNode,
      pid_to_topics=
          ets:new(list_to_atom(?TABLE_PREFIX ++ "pid_to_topics"), [bag]),
      topic_to_pids=
          ets:new(list_to_atom(?TABLE_PREFIX ++ "topic_to_pids"), [bag])}}.

handle_call({subscribe, QPid, Topics}, _From,
    State=#state{pid_to_topics=PidToTopics,
                 topic_to_pids=TopicToPids}) ->
  log4erl:debug("(~p) Subscribe ~p ~p", [self(), QPid, Topics]),
  % Must be done before the local subscribe since the proxy only wants to subscribe
  % to topics that it hasn't already subscribed to.
  ok = with_proxy_to_node(State, fun(Node) ->
    NewTopics = lists:filter(fun(Topic) ->
      not ets:member(State#state.topic_to_pids, Topic)
    end, Topics),
    rpc:call(Node, kraken_router, subscribe, [self(), NewTopics], 5000)
  end),
  lists:foreach(fun(Topic) ->
    ets:insert(PidToTopics, {QPid, Topic}),
    ets:insert(TopicToPids, {Topic, QPid})
  end, Topics),
  {reply, ok, State};

handle_call({unsubscribe, QPid, Topics}, _From,
    State=#state{pid_to_topics=PidToTopics,
                 topic_to_pids=TopicToPids}) ->
  log4erl:debug("(~p) Unsubscribe ~p ~p", [self(), QPid, Topics]),
  lists:foreach(fun(Topic) ->
    ets:delete_object(PidToTopics, {QPid, Topic}),
    ets:delete_object(TopicToPids, {Topic, QPid})
  end, Topics),
  % Must be done after the local unsubscribe since the proxy must only unsubscribe
  % from topics that the local subscribe no longer cares about.
  ok = with_proxy_to_node(State, fun(Node) ->
    % Unsubscribe from topics with no subscribers on the remote router.
    EmptyTopics = lists:filter(fun(Topic) ->
      not ets:member(State#state.topic_to_pids, Topic)
    end, Topics),
    rpc:call(Node, kraken_router, unsubscribe, [self(), EmptyTopics], 5000)
  end),
  {reply, ok, State};

handle_call(queue_pids, _From, State=#state{pid_to_topics=PidToTopics}) ->
  {reply, ets_keys(PidToTopics), State};

handle_call({topics, QPid}, _From, State=#state{pid_to_topics=PidToTopics}) ->
  {reply, ets_lookup_list(PidToTopics, QPid), State};

handle_call(topic_status, _From, State=#state{topic_to_pids=TopicToPids}) ->
  TopicStatus = ets:foldl(fun({Topic, _QPid}, Acc) ->
    dict:update_counter(Topic, 1, Acc)
  end, dict:new(), TopicToPids),
  {reply, TopicStatus, State}.

%% PERF NOTE: We could consider moving most of the publish logic into the caller so
%% that it can be distributed across cores, or even nodes. The problem with that
%% approach is that it may be expensive to return large lists of subscribers
%% to the caller so we leave all of the logic in the router for now. Routers are
%% already sharded so this should leverage multiple cores regardless.
handle_cast({publish, PublisherQPid, Topics, Message}, State) ->
  log4erl:debug("(~p) Publish ~p ~p ~p", [self(), PublisherQPid, Topics, Message]),
  publish_local(PublisherQPid, Topics, Message, State),
  ok = with_proxy_to_node(State, fun(Node) ->
    rpc:call(Node, kraken_router, publish, [self(), Topics, Message], 5000)
  end),
  {noreply, State};

% If this router is acting as a proxy, then it will subscribe itself to the
% upstream router as if it were a queue itself. So we must implement the
% queue interface to receive messages and publish them to our local queues.
handle_cast({enqueue_message, Topics, Message}, State) ->
  publish_local(undefined, Topics, Message, State),
  {noreply, State};

% Cast is ok for register because it's ok if we are not notified immediatly
% when a QPid process dies.
handle_cast({register, QPid}, State) ->
  erlang:monitor(process, QPid),
  {noreply, State}.

handle_info({'DOWN', _MonitorRef, process, DownPid, _Reason},
    State=#state{pid_to_topics=PidToTopics,
                 topic_to_pids=TopicToPids}) ->
  % Remove the QPid from each of the Topic lists it was previouly in.
  lists:foreach(fun({_Pid, Topic}) ->
    ets:delete_object(TopicToPids, {Topic, DownPid})
  end, ets:lookup(PidToTopics, DownPid)),
  % Then remove the list of Topics for the QPid.
  ets:delete(PidToTopics, DownPid),
  {noreply, State};

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

%% @doc Returns the list of values stored in Key. Useful becuase ets returns
%% them all as Key Value pairs.
ets_lookup_list(Table, Key) ->
  lists:map(fun({_Key, Value}) ->
    Value
  end, ets:lookup(Table, Key)).

ets_keys(Tab) ->
  First = ets:first(Tab),
  ets_keys(Tab, First, []).

ets_keys(_Tab, '$end_of_table', Acc) ->
  Acc;
ets_keys(Tab, Key, Acc) ->
  Next = ets:next(Tab, Key),
  ets_keys(Tab, Next, [Key|Acc]).

with_proxy_to_node(_State=#state{proxy_to_node=undefined}, _Fun) ->
  ok;
with_proxy_to_node(_State=#state{proxy_to_node=ProxyToNode}, Fun) ->
  Fun(ProxyToNode).

%% @doc Distributes Message to the appropriate subscribers on this node only.
publish_local(PublisherQPid, Topics, Message,
              _State=#state{topic_to_pids=TopicToPids}) ->
  % Creates a list like [{Topic1, Pid1}, {Topic1, Pid2}, {Topic2, Pid1}].
  TopicPidPairs = lists:flatten(
      lists:map(fun(Topic) ->
        ets:lookup(TopicToPids, Topic)
      end, Topics)),

  % TODO:
  % All of this work we do to ensure we only enqueue a single message per subscriber
  % may not be necessary, especially now that we shard the routers and
  % therefore cannot merge messages that happen to span shards. If we are going to
  % do this at all, we probably should be doing it in the queues themselves so as
  % not to block the routers and we should be doing it when the queues request the
  % messages in a single batch across all publish ops. It's probably rare that a
  % message is published to more than a handful of queues anyways.

  % Transforms to a dictionary like [{Pid1, [Topic1, Topic2]}, {Pid2, Topic1}].
  PidToTopics = lists:foldl(fun({Topic, QPid}, Dict) ->
    dict:append(QPid, Topic, Dict)
  end, dict:new(), TopicPidPairs),
  % Sends the message to each pid, except for the one that is the same as the
  % publisher itself.
  FanOutCount = lists:foldl(fun({QPid, PidTopics}, Acc) ->
    case QPid of
      PublisherQPid ->
        Acc;
      _ ->
        kraken_queue:enqueue_message(QPid, PidTopics, Message),
        Acc + 1
    end
  end, 0, dict:to_list(PidToTopics)),
  case application:get_env(router_min_fanout_to_warn) of
    {ok, MinFanoutToWarn} ->
      if
        FanOutCount >= MinFanoutToWarn ->
          log4erl:warn(
              "Publish fanout of ~p, publisher ~p, topics ~p, message ~p",
              [FanOutCount, PublisherQPid, Topics, Message]);
        true -> ok
      end;
    undefined -> ok
  end.


