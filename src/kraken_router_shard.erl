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
-export([start_link/0, subscribe/3, unsubscribe/3, publish/4,
    topics/2, topic_status/1, waitress_pids/1, get_buffered_msgs/3,
    current_timestamp/0]).

%%%-----------------------------------------------------------------
%%% Definitions
%%%-----------------------------------------------------------------

-define(SERVER, ?MODULE).
-define(TABLE_PREFIX, atom_to_list(?MODULE) ++ "_").

-record(state, {
    % Total count of topics in the system
    pid_to_topics,
    topic_to_pids,
    oldest_stored_timestamp,
    eviction_queue,
    per_topic_message_queue
    }).

%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------

start_link() ->
  gen_server:start_link(?MODULE, [], []).

%% @doc Subscribes WPid to a list of topics so that they will receive messages
%% whenever another client publishes to the topic. This is a synchronous call.
%% Subscribers will not receive their own messages.
%%
%% @spec subscribe(RPid :: pid(), WPid :: pid(), Topics :: [string()]) -> ok
subscribe(RPid, WPid, Topics) ->
  gen_server:call(RPid, {subscribe, WPid, Topics}).

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
%% @spec unsubscribe(RPid :: pid(), WPid :: pid(), Topics :: [string()]) -> ok
unsubscribe(RPid, WPid, Topics) ->
  gen_server:call(RPid, {unsubscribe, WPid, Topics}).

%% @doc Publishes a messages to all subscribers of Topics except the publisher
%% themself. This is a asynchronous call because the publisher should not need
%% to wait for it to complete before it can move on to other processing.
%%
%% @spec publish(RPid :: pid(), PublisherWPid :: pid(), Topics :: [string()], Message :: string()) -> ok
publish(RPid, PublisherWPid, Topics, Message) ->
  gen_server:cast(RPid, {publish, PublisherWPid, Topics, Message}),
  ok.

%% @doc Returns the list of waitress pids.
%%
%% @spec waitress_pids(RPid :: pid()) -> [Pid :: pid()]
waitress_pids(RPid) ->
  gen_server:call(RPid, waitress_pids).

%% @doc Lists the topics that WPid is subscribed to.
%%
%% @spec topics(RPid :: pid(), WPid :: pid()) -> {ok, [Topics :: string()]}
topics(RPid, WPid) ->
  gen_server:call(RPid, {topics, WPid}).

%% @doc Lists all topics, with the count of subscribers
%%
%% @spec topic_status(RPid :: pid()) -> [Topics :: {string(), integer()}]
topic_status(RPid) ->
  gen_server:call(RPid, topic_status).

get_buffered_msgs(RPid, ShardHorizon, ShardTopics) ->
  gen_server:call(RPid, {get_buffered_msgs, ShardHorizon, ShardTopics}).

%%%-----------------------------------------------------------------
%%% Callbacks
%%%-----------------------------------------------------------------

init([]) ->
  {ok, MessagesStoredPerRouterShard} = application:get_env(messages_stored_per_router_shard),
  {ok, #state{
      pid_to_topics = ets:new(list_to_atom(?TABLE_PREFIX ++ "pid_to_topics"), [bag]),
      topic_to_pids = ets:new(list_to_atom(?TABLE_PREFIX ++ "topic_to_pids"), [bag]),
      oldest_stored_timestamp = current_timestamp(),
      %% Bounded queue for keeping track of what we stick into the per_topic_message_queue
      %% This is used for bookeeping for evicting things from the per_topic_message_queue
      %% [{Topics[], Timestamp}, ...]
      eviction_queue = bounded_queue:new(MessagesStoredPerRouterShard),
      %% Keeps track of the recent messages for a given topic
      %% {Topic => Queue()}
      per_topic_message_queue = dict:new()
      }}.

handle_call({subscribe, WPid, Topics}, _From,
            State=#state{pid_to_topics=PidToTopics,
                         topic_to_pids=TopicToPids}) ->
  lists:foreach(fun(Topic) ->
        ets:insert(PidToTopics, {WPid, Topic}),
        ets:insert(TopicToPids, {Topic, WPid})
    end, Topics),
  {reply, ok, State};

handle_call({unsubscribe, WPid, Topics}, _From,
            State=#state{pid_to_topics=PidToTopics,
                         topic_to_pids=TopicToPids}) ->
  lists:foreach(fun(Topic) ->
        ets:delete_object(PidToTopics, {WPid, Topic}),
        ets:delete_object(TopicToPids, {Topic, WPid})
    end, Topics),
  {reply, ok, State};

handle_call(waitress_pids, _From, State=#state{pid_to_topics=PidToTopics}) ->
  {reply, ets_keys(PidToTopics), State};

handle_call({topics, WPid}, _From, State=#state{pid_to_topics=PidToTopics}) ->
  {reply, ets_lookup_list(PidToTopics, WPid), State};

handle_call(topic_status, _From, State=#state{topic_to_pids=TopicToPids}) ->
  TopicStatus = ets:foldl(fun({Topic, _WPid}, Acc) ->
          dict:update_counter(Topic, 1, Acc)
      end, dict:new(), TopicToPids),
  {reply, TopicStatus, State};

handle_call({get_buffered_msgs, RequestedTimestamp, ShardTopics}, _From,
            State=#state{oldest_stored_timestamp=OldestStoredTimestamp,
                         per_topic_message_queue=QueueMap}) ->
  %% Invalid Case
  if (OldestStoredTimestamp > RequestedTimestamp) ->
      log4erl:warn(
        "Client requested messages from too far in the past. Requested timestamp: ~p oldest stored timestamp: ~p",
        [RequestedTimestamp, OldestStoredTimestamp]),
      {reply, failure, State};
    true ->
      {reply,
        lists:foldl(fun (Topic, AccIn) ->
              %% log4erl:debug("About to Fetch topic queue for topic: ~p" , [Topic]),
              ContainsTopic = dict:is_key(Topic, QueueMap),
              if ContainsTopic ->
                  SubQueue = dict:fetch(Topic, QueueMap),
                  FilteredMessages = get_messages_above_limit(SubQueue, RequestedTimestamp, []),
                  lists:append(AccIn, FilteredMessages);
                true ->
                  AccIn
      end
  end , [], ShardTopics), State}
end.

get_messages_above_limit(Queue, MinTimestamp, AggList) ->
  {Item, Rest} = queue:out_r(Queue),
  case Item of
    empty ->
      AggList;
    {value, MessagePack = {_Message, _Topics, MessageTimestamp}} ->
      if (MessageTimestamp =< MinTimestamp) ->
          % MinTimestamp is the time of the latest message that we don't need.
          % Thus, here, we've reached the first message in this per-topic
          % queue that we don't need. Return what we have.
          AggList;
        true ->
          % Haven't reached the first message we don't need yet, keep collecting
          get_messages_above_limit(Rest, MinTimestamp, [MessagePack | AggList])
      end
  end.


%% PERF NOTE: We could consider moving most of the publish logic into the caller so
%% that it can be distributed across cores, or even nodes. The problem with that
%% approach is that it may be expensive to return large lists of subscribers
%% to the caller so we leave all of the logic in the router for now. Routers are
%% already sharded so this should leverage multiple cores regardless.

%% Things we do here:
%% take the message and push the messagepack into the EvictionQueue
%% if something was dropped, nextMapg = new, else nextMap = current
%% enqueue messages normally

%% Generates the per_topic_message_queue after the dirty messages are evicted
%% Erases KV Pairs if the value-queue is emptied
get_clean_per_topic_message_queue(MQueueMap, normal) ->
  MQueueMap;
get_clean_per_topic_message_queue(MQueueMap, {dropped, TopicPack}) ->
  %% TODO: assert that the queue isnt already empty
  {DroppedTopics, _Timestamp} = TopicPack,
  lists:foldl(fun (Topic, AccIn) -> 
        Map = dict:update(Topic, fun (Q) -> queue:drop(Q) end, AccIn),
        SubQueue = dict:fetch(Topic, Map),
        IsEmpty = queue:is_empty(SubQueue),
        if IsEmpty ->
            dict:erase(Topic, Map);
          true ->
            Map
        end
    end, MQueueMap, DroppedTopics).

%% Generate the per_topic_message_queue after the new messagepack is added
push_mpack_on_per_topic_message_queue(MQueueMap, MessagePack) ->
  {_Message, Topics, _Timestamp} = MessagePack,
  lists:foldl(fun (Topic, AccIn) -> 
        dict:update(Topic, fun (Q) -> queue:in(MessagePack, Q) end,
                    queue:in(MessagePack, queue:new()), AccIn) end,
              MQueueMap, Topics).

%% Takes in the current MessageQueueMap and returns a new one, updated
%% to have evicted and then pushed items
next_per_topic_message_queue(CurrentMQueueMap, EvictionSignal, MessagePack) ->
  CleanedMQueueMap = get_clean_per_topic_message_queue(CurrentMQueueMap, EvictionSignal),
  push_mpack_on_per_topic_message_queue(CleanedMQueueMap, MessagePack).

handle_cast({publish, PublisherWPid, Topics, Message},
            State=#state{topic_to_pids=TopicToPids, 
                         eviction_queue=CurrentEvictionQueue,
                         oldest_stored_timestamp=CurrentOldestStoredTimestamp,
                         per_topic_message_queue=CurrentMQueueMap}) ->
  CurrentTimestamp = current_timestamp(),

  %% The minimum information to know how to: rm things from the message queue
  %% and to know if the client timestamp is too old to retroact properly
  TopicPack = {Topics, CurrentTimestamp},
  {EvictionSignal, NextEvictionQueue} = bounded_queue:push(TopicPack, CurrentEvictionQueue),
  %% log4erl:debug("EvictionSignal: ~p", [EvictionSignal]),
  %% log4erl:debug("Pushed into Queue: ~p", [TopicPack]),
  %% log4erl:debug("New EvictionQueue: ~p", [NextEvictionQueue]),

  %% Update the oldest stored timestamp if we dropped something
  case EvictionSignal of
    normal ->
      NextOldestStoredTimestamp = CurrentOldestStoredTimestamp;
    {dropped, _} ->
      QueuePeek = bounded_queue:peek(NextEvictionQueue),
      {_Topics, OldestMessageTimestamp} = (if (QueuePeek == empty) -> {[], 0};
        true -> QueuePeek end),
      NextOldestStoredTimestamp = OldestMessageTimestamp,

      % Print the current oldest timestamp occasionally (should be every few seconds)
      if ((NextOldestStoredTimestamp rem 1000) == 0) ->
        log4erl:warn("(~p) Oldest available timestamp: ~p",
          [self(), NextOldestStoredTimestamp]);
        true ->
          % Wtf erlang?
          true
      end
  end,


  %% The data we store in the message queue is constructed from this MessagePack
  %% For each Topic Key we store the message
  MessagePack = {Message, Topics, CurrentTimestamp},
  %% log4erl:debug("MessagePack: ~p", [MessagePack]),
  NextMQueueMap = next_per_topic_message_queue(CurrentMQueueMap, EvictionSignal, MessagePack),

  %% log4erl:debug("NextMQueueMap: ~p", [NextMQueueMap]),

  % Creates a list like [{Topic1, Pid1}, {Topic1, Pid2}, {Topic2, Pid1}].
  TopicPidPairs = lists:flatten(
      lists:map(fun(Topic) ->
            ets:lookup(TopicToPids, Topic) end, Topics)),

  % TODO:
  % All of this work we do to ensure we only enqueue a single message per subscriber
  % may not be necessary, especially now that we shard the routers and
  % therefore cannot merge messages that happen to span shards. If we are going to
  % do this at all, we probably should be doing it in the waitresses themselves so as
  % not to block the routers and we should be doing it when the waitresses request the
  % messages in a single batch across all publish ops. It's probably rare that a
  % message is published to more than a handful of waitresses anyways.

  % Transforms to a dictionary like [{Pid1, [Topic1, Topic2]}, {Pid2, Topic1}].
  PidToTopics = lists:foldl(fun({Topic, WPid}, Dict) ->
          dict:append(WPid, Topic, Dict)
      end, dict:new(), TopicPidPairs),
  % Sends the message to each pid, except for the one that is the same as the
  % publisher itself.
  FanOutCount = lists:foldl(fun({WPid, PidTopics}, Acc) ->
          case WPid of
            PublisherWPid ->
              Acc;
            _ ->
              kraken_waitress:enqueue_message(WPid, PidTopics, Message),
              Acc + 1
          end
      end, 0, dict:to_list(PidToTopics)),
  case application:get_env(router_min_fanout_to_warn) of
    {ok, MinFanoutToWarn} ->
      if
        FanOutCount >= MinFanoutToWarn ->
          log4erl:warn(
            "Publish subscriber fanout of ~p, publisher ~p, topics ~p, message ~p",
            [FanOutCount, PublisherWPid, Topics, Message]);
        true -> ok
      end;
    undefined -> ok
  end,
  {noreply, State#state{
      eviction_queue=NextEvictionQueue,
      oldest_stored_timestamp=NextOldestStoredTimestamp,
      per_topic_message_queue=NextMQueueMap}};

% Cast is ok for register because it's ok if we are not notified immediatly
% when a WPid process dies.
handle_cast({register_waitress, WPid}, State) ->
  erlang:monitor(process, WPid),
  {noreply, State}.

current_timestamp() ->
  {Mega, Secs, _} = os:timestamp(),
  Mega * 1000000 + Secs.

handle_info({'DOWN', _MonitorRef, process, DownPid, _Reason},
            State=#state{pid_to_topics=PidToTopics,
                         topic_to_pids=TopicToPids}) ->
  % Remove the WPid from each of the Topic lists it was previouly in.
  lists:foreach(fun({_Pid, Topic}) ->
        ets:delete_object(TopicToPids, {Topic, DownPid})
    end, ets:lookup(PidToTopics, DownPid)),
  % Then remove the list of Topics for the WPid.
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

%%%-----------------------------------------------------------------
%%% Tests
%%%-----------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).
-include_lib("kraken_test.hrl").

-endif.
