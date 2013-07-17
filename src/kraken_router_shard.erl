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
         topics/2, topic_status/1, waitress_pids/1]).

-compile(export_all).
%%%-----------------------------------------------------------------
%%% Definitions
%%%-----------------------------------------------------------------

-define(SERVER, ?MODULE).
-define(TABLE_PREFIX, atom_to_list(?MODULE) ++ "_").

-record(state, {
    % Total count of topics in the system
    pid_to_topics,
    topic_to_pids,
    serial_number
    }).

%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------

start_link() ->
  gen_server:start_link(?MODULE, [], []).

%% @doc Gets and returns the serial from the Router Shard with 
%% Pid = RPid
%%
%% @spec register(RPid :: pid(), WPid :: pid()) -> ok
register(RPid, WPid) -> %%Not using WPid atm
  log4erl:debug("In router_shard:register"),
  gen_server:call(RPid, {register, WPid}).

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

%%%-----------------------------------------------------------------
%%% Callbacks
%%%-----------------------------------------------------------------

init([]) ->
  {ok, #state{
      pid_to_topics = ets:new(list_to_atom(?TABLE_PREFIX ++ "pid_to_topics"), [bag]),
      topic_to_pids = ets:new(list_to_atom(?TABLE_PREFIX ++ "topic_to_pids"), [bag]),
      serial_number = 0
      }}.

%% @doc Incs and returns the current Serial
handle_call({register, WPid}, _From,
            State=#state{serial_number=SerialNumber}) ->
  NextSerial = SerialNumber + 1,
  io:format("shard:handle_call:register.NextSerial: ~p\n", [NextSerial]),
  {reply, NextSerial, State#state{serial_number=NextSerial}};

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
  {reply, TopicStatus, State}.

%% PERF NOTE: We could consider moving most of the publish logic into the caller so
%% that it can be distributed across cores, or even nodes. The problem with that
%% approach is that it may be expensive to return large lists of subscribers
%% to the caller so we leave all of the logic in the router for now. Routers are
%% already sharded so this should leverage multiple cores regardless.
handle_cast({publish, PublisherWPid, Topics, Message},
            State=#state{topic_to_pids=TopicToPids, serial_number=SerialNumber}) ->
  % Creates a list like [{Topic1, Pid1}, {Topic1, Pid2}, {Topic2, Pid1}].
  TopicPidPairs = lists:flatten(
      lists:map(fun(Topic) ->
            ets:lookup(TopicToPids, Topic)
        end, Topics)),

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
  {noreply, State#state{serial_number=SerialNumber + 1}};

% Cast is ok for register because it's ok if we are not notified immediatly
% when a WPid process dies.
handle_cast({register_waitress, WPid}, State) ->
  erlang:monitor(process, WPid),
  {noreply, State}.

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
