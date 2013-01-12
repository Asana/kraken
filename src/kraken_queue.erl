%% @doc Generic queue

-module(kraken_queue).

-behavior(gen_server).

%%%-----------------------------------------------------------------
%%% Exports
%%%-----------------------------------------------------------------

%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
        code_change/3]).
%% API
-export([start_link/1, enqueue_message/3, receive_messages/1, stop/1, status/1]).

%%%-----------------------------------------------------------------
%%% Definitions
%%%-----------------------------------------------------------------

-record(state, {
    % Name
    name,
    % Count of received messages
    received_message_count=0,
    % A simple list is all that is needed for now since we always send the
    % entire queue to the client on receive_messages. If that were not the
    % case then we may want to use a stdlib queue.
    queue=[],
    % The time when the queue was started
    start_time,
    % The time of the last request to receive messages or the start time if
    % no requests have been made.
    last_receive_messages_time
    }).

%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------

start_link(Name) ->
  gen_server:start_link(?MODULE, [Name], []).

status(Pid) ->
  gen_server:call(Pid, status).

receive_messages(Pid) ->
  gen_server:call(Pid, receive_messages).

enqueue_message(Pid, Topics, Message) ->
  gen_server:cast(Pid, {enqueue_message, Topics, Message}).

stop(Pid) ->
  gen_server:cast(Pid, stop).

%%%-----------------------------------------------------------------
%%% Callbacks
%%%-----------------------------------------------------------------

init([Name]) ->
  StartTime = now(),
  {ok, #state{
      name=Name,
      start_time=StartTime,
      last_receive_messages_time=StartTime}}.

handle_call(status, _From,
    State=#state{
        name=Name,
        queue=Queue,
        received_message_count=ReceivedMessageCount,
        start_time=StartTime,
        last_receive_messages_time=LastReceiveMessagesTime}) ->

  {reply, {ok, [
    {name, Name},
    {length, length(Queue)},
    {received_message_count, ReceivedMessageCount},
    {time_since_start,
        lists:flatten(kraken_util:time_ago_in_words(StartTime))},
    {time_since_last_receive_messages,
        lists:flatten(kraken_util:time_ago_in_words(LastReceiveMessagesTime))}
  ]}, State};

handle_call(receive_messages, _From, State=#state{queue=Queue}) ->
  {reply, lists:reverse(Queue),
      State#state{queue=[], last_receive_messages_time=now()}}.

handle_cast({enqueue_message, Topics, Message},
            State=#state{
              queue=Queue,
              received_message_count=ReceivedMessageCount}) ->
  {noreply, State#state{
      queue=[{Topics, Message}|Queue],
      received_message_count=ReceivedMessageCount+1}};

handle_cast(stop, State) ->
  {stop, normal, State}.

handle_info(Info, State) ->
  error_logger:error_report([{'INFO', Info}, {'State', State}]),
  {stop, {unhandled_info, Info}, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%-----------------------------------------------------------------
%%% Tests
%%%-----------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

start_link_stop_test() ->
  {ok, Pid} = start_link("Foo"),
  stop(Pid).

enqueue_message_and_receive_test() ->
  {ok, Pid} = start_link("Bar"),
  enqueue_message(Pid, ["foo", "bar"], <<"hello world 1">>),
  enqueue_message(Pid, ["baz"], <<"hello world 2">>),
  ?assertMatch(
      [{["foo", "bar"], <<"hello world 1">>},
       {["baz"], <<"hello world 2">>}],
      receive_messages(Pid)),
  ?assertMatch(
      [],
      receive_messages(Pid)),
  stop(Pid).

-endif.

