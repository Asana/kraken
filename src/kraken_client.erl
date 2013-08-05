%% @doc TCP client for the memcached based kraken protocol for testing and
%% benchmarking.

-module(kraken_client).

%%%-----------------------------------------------------------------
%%% Exports
%%%-----------------------------------------------------------------

%% API
-export([connect/2, new_client/0, register/1, subscribe/2, unsubscribe/2, disconnect/1, publish/2,
         assert_receive/2, receive_messages/1, quit/1]).

%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------

connect(Host, Port) ->
  gen_tcp:connect(Host, Port, [binary,
                               {packet, line},
                               {active, false},
                               {reuseaddr, true}]).

disconnect(Socket) ->
  gen_tcp:close(Socket).

register(Socket) ->
  set_command(Socket, <<"register">>, <<>>).

subscribe(Socket, Topics) ->
  topic_command(Socket, <<"subscribe">>, Topics).

unsubscribe(Socket, Topics) ->
  topic_command(Socket, <<"unsubscribe">>, Topics).

publish(Socket, MessageEntries) ->
  {DataBytes, DataBlock} = kraken_memcached:serialize_message_entries(MessageEntries),
  set_command(Socket, <<"publish">>, DataBytes, DataBlock).

receive_messages(Socket) ->
  Data = get_command(Socket, <<"messages">>),
  kraken_memcached:parse_publish_entries(Data).

quit(Socket) ->
  gen_tcp:send(Socket, <<"quit\r\n">>).

%%%-----------------------------------------------------------------
%%% Utility Methods
%%%-----------------------------------------------------------------

topic_command(Socket, Command, Topics) ->
  set_command(Socket, Command, kraken_memcached:serialize_topics(Topics)).

set_command(Socket, Command, Data) ->
  set_command(Socket, Command, size(Data), Data).

set_command(Socket, Command, Bytes, Data) ->
  gen_tcp:send(Socket,
               [<<"set ">>, Command, <<" 0 0 ">>,
                list_to_binary(integer_to_list(Bytes)),
                <<"\r\n">>, Data, <<"\r\n">>]),
  case gen_tcp:recv(Socket, 0) of
    {ok, <<"STORED\r\n">>} ->
      ok;
    Error ->
      {error, Error}
  end.

get_command(Socket, Command) ->
  gen_tcp:send(Socket,
               [<<"get ">>, Command, <<"\r\n">>]),
  {ok, Data} = receive_value(Socket, Command),
  Data.

receive_value(Socket, Key) ->
  KeyLen = size(Key),
  case gen_tcp:recv(Socket, 0) of
    {ok, <<"END\r\n">>} ->
      {ok, <<>>};
    {ok, <<"VALUE ", Key:KeyLen/binary, " 0 ", Rest/binary>>} ->
      Bytes = binary_part(Rest, 0, size(Rest) - 2),
      % +5 bytes below to include the "END\r\n" and +2 bytes to include
      % the \r\n just before that.
      receive_data_block(Socket, [], list_to_integer(binary_to_list(Bytes))+5+2);
    {ok, Other} ->
      {error, {unexpected_data, Other}};
    Error ->
      {error, Error}
  end.

receive_data_block(Socket, Acc, BytesRemaining) ->
  case gen_tcp:recv(Socket, 0) of
    {ok, Data} ->
      Bytes = size(Data),
      if
        Bytes =:= BytesRemaining ->
          % Note we do not care about the last line because it is "END\r\n"
          {ok, list_to_binary(lists:reverse(Acc))};
        Bytes > BytesRemaining ->
          {error, bad_data_block};
        true ->
          receive_data_block(Socket, [Data|Acc], BytesRemaining-Bytes)
      end;
    Error ->
      {error, Error}
  end.

%%%-----------------------------------------------------------------
%%% Tests
%%%-----------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).
-include_lib("kraken_test.hrl").

%% The time to sleep between asynchronous checks.
-define(SLEEP_TIME, 50).
%% The maximum number of milliseconds to wait for an asynchronous operation.
-define(MAX_WAIT_TIME, 2000).

%% Messages are received asynchronously so this helper is used to continue
%% trying a receive until a timeout is reached.
assert_receive(ExpectedResults, Socket) ->
  assert_receive([], ExpectedResults, Socket, ?MAX_WAIT_TIME).

assert_receive(Results, ExpectedResults, _Socket, 0) ->
  assert_received_messages(Results, ExpectedResults);

assert_receive(Results, ExpectedResults, Socket, MaxTimeMillis) ->
  NewResults=lists:append(Results, kraken_client:receive_messages(Socket)),
  if
    length(NewResults) < length(ExpectedResults) ->
      timer:sleep(?SLEEP_TIME),
      assert_receive(
        NewResults,
        ExpectedResults,
        Socket,
        erlang:max(MaxTimeMillis - ?SLEEP_TIME, 0));
    true ->
      assert_receive(NewResults, ExpectedResults, Socket, 0)
  end.

%% Asserts that the set of active processes in the erlang vm is eventually
%% equivalent to the list of proccess passed in before ?MAX_WAIT_TIME
%% milliseconds have passed.
assert_no_leaked_processes(Processes) ->
  wait_for_processes_to_exit(Processes, ?MAX_WAIT_TIME).

wait_for_processes_to_exit(Processes, 0) ->
  CurrentProcessCount = length(processes()),
  TargetProcessCount = length(Processes),
  if
    CurrentProcessCount > TargetProcessCount ->
      lists:foreach(fun(Pid) ->
            ?debugFmt("Leaked process ~n~p~n", [erlang:process_info(Pid)])
        end, lists:subtract(processes(), Processes)),
      ?assertEqual(TargetProcessCount, CurrentProcessCount);
    true ->
      ok
  end;
wait_for_processes_to_exit(Processes, MaxTimeMillis) ->
  CurrentProcessCount = length(processes()),
  TargetProcessCount = length(Processes),
  if
    CurrentProcessCount > TargetProcessCount ->
      timer:sleep(?SLEEP_TIME),
      wait_for_processes_to_exit(
        TargetProcessCount,
        erlang:max(MaxTimeMillis - ?SLEEP_TIME, 0));
    true ->
      ok
  end.

new_client() ->
  {ok, Port} = application:get_env(kraken, tcp_server_port),
  {ok, Socket} = kraken_client:connect("localhost", Port),
  Socket.

kraken_client_test_() ->
  {setup,
   fun() ->
        ok = kraken:start(),
        % We make the following call to ensure the erlang inet processes are
        % already started so that we do not consider them in our process leak
        % checks later. This call will fail but we can ignore the error.
        _ = inet_res:gethostbyname("localhost"),
        processes()
    end,
   fun(Processes) ->
        ok = kraken:stop(),
        assert_no_leaked_processes(Processes)
    end,
   [{foreach,
     fun new_client/0,
     [{with, [fun test_disconnect/1]},
      {with, [fun test_quit/1]},
      {with, [fun test_topic_commands/1]},
      {with, [fun test_publish_receive_commands/1]},
      {with, [fun test_retro_basic/1]},
      {with, [fun test_register_cutoff/1]}
     ]}]}.

test_quit(Socket) ->
  ok = kraken_client:quit(Socket).

test_disconnect(Socket) ->
  ok = kraken_client:disconnect(Socket).

test_topic_commands(Socket) ->
  ok = kraken_client:subscribe(Socket, [<<"topic1">>, <<"topic2">>]),
  ok = kraken_client:unsubscribe(Socket, [<<"topic1">>]).

test_publish_receive_commands(Socket1) ->
  Socket2 = new_client(),
  ?assertMatch([], kraken_client:receive_messages(Socket2)),
  ok = kraken_client:subscribe(Socket2, [<<"topic1">>, <<"topic2">>]),
  ok = kraken_client:publish(Socket1,
                             [{[<<"topic1">>, <<"topic2">>], <<"m1">>},
                              {[<<"topic3">>], <<"m2">>}]),
  assert_receive([{<<"topic1">>,<<"m1">>}, {<<"topic2">>,<<"m1">>}], Socket2).

test_retro_basic(PublisherSock) ->
  Socket = kraken_client:new_client(),
  %% Buffer some messages so we dont just fail automatically
  ok = kraken_client:publish(PublisherSock, 
                             [{[<<"other">>], <<"dummy1">>},
                              {[<<"other">>], <<"dummy2">>},
                              {[<<"other">>], <<"dummy3">>},
                              {[<<"other">>], <<"dummy4">>},
                              {[<<"other">>], <<"dummy5">>},
                              {[<<"other">>], <<"dummy6">>},
                              {[<<"other">>], <<"dummy7">>}]),
  kraken_client:register(Socket),
  ok = kraken_client:publish(PublisherSock, 
                             [{[<<"t1">>, <<"t2">>, <<"other">>], <<"msg1">>},
                              {[<<"t1">>, <<"t2">>], <<"msg2">>}]),
  ?assertMatch([], kraken_client:receive_messages(PublisherSock)),
  kraken_client:subscribe(Socket, [<<"t1">>, <<"t2">>, <<"null">>]),
  kraken_client:assert_receive([{<<"t1">>,<<"msg1">>}, {<<"t2">>,<<"msg2">>},
                                {<<"t1">>,<<"msg2">>}, {<<"t2">>,<<"msg1">>}], Socket),
  ok.

test_register_cutoff(PublisherSock) ->
  Socket = kraken_client:new_client(),
  ok = kraken_client:publish(PublisherSock,
                             [{[<<"t1">>, <<"t2">>, <<"other">>], <<"old1">>},
                              {[<<"t1">>, <<"t2">>], <<"old2">>}]),
  kraken_client:register(Socket),
  ok = kraken_client:publish(PublisherSock,
                             [{[<<"t1">>, <<"t2">>, <<"other">>], <<"msg1">>},
                              {[<<"t1">>, <<"t2">>], <<"msg2">>}]),
  kraken_client:subscribe(Socket, [<<"t1">>, <<"t2">>, <<"null">>]),
  ?assertMatch([], kraken_client:receive_messages(PublisherSock)),
  Msgs = kraken_client:receive_messages(Socket),
  assert_not_received_message(Msgs, <<"old1">>),
  assert_not_received_message(Msgs, <<"old2">>),
  ok.

-endif.
