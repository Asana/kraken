%% @doc Performance benchmark tool for the kraken application.
%% To run:
%%  kraken run -s kraken_benchmark profile -kraken log_level error
%%
%% TODO: Use updated pubsub performance goals to design a better test and run
%% it on multiple machines to get better performance numbers. Also ensure erlang
%% is started to use multiple CPU cores.

-module(kraken_benchmark).

%%%-----------------------------------------------------------------
%%% Exports
%%%-----------------------------------------------------------------

%% API
-export([profile/0, hard_run/0, run/5]).

%%%-----------------------------------------------------------------
%%% Definitions
%%%-----------------------------------------------------------------

-define(TIMES_PER_CONNECTION, 10).

%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------
hard_run() ->
  run(100, 20, 1000, 1000, 1000).

profile() ->
  run(10, 20, 300, 300, 1).

run(NumClients, N, NumBaseTopics, NumTopics, NumMessages) ->
  Offset = random:uniform(NumBaseTopics*2),
  BaseTopics = generate_topics(NumBaseTopics, Offset),
  DeltaTopics = generate_topics(NumTopics, Offset),
  Messages = generate_messages(NumMessages),
  io:format("Setting up collector...~n"),
  CollectorPid = spawn(fun() -> collect_metrics() end),
  io:format("Preparing clients...~n"),
  ClientFuns = prepare_clients(
      CollectorPid, [], NumClients, BaseTopics, DeltaTopics, Messages),
  io:format("Running general benchmark...~n"),
  Time = benchmark(fun() ->
          pmap(ClientFuns)
      end, N),
  log_section("General results"),
  log_metric("Milliseconds", Time),
  log_metric("Messages/Sec", (NumMessages * NumClients * N * ?TIMES_PER_CONNECTION) / Time * 1000),
  log_metric("Topics/Sec", (NumTopics * NumClients * N * ?TIMES_PER_CONNECTION) / Time * 1000),
  CollectorPid ! stop.

prepare_clients(_CollectorPid, Acc, 0, _, _, _) ->
  Acc;
prepare_clients(CollectorPid, Acc, NumClients, BaseTopics, DeltaTopics, Messages) ->
  Client = prepare_client(CollectorPid, BaseTopics, DeltaTopics, Messages),
  prepare_clients(CollectorPid, [Client|Acc], NumClients-1, BaseTopics, DeltaTopics, Messages).

prepare_client(CollectorPid, BaseTopics, DeltaTopics, Messages) ->
  Host = "localhost",
  {ok, Port} = application:get_env(kraken, tcp_server_port),
  {ok, Socket} = kraken_client:connect(Host, Port),
  kraken_client:subscribe(Socket, BaseTopics),
  fun() ->
      test_server:do_times(?TIMES_PER_CONNECTION, fun() ->
            SubscribeTime = benchmark(fun() ->
                    kraken_client:subscribe(Socket, DeltaTopics)
                end, 1),
            UnsubscribeTime = benchmark(fun() ->
                    kraken_client:unsubscribe(Socket, DeltaTopics)
                end, 1),
            PublishTime = benchmark(fun() ->
                    kraken_client:publish(Socket, Messages)
                end, 1),
            ReceiveTime = benchmark(fun() ->
                    kraken_client:receive_messages(Socket)
                end, 1),
            CollectorPid ! {metrics, SubscribeTime, UnsubscribeTime, PublishTime, ReceiveTime}
        end)
  end.

collect_metrics() -> collect_metrics(0, 0, 0, 0, 0).
collect_metrics(N, SubscribeTime, UnsubscribeTime, PublishTime, ReceiveTime) ->
  receive
    {metrics, Subscribe, Unsubscribe, Publish, Receive} ->
      collect_metrics(
        N+1,
        SubscribeTime + Subscribe,
        UnsubscribeTime + Unsubscribe,
        PublishTime + Publish,
        ReceiveTime + Receive);
    stop ->
      log_section("Operation times (ms)"),
      log_metric("Average Subscribe Time", SubscribeTime / N),
      log_metric("Average Unsubscribe Time", UnsubscribeTime / N),
      log_metric("Average Publish Time", PublishTime / N),
      log_metric("Average Receive Time", ReceiveTime / N);
    Other ->
      log4erl:error("Unknown message ~p", [Other])
  end.

%%%-----------------------------------------------------------------
%%% Utility
%%%-----------------------------------------------------------------

benchmark(Fun, Times) ->
  Start = erlang:now(),
  test_server:do_times(Times, Fun),
  End = erlang:now(),
  Diff = timer:now_diff(End, Start),
  Diff / 1000.

generate_messages(Count) ->
  generate_messages([], Count).

generate_messages(Acc, 0) ->
  Acc;
generate_messages(Acc, Count) ->
  Bin = list_to_binary(integer_to_list(Count)),
  Size = size(Bin),
  Message = {[<<"topic-a">>, <<"topic-b">>], <<"some-reasonably-sized-message-", Bin:Size/binary>>},
  generate_messages([Message|Acc], Count-1).

generate_topics(Count, Offset) ->
  generate_topics([], Count, Offset).

generate_topics(Acc, 0, _Offset) ->
  Acc;
generate_topics(Acc, Count, Offset) ->
  generate_topics([topic_name(Count+Offset)|Acc], Count-1, Offset).

topic_name(Integer) ->
  Bin = list_to_binary(integer_to_list(Integer)),
  Size = size(Bin),
  <<"topic-name-", Bin:Size/binary>>.

pmap(Funs) ->
  Parent = self(),
  Pids = [spawn(fun() -> Parent ! {self(), Fun()} end) || Fun <- Funs],
  [receive {Pid, Val} -> Val end || Pid <- Pids].

%%%-----------------------------------------------------------------
%%% Formatting
%%%-----------------------------------------------------------------

log_section(Msg) -> io:format("~n~p~n=======~n", [Msg]).
log_metric(Name, Value) -> io:format("~p: ~p~n", [Name, Value]).
