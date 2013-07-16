%% @doc Control module for kraken CLI script.

-module(kraken_ctl).

%% External exports
-export([stop/1, status/1, change_log_level/1, dump_waitresses/1, dump_waitress_topics/1,
         dump_topics/1]).

%% ===================================================================
%% API functions
%% ===================================================================

stop([Node]) ->
  io:format("Stop: ~p~n", [Node]),
  case application_is_running(Node, kraken) of
    nodedown ->
      io:format("Node ~p is down~n", [Node]);
    true ->
      % Stop the application first to ensure we get a synchronous stop.
      rpc:call(Node, kraken, stop, []),
      rpc:call(Node, init, stop, []),
      io:format("Stopped.~n");
    false ->
      rpc:call(Node, init, stop, []),
      io:format("Application is not running on node ~p~n", [Node])
  end,
  init:stop().

status([Node]) ->
  io:format("Status: ~p~n", [Node]),
  case application_is_running(Node, kraken) of
    nodedown ->
      io:format("Not running: Node ~p is down~n", [Node]),
      init:stop(1);
    false ->
      io:format("Not running: Application is not running but Node ~p is up~n", [Node]),
      init:stop(2);
    true ->
      io:format("Running~n"),
      init:stop()
  end.

change_log_level([Node, DebugLevel]) ->
  case rpc:call(Node, log4erl, change_log_level, [DebugLevel]) of
    ok ->
      io:format("Changed log level for ~s to ~s~n", [Node, DebugLevel]),
      init:stop(0);
    Error ->
      io:format("Error: ~p~n", [Error]),
      init:stop(1)
  end.

dump_waitresses([Node]) ->
  rpc:call(Node, kraken_router, status, []),
  init:stop(0).

dump_waitress_topics([Node, WpidAtom]) ->
  WpidString = atom_to_list(WpidAtom),
  Wpids = rpc:call(Node, kraken_router, dump_waitress_topics, []),
  [Wpid] = lists:filter(fun(X) ->
          string:equal(pid_to_list(X), WpidString)
      end, Wpids),
  Topics = rpc:call(Node, kraken_router, topics, [Wpid]),
  io:format("Waitress ~p, topics: ~n", [Wpid]),
  lists:foreach(fun(X) -> io:format("~s~n", [X]) end, Topics),
  init:stop().

%% @doc Dumps topic names with subscriber counts sorted by subscriber count
dump_topics([Node]) ->
  TopicStatus = rpc:call(Node, kraken_router, topic_status, []),
  SortedTopicStatus = lists:sort(fun({_TopicA, CountA}, {_TopicB, CountB}) ->
          CountA >= CountB
      end, dict:to_list(TopicStatus)),
  lists:foreach(fun({Topic, Count}) ->
        io:format("~s\t~p~n", [Topic, Count])
    end, SortedTopicStatus),
  init:stop(0).

application_is_running(Node, Name) ->
  case rpc:call(Node, application, which_applications, []) of
    {badrpc, nodedown} -> nodedown;
    Applications -> application_in_list(Applications, Name)
  end.

application_in_list([], _Name) ->
  false;
application_in_list([AppData|Rest], Name) ->
  case element(1, AppData) of
    Name -> true;
    _Other -> application_in_list(Rest, Name)
  end.
