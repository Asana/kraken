%% @doc Misc utility functions

-module(kraken_util).

%%%-----------------------------------------------------------------
%%% Exports
%%%-----------------------------------------------------------------

%% API
-export([time_ago_in_words/1, pluralize/2, now_to_seconds/1]).

%%%-----------------------------------------------------------------
%%% Definitions
%%%-----------------------------------------------------------------

-define(SECOND, 1).
-define(MINUTE, 60).
-define(HOUR, 60*60).
-define(DAY, 60*60*24).

%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------

time_ago_in_words(NowTime={_, _, _}) ->
  DeltaSeconds = now_to_seconds(now()) - now_to_seconds(NowTime),
  time_delta_in_words(DeltaSeconds).

pluralize(Name, 1) -> lists:flatten(io_lib:format("1 ~s", [Name]));
pluralize(Name, Count) ->
  lists:flatten(io_lib:format("~p ~ss", [Count, Name])).

now_to_seconds({Mega, Sec, _}) ->
  (Mega * 1000000) + Sec.

%%%-----------------------------------------------------------------
%%% Private
%%%-----------------------------------------------------------------

time_delta_in_words(DeltaSeconds) when DeltaSeconds < ?MINUTE ->
  time_delta_in_words("second", ?SECOND, DeltaSeconds);
time_delta_in_words(DeltaSeconds) when DeltaSeconds < ?HOUR ->
  time_delta_in_words("minute", ?MINUTE, DeltaSeconds);
time_delta_in_words(DeltaSeconds) when DeltaSeconds < ?DAY ->
  time_delta_in_words("hour", ?HOUR, DeltaSeconds);
time_delta_in_words(DeltaSeconds) ->
  time_delta_in_words("day", ?DAY, DeltaSeconds).

time_delta_in_words(UnitName, UnitSeconds, UnitSeconds) ->
  io_lib:format("1 ~s", [UnitName]);
time_delta_in_words(UnitName, UnitSeconds, DeltaSeconds) ->
  Rem = DeltaSeconds rem UnitSeconds,
  Div = DeltaSeconds div UnitSeconds,
  Name = pluralize(UnitName, Div),
  if
    Rem == 0 ->
      io_lib:format("~s", [Name]);
    Rem > 0 ->
      io_lib:format("~s ~s", [Name, time_delta_in_words(Rem)])
  end.

%%%-----------------------------------------------------------------
%%% Tests
%%%-----------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).

time_delta_in_words_test() ->
  ?assertMatch("1 second", lists:flatten(time_delta_in_words(1))),
  ?assertMatch("5 seconds", lists:flatten(time_delta_in_words(5))),
  ?assertMatch("1 minute", lists:flatten(time_delta_in_words(60))),
  ?assertMatch("1 minute 2 seconds", lists:flatten(time_delta_in_words(62))),
  ?assertMatch("55 minutes 10 seconds", lists:flatten(time_delta_in_words((60*55)+10))),
  ?assertMatch("1 hour", lists:flatten(time_delta_in_words((60*60)))),
  ?assertMatch("3 hours", lists:flatten(time_delta_in_words((60*60*3)))),
  ?assertMatch("3 hours 10 minutes 45 seconds", lists:flatten(time_delta_in_words((60*60*3 + 60*10 + 45)))),
  ?assertMatch("5 days 10 seconds", lists:flatten(time_delta_in_words((60*60*24 * 5 + 10)))),
  ok.

-endif.
