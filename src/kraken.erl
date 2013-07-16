%% @doc kraken top level interface to manage kraken application.

-module(kraken).
-export([start/0, stop/0]).

ensure_started(App) ->
  case application:start(App) of
    ok -> ok;
    {error, {already_started, App}} -> ok
  end.

%% @spec start() -> ok
%% @doc Start the kraken server.
start() ->
  ensure_started(log4erl),
  application:start(kraken).

%% @spec stop() -> ok
%% @doc Stop the kraken server.
stop() ->
  Res = application:stop(kraken),
  application:stop(log4erl),
  Res.
