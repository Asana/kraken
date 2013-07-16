%% @doc Callbacks for the kraken application.

-module(kraken_app).
-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for kraken.
start(_Type, _StartArgs) ->
  % Setup the log level.
  LogLevel = case application:get_env(log_level) of
    {ok, Level} -> Level;
    undefined -> info
  end,
  % Setup logging to file if configured.
  case application:get_env(log_file) of
    undefined ->
      % Setup console appender but do not setup log4erl report handler
      % because the normal SASL report handler does better printing of
      % reports still because it indents things more sensibly.
      log4erl:add_console_appender(cmd_logs, {LogLevel, "[%L] %T %l%n%n"});
    {ok, LogFile} ->
      % Stop logging to console.
      error_logger:tty(false),
      % Ensure we log error_logger messages to log4erl as well.
      error_logger:add_report_handler(kraken_error_logger_h),
      LogFileExt = case filename:extension(LogFile) of
        [] ->
          % log4erl requires an extension.
          "log";
        Ext ->
          % Strip period.
          lists:nthtail(1, Ext)
      end,
      log4erl:add_file_appender(
        file_log,
        {filename:dirname(LogFile),
         filename:basename(filename:rootname(LogFile)),
         {no_max_size, 0},
         0, % no rotate
         LogFileExt,
         LogLevel,
         "[%L] %j::%T %l%n"})
  end,
  % Update the pid file if configured.
  PidFile = case application:get_env(pid_file) of
    undefined ->
      ok;
    {ok, File} ->
      file:write_file(File, os:getpid()),
      File
  end,
  {ok, Pid} = kraken_sup:start_link(),
  log4erl:warn("Kraken App Started"),
  {ok, Pid, PidFile}.

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for kraken.
stop(PidFile) ->
  % Clear pid_file if one was used.
  case PidFile of
    undefined -> ok;
    _ -> file:delete(PidFile)
  end,
  ok.


%%
%% Tests
%%
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
