%% Custom error_logger event handler because the log4erl one does not log
%% reports and we also want those.

-module(kraken_error_logger_h).

-behaviour(gen_event).

%%%-----------------------------------------------------------------
%%% Exports
%%%-----------------------------------------------------------------

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2, handle_info/2,
         terminate/2, code_change/3]).

%%%-----------------------------------------------------------------
%%% Callbacks
%%%-----------------------------------------------------------------

init([])->
  {ok, []}.

handle_event({error, _GLeader, {_Pid, Msg, Data}}, State) ->
  R = log4erl:error(Msg, Data),
  {R, State};
handle_event({info_msg, _GLeader, {_Pid, Msg, Data}}, State) ->
  R = log4erl:info(Msg, Data),
  {R, State};
handle_event({warning_msg, _GLeader, {_Pid, Msg, Data}}, State) ->
  R = log4erl:warn(Msg, Data),
  {R, State};
handle_event({error_report, _GLeader, {_Pid, crash_report, Rep}}, State) ->
  R = log4erl:error(format_and_prefix_report("CRASH REPORT", Rep)),
  {R, State};
handle_event({error_report, _GLeader, {_Pid, supervisor_report, Rep}}, State) ->
  R = log4erl:error(format_and_prefix_report("SUPERVISOR REPORT", Rep)),
  {R, State};
handle_event({error_report, _GLeader, {_Pid, _Type, Rep}}, State) ->
  R = log4erl:error(format_and_prefix_report("ERROR REPORT", Rep)),
  {R, State};
handle_event({info_report, _GLeader, {_Pid, progress, Rep}}, State) ->
  R = log4erl:info(format_and_prefix_report("PROGRESS REPORT", Rep)),
  {R, State};
handle_event({info_report, _GLeader, {_Pid, _Type, Rep}}, State) ->
  R = log4erl:info(format_and_prefix_report("INFO REPORT", Rep)),
  {R, State};
handle_event({warning_report, _GLeader, {_Pid, _, Rep}}, State) ->
  R = log4erl:warn(format_and_prefix_report("WARNING REPORT", Rep)),
  {R, State}.

handle_call(_Request, State) ->
  Reply = ok,
  {ok, Reply, State}.

handle_info(_Info, State) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%-----------------------------------------------------------------
%%% Utility code taken from the erlang error_logger source.
%%%-----------------------------------------------------------------

format_and_prefix_report(Name, Rep) ->
  io_lib:format("[~s]~n~s", [Name, format_report(Rep)]).

format_report(Rep) when is_list(Rep) ->
  case string_p(Rep) of
    true ->
      io_lib:format("~s~n",[Rep]);
    _ ->
      format_rep(Rep)
  end;
format_report(Rep) ->
  io_lib:format("~p~n",[Rep]).

format_rep([{Tag,Data}|Rep]) ->
  io_lib:format("    ~p: ~p~n",[Tag,Data]) ++ format_rep(Rep);
format_rep([Other|Rep]) ->
  io_lib:format("    ~p~n",[Other]) ++ format_rep(Rep);
format_rep(_) ->
  [].

string_p([]) ->
  false;
string_p(Term) ->
  string_p1(Term).

string_p1([H|T]) when is_integer(H), H >= $\s, H < 255 ->
  string_p1(T);
string_p1([$\n|T]) -> string_p1(T);
string_p1([$\r|T]) -> string_p1(T);
string_p1([$\t|T]) -> string_p1(T);
string_p1([$\v|T]) -> string_p1(T);
string_p1([$\b|T]) -> string_p1(T);
string_p1([$\f|T]) -> string_p1(T);
string_p1([$\e|T]) -> string_p1(T);
string_p1([H|T]) when is_list(H) ->
  case string_p1(H) of
    true -> string_p1(T);
    _    -> false
  end;
string_p1([]) -> true;
string_p1(_) ->  false.
