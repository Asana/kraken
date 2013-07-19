-module(bounded_queue).

%%%-----------------------------------------------------------------
%%% Exports
%%%-----------------------------------------------------------------

-export([new/0, new/1, push/2, peek/1, drop/1, update_bound/2, len/1]).

%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------

%% @doc Returns an empty bounded queue, fixed at size = bound
new() ->
  {queue:new(), infinity, 0}.
new(Bound) ->
  {queue:new(), Bound, 0}.

%% @doc Returns the result of adding Item to queue.
%% Drops Item at head of the queue if the queue is at its max size
push(Item, {Queue, Bound, Len}) ->
  if (Len =:= Bound) ->
      log4erl:warn("Dropped Item. Queue at Maximum Size: ~p", [Bound]),
      {queue:in(Item, queue:drop(Queue)), Bound, Len};
    true ->
      {queue:in(Item, Queue), Bound, Len + 1}
  end.

peek({Queue, _Bound, _Len}) ->
  Q = queue:out(Queue),
  case Q of
    {{value, Item}, _Q2} ->
      Item;
    {empty,{[],[]}} ->
      empty
  end.

drop({Queue, Bound, Len}) ->
  {queue:drop(Queue), Bound, Len - 1}.

%% @doc Returns Bounded Queue with updated bound
%% If the new bound is smaller than len, will drop items at head
update_bound(NewBound, BQueue={Queue, _Bound, Len}) ->
  if (NewBound < Len) ->
      log4erl:warn("Dropping Item. Bound : ~p, Len: ~p", [NewBound, Len]),
      update_bound(NewBound, drop(BQueue));
    true ->
      {Queue, NewBound, Len}
  end.

len({_Queue, _Bound, Len}) ->
  Len.

bound({_Queue, Bound, _Len}) ->
  Bound.
