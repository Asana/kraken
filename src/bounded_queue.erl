-module(bounded_queue).

%%%-----------------------------------------------------------------
%%% Exports
%%%-----------------------------------------------------------------

-export([new/0, new/1, push/2, peek/1, drop/1, update_bound/2, len/1, bound/1]).

%%%-----------------------------------------------------------------
%%% API
%%%-----------------------------------------------------------------

%% @doc Returns an empty bounded queue, fixed at size = bound
%% @spec new() -> bounded_queue()
new() ->
  {queue:new(), infinity, 0}.
%% @spec new(Bound :: int()) -> bounded_queue()
new(Bound) ->
  {queue:new(), Bound, 0}.

%% @doc Returns the result of adding Item to queue.
%% Drops Item at head of the queue if the queue is at its max size
%% @spec push(Item :: Type(), bounded_queue()) -> {Type(), bounded_queue()}
push(Item, {Queue, Bound, Len}) ->
  if (Len =:= Bound) ->
      {{value, DroppedItem }, SmallerQueue} = queue:out(Queue),
      {{dropped, DroppedItem}, {queue:in(Item, SmallerQueue), Bound, Len}};
    true ->
      {normal, {queue:in(Item, Queue), Bound, Len + 1}}
  end.

%% @doc Return the item at the head of the queue
%% @spec peek(bounded_queue()) -> Type()
peek({Queue, _Bound, _Len}) ->
  Q = queue:out(Queue),
  case Q of
    {{value, Item}, _Q2} ->
      Item;
    {empty,{[],[]}} ->
      empty
  end.

%% @doc Return everything by the head of the queue
%% @spec drop(bounded_queue()) -> bounded_queue()
drop({Queue, Bound, Len}) ->
  {queue:drop(Queue), Bound, Len - 1}.

%% @doc Returns Bounded Queue with updated bound
%% If the new bound is smaller than len, will drop items at head
%% @spec update_bound(NewBound :: int(), BQueue:: bounded_queue()) -> bounded_queue()
update_bound(NewBound, BQueue={Queue, _Bound, Len}) ->
  if (NewBound < Len) ->
      log4erl:warn("Dropping Item. Bound : ~p, Len: ~p", [NewBound, Len]),
      update_bound(NewBound, drop(BQueue));
    true ->
      {Queue, NewBound, Len}
  end.

%% @doc Return the number of items in the queue
%% @spec len(bounded_queue()) -> int()
len({_Queue, _Bound, Len}) ->
  Len.

%% @spec bound(bounded_queue()) -> int()
bound({_Queue, Bound, _Len}) ->
  Bound.
