assert_received_messages(Results, ExpectedResults) ->
  % Verify that Results contains Expected results. Note that although routers
  % still attempt to avoid copying messages when they are published through multiple
  % topics that have the same set of subscribers, we do not test for that here since
  % router sharding can still cause those messages to be duplicated.
  Result = lists:all(fun({ExpectedTopic, ExpectedMessage}) ->
          lists:any(fun({ActualTopics, ActualMessage}) ->
                lists:member(ExpectedTopic, ActualTopics) and (ActualMessage =:= ExpectedMessage)
            end, Results)
      end, ExpectedResults),
  ?assert(Result =:= true).

assert_not_received_message(Results, NotAllowed) ->
  Result = lists:any(fun({_ActualTopics, ActualMessage}) ->
            (ActualMessage =:= NotAllowed)
          end, Results),
  ?assert(Result =:= false).
