-module(kraken_tcp_connection).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     % Called on each new connection. Expected to return {ok, State}.
     {init, 1},
     % Called whenever the socket receives new data.
     {handle_data, 3},
     % Called when a socket is closed by the client.
     {handle_client_disconnect, 2},
     % Called when a client is idle for too long.
     {handle_client_timeout, 2},
     % Called when the server has reached max clients and will close the
     % socket as a result.
     {handle_server_busy, 1}
    ];
behaviour_info(_Other) ->
    undefined.

