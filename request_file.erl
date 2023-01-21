-module(request_file).
-export([msgs_conveyed_now_kill/5, listen_finishing_task/2]).


kill_each_node([], _) ->
    ok;
kill_each_node(ChordNodes, NetworkState) -> 
    [First | Rest] = ChordNodes,
    retrieve_node_pid(First, NetworkState) ! {kill},
    kill_each_node(Rest, NetworkState).

retrieve_total_hops() ->
    receive
        {totalhops, Count_the_hops} ->
            Count_the_hops
        end.

retrieve_node_pid(Hash, NetworkState) -> 
    case dict:find(Hash, NetworkState) of
        error -> nil;
        _ -> dict:fetch(Hash, NetworkState)
    end
.
msg_convey_to_node(_, [], _) ->
    ok;
msg_convey_to_node(Key, ChordNodes, NetworkState) ->
    [First | Rest] = ChordNodes,
    Pid = retrieve_node_pid(First, NetworkState),
    Pid ! {lookup, First, Key, 0, self()},
    msg_convey_to_node(Key, Rest, NetworkState)
.

msgs_convey_to_all_nodes(_, 0, _, _) ->
    ok;
msgs_convey_to_all_nodes(ChordNodes, NumRequest, M, NetworkState) ->
    timer:sleep(1000),
    Key = lists:nth(rand:uniform(length(ChordNodes)), ChordNodes),
    msg_convey_to_node(Key, ChordNodes, NetworkState),
    msgs_convey_to_all_nodes(ChordNodes, NumRequest - 1, M, NetworkState)
.

listen_finishing_task(0, Count_the_hops) ->
    mainprocess ! {totalhops, Count_the_hops}
;

listen_finishing_task(NumRequests, Count_the_hops) ->
    receive 
        {completed, _Pid, HopsCountForTask, _Key} ->
            %io:format("received completion from ~p, Number of Hops ~p, For Key ~p", [_Pid, HopsCountForTask, _Key]),
            listen_finishing_task(NumRequests - 1, Count_the_hops + HopsCountForTask)
    end
.

msgs_conveyed_now_kill(ChordNodes, NumNodes, NumRequest, M, NetworkState) ->
    register(taskcompletionmonitor, spawn(request_file, listen_finishing_task, [NumNodes * NumRequest, 0])),
    msgs_convey_to_all_nodes(ChordNodes, NumRequest, M, NetworkState),
    TotalHops = retrieve_total_hops(),
    {ok, File} = file:open("./stats.txt", [append]),
    io:format(File, "~n Average Hops = ~p   Total Hops = ~p    Number of Nodes = ~p    Number of Requests = ~p  ~n", [TotalHops/(NumNodes * NumRequest), TotalHops, NumNodes , NumRequest]),
    io:format("~n Average Hops = ~p   Total Hops = ~p    Number of Nodes = ~p    Number of Requests = ~p  ~n", [TotalHops/(NumNodes * NumRequest), TotalHops, NumNodes , NumRequest]),
    kill_each_node(ChordNodes, NetworkState)
.
