
-module(node_chord_file).
-export([generate_nodes/5, listen_to_node/1, node/4]).


arbitrary_node(Node_id, []) -> Node_id;
arbitrary_node(_, ExistingNodes) -> lists:nth(rand:uniform(length(ExistingNodes)), ExistingNodes).

retrieve_nearest(_, [], MinNode, _, _) ->
    MinNode;
retrieve_nearest(Key, FingerNodeIds, MinNode, MinVal, State) ->
    [First| Rest] = FingerNodeIds,
    Distance = retrieve_distance(Key, First, dict:fetch(m, State), 0),
    if
        Distance < MinVal ->
            retrieve_nearest(Key, Rest, First, Distance, State);
        true -> 
            retrieve_nearest(Key, Rest, MinNode, MinVal, State)
    end.

retrieve_nearest_node(Key, FingerNodeIds, State) ->
    case lists:member(Key, FingerNodeIds) of
        true -> Key;
        _ -> retrieve_nearest(Key, FingerNodeIds, -1, 10000000, State)
    end.

retrieve_distance(Key, Key, _, Distance) ->
    Distance;
retrieve_distance(Key, NodeId, M, Distance) ->
    retrieve_distance(Key, (NodeId + 1) rem trunc(math:pow(2, M)), M, Distance + 1).

check_range(From, To, Key, M) ->
    if 
        From < To -> 
            (From =< Key) and (Key =< To);
        trunc(From) == trunc(To) ->
            trunc(Key) == trunc(From);
        From > To ->
            ((Key >= 0) and (Key =< To)) or ((Key >= From) and (Key < trunc(math:pow(2, M))))
    end.

nearest_prior_finger(_, NodeState, 0) -> NodeState;
nearest_prior_finger(Id, NodeState, M) -> 
    MthFinger = lists:nth(M, dict:fetch(finger_table, NodeState)),
    
    case check_range(dict:fetch(id, NodeState), Id, dict:fetch(node ,MthFinger), dict:fetch(m, NodeState)) of
        true -> 

            dict:fetch(pid ,MthFinger) ! {state, self()},
            receive
                {statereply, FingerNodeState} ->
                    FingerNodeState
            end,
            FingerNodeState;
        _ -> nearest_prior_finger(Id, NodeState, M - 1)
    end
.

detect_previous(Id, NodeState) ->
    case 
        check_range(dict:fetch(id, NodeState) + 1, dict:fetch(id, dict:fetch(successor, NodeState)), Id, dict:fetch(m, NodeState)) of 
            true -> NodeState;
            _ -> detect_previous(Id, nearest_prior_finger(Id, NodeState, dict:fetch(m, NodeState)))
    end
.

detect_next(Id, NodeState) ->
    PredicessorNodeState = detect_previous(Id, NodeState),
    dict:fetch(successor, PredicessorNodeState)
.

listen_to_node(NodeState) ->
    Hash = dict:fetch(id, NodeState),
    receive
        {lookup, Id, Key, Count_the_hops, _Pid} ->

                NodeVal = retrieve_nearest_node(Key, dict:fetch_keys(dict:fetch(finger_table ,NodeState)), NodeState),
                UpdatedState = NodeState,
                io:format("Searching::: ~p  For Key ~p  Nearest Node ~p ~n", [Hash, Key, NodeVal]),
                if 
                    (Hash == Key) -> 
                        taskcompletionmonitor ! {completed, Hash, Count_the_hops, Key};
                    (NodeVal == Key) and (Hash =/= Key) -> 
                        taskcompletionmonitor ! {completed, Hash, Count_the_hops, Key};
                    true ->
                        dict:fetch(NodeVal, dict:fetch(finger_table, NodeState)) ! {lookup, Id, Key, Count_the_hops + 1, self()}
                end
                ;
        {kill} ->
            UpdatedState = NodeState,
            exit("received exit signal");
        {state, Pid} -> Pid ! NodeState,
                        UpdatedState = NodeState;
        {get_successor, Id, Pid} ->
                        FoundSeccessor = detect_next(Id, NodeState),
                        UpdatedState = NodeState,
                        {Pid} ! {get_successor_reply, FoundSeccessor};

        
        {fix_fingers, FingerTable} -> 
            io:format("Received Finger for ~p ~p", [Hash, FingerTable]),
            UpdatedState = dict:store(finger_table, FingerTable, NodeState)
    end, 
    listen_to_node(UpdatedState).


node(Hash, M, ChordNodes, _NodeState) -> 
    io:format("Node is spawned with hash ~p",[Hash]),
    FingerTable = lists:duplicate(M, arbitrary_node(Hash, ChordNodes)),
    NodeStateUpdated = dict:from_list([{id, Hash}, {predecessor, nil}, {finger_table, FingerTable}, {next, 0}, {m, M}]),
    listen_to_node(NodeStateUpdated).


append_node_to_chord(ChordNodes, TotalNodes, M, NetworkState) ->
    RemainingHashes = lists:seq(0, TotalNodes - 1, 1) -- ChordNodes,
    Hash = lists:nth(rand:uniform(length(RemainingHashes)), RemainingHashes),
    Pid = spawn(node_chord_file, node, [Hash, M, ChordNodes, dict:new()]),
    io:format("~n ~p ~p ~n", [Hash, Pid]),
    [Hash, dict:store(Hash, Pid, NetworkState)]
.

generate_nodes(ChordNodes, _, _, 0, NetworkState) -> 
    [ChordNodes, NetworkState];
generate_nodes(ChordNodes, TotalNodes, M, NumNodes, NetworkState) ->
    [Hash, NewNetworkState] = append_node_to_chord(ChordNodes, TotalNodes,  M, NetworkState),
    generate_nodes(lists:append(ChordNodes, [Hash]), TotalNodes, M, NumNodes - 1, NewNetworkState).

