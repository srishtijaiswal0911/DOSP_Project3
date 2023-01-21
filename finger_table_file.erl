-module(finger_table_file).
-export([convey_finger_tables/2]).

retrieve_ith_successor(Hash, NetworkState, I,  M) -> 
    case dict:find((Hash + I) rem trunc(math:pow(2, M)), NetworkState) of
        error ->
             retrieve_ith_successor(Hash, NetworkState, I + 1, M);
        _ -> (Hash + I) rem trunc(math:pow(2, M))
    end.

retrieve_finger_tables(_, _, M, M,FingerList) ->
    FingerList;
retrieve_finger_tables(Node, NetworkState, M, I, FingerList) ->
    Hash = element(1, Node),
    Ith_succesor = retrieve_ith_successor(Hash, NetworkState, trunc(math:pow(2, I)), M),
    retrieve_finger_tables(Node, NetworkState, M, I + 1, FingerList ++ [{Ith_succesor, dict:fetch(Ith_succesor, NetworkState)}] ).
assemble_finger_tables(_, [], FTDict,_) ->
    FTDict;

assemble_finger_tables(NetworkState, NetList, FTDict,M) ->
    [First | Rest] = NetList,
    FingerTables = retrieve_finger_tables(First, NetworkState,M, 0,[]),
    assemble_finger_tables(NetworkState, Rest, dict:store(element(1, First), FingerTables, FTDict), M).

convey_finger_tables_nodes([], _, _) ->
    ok;
convey_finger_tables_nodes(NodesToSend, NetworkState, FingerTables) ->
    [First|Rest] = NodesToSend,
    Pid = dict:fetch(First ,NetworkState),
    Pid ! {fix_fingers, dict:from_list(dict:fetch(First, FingerTables))},
    convey_finger_tables_nodes(Rest, NetworkState, FingerTables).

convey_finger_tables(NetworkState,M) ->
    FingerTables = assemble_finger_tables(NetworkState, dict:to_list(NetworkState), dict:new(),M),
    io:format("~n~p~n", [FingerTables]),
    convey_finger_tables_nodes(dict:fetch_keys(FingerTables), NetworkState, FingerTables).

