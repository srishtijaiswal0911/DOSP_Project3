-module(main).
-import(node_chord_file, [generate_nodes/5, listen_to_node/1, node/4]).
-import(request_file, [msgs_conveyed_now_kill/5, listen_finishing_task/2]).
-import(finger_table_file, [convey_finger_tables/2]).
-export([main/2, create_network/2]).

get_m(NumNodes) ->
    trunc(math:ceil(math:log2(NumNodes))).

create_network(NumNodes, NumRequest) ->
    M = get_m(NumNodes),
    [ChordNodes, NetworkState] = generate_nodes([], round(math:pow(2, M)), M, NumNodes, dict:new()),
    convey_finger_tables(NetworkState,M),
    msgs_conveyed_now_kill(ChordNodes, NumNodes, NumRequest, M, NetworkState).

main(NumNodes, NumRequest) ->
    register(mainprocess, spawn(main, create_network, [NumNodes, NumRequest])).
