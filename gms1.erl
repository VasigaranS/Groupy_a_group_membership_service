%% @author vasigarans
%% @doc @todo Add description to gms1.


-module(gms1).

%% ====================================================================
%% API functions
%% ====================================================================
-export([leader/4,start/1,start/2]).


leader(Id, Master, Slaves, Group) ->
	%io:format("Id ~w,Master ~w,Slaves ~w,Group ~w ~n",[Id, Master, Slaves, Group]),
    receive
        %message either from master or slave
        {mcast, Msg} ->
			%leader multicasting message to all the slaves.
            bcast(Id, {msg, Msg}, Slaves),
            %Master process id is constant
            Master ! Msg,
			%io:format("Slaves ~w Msg ~w~n",[Slaves,Msg]),
            leader(Id, Master, Slaves, Group);

        {join, Wrk, Peer} ->
            %message from either master or peer to join a new slave where Wrk is pid of app layer and process identifier of its group process
            %Every slave has its unique group process.
            Slaves2 = lists:append(Slaves, [Peer]),
            %append a new slave process
            Group2 = lists:append(Group, [Wrk]),
            %append a new group process
            %io:format("Slaves ~w Group ~w~n",[Slaves2,Group2]),
            bcast(Id, {view, [self()|Slaves2], Group2}, Slaves2),
            %broadcasts the new slaves and group process to all the nodes 
            Master ! {view, Group2},
            %maybe implements new window since new slave is joined
            leader(Id, Master, Slaves2, Group2);
stop -> ok
end.


slave(Id, Master, Leader, Slaves, Group) ->
	%io:format("Id ~w,Master ~w,Leader ~w,Slaves ~w,Group ~w ~n",[Id, Master, Leader, Slaves, Group]),
    receive
        {mcast, Msg} ->
			% from master multicast message to the leader who will multcast to all the nodes
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader, Slaves, Group);
        {join, Wrk, Peer} ->
            % from the master group of slaves receive a response for a new node to join, redirected to the leader
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, Slaves, Group);
        {msg, Msg} ->
            %message coming from leader and sent to the master
            Master ! Msg,
            slave(Id, Master, Leader, Slaves, Group);

        {view, [Leader|Slaves2], Group2} ->
            %multicastd view from the leader which if forwared to the Master.
            io:format("Group2 ~w ~n",[Group2]),
            Master ! {view, Group2},
            slave(Id, Master, Leader, Slaves2, Group2);
        stop ->
ok end.

start(Id) ->
    Self = self(),
    {ok, spawn_link(fun()-> init(Id, Self) end)}.

init(Id, Master) ->
    io:format(" Master~w~n ",[Master]),
    leader(Id, Master, [], [Master]).



start(Id, Grp) ->
    %the entire group has one process
    
    Self = self(),
    {ok, spawn_link(fun()-> init(Id, Grp, Self) end)}.
init(Id, Grp, Master) ->
    %io:format("Grp~w Master~w~n ",[Grp,Master]),
    Self = self(),
    Grp ! {join, Master, Self},
    receive
        {view, [Leader|Slaves], Group} ->
            Master ! {view, Group},
            slave(Id, Master, Leader, Slaves, Group)
end.

%bcast/3 that will send a message to each of the processes in a list.
bcast(_, Msg, Nodes) ->
    lists:foreach(fun(Node) -> Node ! Msg end, Nodes).







%% ====================================================================
%% Internal functions
%% ====================================================================


