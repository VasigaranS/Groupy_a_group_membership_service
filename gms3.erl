%% @author vasigarans
%% @doc @todo Add description to gms3.


-module(gms3).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start/1,start/2]).
-define(arghh,100).
-define(timeout,300).


leader(Id, Master,N, Slaves, Group) ->
    receive
        {mcast, Msg} ->
            bcast(Id, {msg,N, Msg}, Slaves),
            Master ! Msg,
            leader(Id, Master,N+1, Slaves, Group);
        %message from a peer or master that requests to join the group
        %Wrk = process identifier of the application layer
        %Peer = process identifier of group process
        {join, Wrk, Peer} ->
            io:format("gms ~w: forward join from ~w to master~n", [Id, Peer]),
            Slaves2 = lists:append(Slaves, [Peer]),
            Group2 = lists:append(Group, [Wrk]),
            bcast(Id, {view,N, [self()|Slaves2], Group2}, Slaves2),
            Master ! {view, Group2},
            io:format("inside the leader loop: Wrk ~w & Peer ~w~n",[Wrk,Peer]),
            leader(Id, Master,N+1, Slaves2, Group2);
stop -> ok
end.


slave(Id, Master, Leader,N,Last, Slaves, Group) ->
    receive
        {mcast, Msg} ->
            %request from the master to multicaset message redirected to the leader
            io:format("gms ~w: received {mcast, ~w} in state ~w~n", [Id, Msg, N]),
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader,N,Last, Slaves, Group);
        {join, Wrk, Peer} ->
            %master request to join a new node to the group, is redirected to leader
            io:format("gms ~w: forward join from ~w to leader~n", [Id, Peer]),
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader,N,Last, Slaves, Group);
		{msg, I, _} when I < N ->
            io:format("Message dropped because ~w is smaller than ~w~n", [I,N]),
            %check if message has already been sent
    slave(Id, Master, Leader, N, Last, Slaves, Group);
          %multicasted message from the leader, redirected to master
        {msg,I, Msg} ->
            io:format("gms ~w: deliver msg ~w in state ~w~n", [Id, Msg, I]),
            Master ! Msg,
            slave(Id, Master, Leader,I,{msg,I, Msg}, Slaves, Group);
		{view,I, [Leader|Slaves2], Group2} when I<N->
            io:format("View dropped because ~w: is smaller than ~w View : ~w~n", [I, N,{view,I, [Leader|Slaves2], Group2}]),
			slave(Id, Master, Leader, N, Last, Slaves, Group);
        %multicasted view from leader, forward to master process
        
        {view,I, [Leader|Slaves2], Group2} ->
            Master ! {view, Group2},
            io:format("gms ~w: received view ~w ~w~n", [Id, N, {view,I, [Leader|Slaves2], Group2}]),
            slave(Id, Master, Leader,I,{view,I, [Leader|Slaves2], Group2}, Slaves2, Group2);
		{'DOWN', _Ref, process, Leader, _Reason} ->
            io:format("received down message! ~w is down! ~n", [Leader]),
            election(Id, Master,N,Last, Slaves, Group);
        stop ->
ok end.
%%start   leader node
start(Id) ->
    Rnd = random:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun()-> init(Id, Rnd, Self) end)}.
init(Id, Rnd, Master) ->
    random:seed(Rnd, Rnd, Rnd),
    leader(Id, Master, 0,[], [Master]).


%start slave node
start(Id,Grp)->
	Rnd=random:uniform(1000),
	Self=self(),
	{ok,spawn_link(fun()->init(Id,Rnd,Grp,Self) end)}.


init(Id,Rnd,Grp,Master)->
	random:seed(Rnd,Rnd,Rnd),
	Self=self(),
    %application to join the group
	Grp!{join,Master,Self},
    %invitaton to join
	receive
		{view,N,[Leader|Slaves],Group}->
			Master!{view,Group},
            %after becoming a slave
			erlang:monitor(process,Leader),
			slave(Id,Master,Leader,N,{view,N,[Leader|Slaves],Group},Slaves,Group)
			after ?timeout ->
            Master ! {error, "no reply from leader"}
	end.


election(Id, Master,N, Last, Slaves, [_|Group]) ->
    Self = self(),
    case Slaves of
        [Self|Rest] ->
			bcast(Id, Last, Rest),
            bcast(Id, {view,N, Slaves, Group}, Rest),
            Master ! {view, Group},
            io:format("new leader elected!! and it is worker: ~w~n", [Id]),
            leader(Id, Master,N, Rest, Group);
        [Leader|Rest] ->
            erlang:monitor(process, Leader),
            slave(Id, Master, Leader,N,Last, Rest, Group)
end.


bcast(Id, Msg, Nodes) ->
    lists:foreach(fun(Node) -> Node ! Msg, crash(Id) end, Nodes).
crash(Id) ->
    case random:uniform(?arghh) of
        ?arghh ->
            io:format("leader ~w: crash~n", [Id]),
            exit(no_luck);
_ -> ok
end.








%% ====================================================================
%% Internal functions
%% ====================================================================


