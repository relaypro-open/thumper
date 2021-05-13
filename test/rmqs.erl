-module(rmqs).
-behaviour(gen_server).
-export([queuejournal_dir/0]).
-export([start_link/1, get_port/1, stop/1, kill/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {args, port, erl_pid, linux_pid}).

-include_lib("eunit/include/eunit.hrl").

-define(Base, "/tmp/thumper_eunit_rmqs").
-define(MnesiaDir, ?Base ++ "/var/lib/rabbitmq/mnesia").
-define(LogDir, ?Base ++ "/var/log/rabbitmq").
-define(ConfigDir, ?Base ++ "/etc/rabbitmq").
-define(ConfigFile, ?ConfigDir ++ "/rabbitmq.conf").
-define(ConfigEnvFile, ?ConfigDir ++ "/rabbitmq-env.conf").
-define(QJournalDir, ?Base ++ "/queuejournal").

queuejournal_dir() ->
    ?QJournalDir.

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

stop(Server) ->
    gen_server:call(Server, stop, infinity).

kill(Server) ->
    gen_server:call(Server, kill, infinity).

get_port(Server) ->
    gen_server:call(Server, get_port, infinity).

init(Args=#{port := Port}) ->
    os:cmd("rm -rf " ++ ?Base),
    filelib:ensure_dir(?Base++"/"),
    filelib:ensure_dir(?MnesiaDir++"/"),
    filelib:ensure_dir(?LogDir++"/"),
    filelib:ensure_dir(?ConfigDir++"/"),

    file:write_file(?ConfigEnvFile, <<>>),
    file:write_file(?ConfigFile, <<>>),

    ActualPort = case Port of
                     0 ->
                         case gen_tcp:listen(0, [{ip, {127,0,0,1}}]) of
                             {ok, LSocket} ->
                                 {ok, LPort} = inet:port(LSocket),
                                 gen_tcp:close(LSocket),
                                 timer:sleep(1000),
                                 LPort
                         end;
                     Port ->
                         Port
                 end,
    Command = "RABBITMQ_MNESIA_BASE=\"" ++ ?MnesiaDir ++ "\" " ++
              "RABBITMQ_NODE_IP_ADDRESS=127.0.0.1 " ++
              "RABBITMQ_NODE_PORT=" ++ integer_to_list(ActualPort) ++ " " ++
              "RABBITMQ_LOG_BASE=\"" ++ ?LogDir ++ "\" " ++
    % loading config files doesn't work
              %"RABBITMQ_CONFIG_ENV_FILE=\"" ++ ?ConfigEnvFile ++ "\" " ++
              %"RABBITMQ_CONFIG_FILE=\"" ++ ?ConfigFile ++ "\" " ++
            "rabbitmq-server",
    ?debugFmt("exec: ~s", [Command]),
    case exec:run_link(Command, [stdout, stderr, {kill_timeout, 1}]) of
        {ok, ErlPid, LinuxPid} ->
            {ok, #state{args=Args, port=ActualPort, erl_pid=ErlPid, linux_pid=LinuxPid}}
    end.

handle_call(get_port, _From, State=#state{port=Port}) ->
    {reply, Port, State};
handle_call(stop, _From, State=#state{erl_pid=Pid}) ->
    ok = exec:stop(Pid),
    {reply, ok, State};
handle_call(kill, _From, State=#state{erl_pid=Pid}) ->
    catch(exec:kill(Pid, sigkill)),
    {reply, ok, State}.

handle_cast(kill, State=#state{erl_pid=Pid}) ->
    catch(exec:kill(Pid, sigkill)),
    {noreply, State}.

handle_info({StdOutErr, LinuxPid, Data}, State=#state{linux_pid=LinuxPid}) when StdOutErr =:= stdout orelse StdOutErr =:= stderr ->
    ?debugMsg(iolist_to_binary(Data)),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.
