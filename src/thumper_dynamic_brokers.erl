-module(thumper_dynamic_brokers).
-behaviour(gen_server).

-export([start_link/0, ensure/1, ensure/2,
    internalize_broker_name/1,
    externalize_broker_name/2,
    get_pid/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("../include/thumper.hrl").

-record(st, {}).

ensure(BrokerName) ->
    % By default we construct the host using the nocell.io convention
    % If this doesn't work for you, use ensure/2, or provide your own
    % construction string in env
    {ConfigName, Extension} = internalize_broker_name(BrokerName),
    HostConstructionFun = fun() ->
         {ok, Construction} = application:get_env(thumper,
                 dynamic_broker_host_construction),
         Pattern = lists:flatten(["{{(\\s)*name(\\s)*}}"]),
         re:replace(Construction, Pattern, ?Str(ConfigName), [{return, list},
                 global])
    end,
    ensure({ConfigName, Extension}, HostConstructionFun).

ensure(BrokerName, BrokerHostInput) ->
    {ConfigName, Extension} = internalize_broker_name(BrokerName),
    SvrName = externalize_broker_name(ConfigName, Extension),
    case get_pid(thumper_utils:get_thumper_svr(SvrName), thumper_sup) of
        {ok, _Pid} ->
            SvrName;
        {error, terminated} ->
            case supervisor:restart_child(thumper_sup,
                    thumper_utils:get_thumper_svr(SvrName)) of
                {ok, _RestartPid} ->
                    SvrName;
                _ ->
                    ?THUMP(error, "cannot restart ~p", [SvrName]),
                    SvrName
            end;
        {error, deleted} ->
            BrokerHost = if is_function(BrokerHostInput) ->
                    BrokerHostInput();
                true ->
                    BrokerHostInput
            end,
            case broker_spec(SvrName, BrokerHost) of
                {ok, Spec} ->
                    case supervisor:start_child(thumper_sup, Spec) of
                        {ok, _StartPid} ->
                            SvrName;
                        _ ->
                            ?THUMP(error, "cannot start ~p", [SvrName]),
                            SvrName
                    end;
                _ ->
                    ?THUMP(error, "cannot spec ~p", [SvrName]),
                    SvrName
            end
    end.

internalize_broker_name({ConfigName, Extension}) ->
    {ConfigName, ?Str(Extension)};
internalize_broker_name(ConfigName) ->
    {ConfigName, ""}.

externalize_broker_name(ConfigName, "") ->
    ?Atom(?Str(ConfigName));
externalize_broker_name(ConfigName, Extension) ->
    ?Atom(?Str(ConfigName) ++ "_" ++ Extension).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

broker_spec(BrokerName, BrokerHost) ->
    % serialize calls for the spec to ensure the env vars are locked
    gen_server:call(?MODULE, {broker_spec, BrokerName, BrokerHost}, infinity).

init([]) ->
    {ok, #st{}}.

handle_call({broker_spec, BrokerName, BrokerHost}, _From, State) ->
    {reply, spec(BrokerName, BrokerHost), State};
handle_call(_Req, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_pid(SvrName, Sup) ->
    case whereis(SvrName) of
        WhereIsPid when is_pid(WhereIsPid) ->
            {ok, WhereIsPid};
        _ ->
            Children = supervisor:which_children(Sup),
            case lists:keyfind(SvrName, 1, Children) of
                {SvrName, Pid, _, _} when is_pid(Pid) ->
                    {ok, Pid};
                {SvrName, _, _, _} ->
                    {error, terminated};
                false ->
                    {error, deleted}
            end
    end.

spec(BrokerName, BrokerHost) ->
    case ensure_vars(BrokerName, BrokerHost) of
        {ok, Broker} ->
            {ok, thumper_sup:child_spec(Broker)};
        _ ->
            {error, spec}
    end.

ensure_vars(Broker, BrokerHost) ->
    Brokers = application:get_env(thumper, brokers, []),
    case proplists:get_value(Broker, Brokers) of
        undefined ->
            %% look up the default config and shove the correct hostname in
            %%  -- probably not a good general solution
            case proplists:get_value(default, Brokers) of
                undefined ->
                    {error, no_default_config};
                DefaultConfig ->
                    Rabbit = proplists:get_value(rabbitmq_config, DefaultConfig),
                    Rabbit2 = proplist_utils:store({host, BrokerHost}, Rabbit),
                    NewConfig = proplist_utils:store({rabbitmq_config, Rabbit2}, DefaultConfig),
                    Brokers2 = proplist_utils:store({Broker, NewConfig}, Brokers),
                    ?THUMP(info, "created new broker config ~p ~p", [Broker, NewConfig]),
                    application:set_env(thumper, brokers, Brokers2),
                    {ok, Broker}
            end;
        _ ->
            {ok, Broker}
    end.
