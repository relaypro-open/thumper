-module(thumper_consumer_sup).

-behaviour(supervisor).

-export([start_link/2, ensure/4, init/1, test/1, spec/1, ref/1, install/2,
        spec/2]).

-include("../include/thumper.hrl").

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(SUB(SrvRef, Broker, Queue, Callback),
    {SrvRef, {thumper_consumer,
            start_link,
            [SrvRef,
                Broker,
                Queue,
                Callback
            ]},
        permanent,
        5000,
        worker,
        [thumper_consumer]
    }
).

start_link(Ref, Consumers) ->
    supervisor:start_link({local, Ref}, ?MODULE, [Consumers]).

test(Ref1) ->
    %% Suggest you keep your consumers' supervisor out of the thumper
    %% supervision tree. This allows your app to have more reliable message
    %% consumption, in case the thumper app crashes.
    install(thumper_app_sup, Ref1).

install(Sup, Ref1) ->
    {Ref, Spec} = spec(Ref1),
    supervisor:start_child(Sup, Spec),
    Ref.

ref(Ref1) ->
    ?Atom("thumper_consumer_sup_" ++ ?Str(Ref1)).

spec(Ref1) ->
    spec(Ref1, []).

spec(Ref1, Consumers) ->
    Ref = ref(Ref1),
    Spec = {{thumper_consumer_sup, Ref1}, {thumper_consumer_sup, start_link, [Ref, Consumers]},
        permanent, 5000, supervisor, [thumper_consumer_sup]},
    {Ref, Spec}.

init([Consumers]) ->
    Children = lists:map(
        fun({Name, Broker, Queue, Callback}) ->
                ?SUB(Name, Broker, thumper_utils:subs(Queue), Callback);
            (C) when is_list(C) ->
                ?SUB(
                    proplists:get_value(name, C),
                    proplists:get_value(broker, C),
                    thumper_utils:subs(proplists:get_value(queue, C)),
                    proplists:get_value(callback, C))
        end, Consumers),
    {ok, { {one_for_one, 5, 10}, Children} }.

ensure(Ref1, BrokerName, QueueName, Callback) when not is_tuple(BrokerName) ->
    ensure(Ref1, {BrokerName, consume}, QueueName, Callback);
ensure(Ref1, BrokerName, QueueName1, Callback) ->
    ConnectionSvrName = thumper:ensure(BrokerName),
    ConsumerSvrName = consumer_svr_name(ConnectionSvrName, QueueName1),
    Sup = ref(Ref1),
    case thumper_dynamic_brokers:get_pid(ConsumerSvrName, Sup) of
        {ok, _Pid} ->
            ConsumerSvrName;
        {error, terminated} ->
            case supervisor:restart_child(Sup, ConsumerSvrName) of
                {ok, _RestartPid} ->
                    ConsumerSvrName;
                _ ->
                    ?THUMP(error, "cannot restart ~p", [ConsumerSvrName]),
                    ConsumerSvrName
            end;
        {error, deleted} ->
            QueueName = thumper_utils:subs(QueueName1),
            Spec = ?SUB(ConsumerSvrName, ConnectionSvrName, QueueName, Callback),
            case supervisor:start_child(Sup, Spec) of
                {ok, _StartPid} ->
                    ConsumerSvrName;
                _ ->
                    ?THUMP(error, "cannot start ~p", [ConsumerSvrName]),
                    ConsumerSvrName
            end
    end.

consumer_svr_name(ConnectionSvrName, QueueName) ->
    ?Atom(?Str(ConnectionSvrName) ++ "_" ++ ?Str(QueueName)).
