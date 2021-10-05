-module(thumper_utils).

-export([
        declare_queue/2,
        delete_queue/2,
        bind_queue/4,
        unbind_queue/4,
        declare_exchange/2,
        bind_exchange/4,
        unbind_exchange/4,
        init_channel/1,
        close_channel/1,
        pid_guard/2,
        monitor/1,
        get_thumper_svr/1,
        basic_get/2,
        subs/1,
        load_sub_rules/0,
        str/1,
        atom/1,
        bin/1
    ]).

%% exports for eunit tests
-export([test_var_value/0]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("../include/thumper.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(BuiltInSubRules, [{host, {inet, gethostname, []}}]).

str(X) when is_list(X) -> X;
str(X) when is_binary(X) -> binary_to_list(X);
str(X) when is_atom(X) -> atom_to_list(X);
str(X) when is_integer(X) -> integer_to_list(X).

atom(X) when is_atom(X) -> X;
atom(X) when is_binary(X) -> atom(binary_to_list(X));
atom(X) when is_list(X) -> try list_to_existing_atom(X) catch error:badarg -> list_to_atom(X) end;
atom(X) when is_integer(X) -> atom(integer_to_list(X)).

bin(X) when is_binary(X) -> X;
bin(X) when is_list(X) -> iolist_to_binary(X);
bin(X) when is_atom(X) -> bin(atom_to_list(X));
bin(X) when is_integer(X) -> bin(integer_to_list(X)).

init_channel(undefined) ->
    {error, channel_undefined};

init_channel(ConnectionPid) ->
    ?THUMP(info, "init_channel on connection: ~p", [ConnectionPid]),
    try amqp_connection:open_channel(ConnectionPid) of
        closing ->
            ?THUMP(error, "cannot open channel while connection is closing", []),
            {error, closing};
        {ok, Pid} ->
            amqp_channel:register_return_handler(Pid, self()),
            amqp_channel:register_flow_handler(Pid, self()),
            {ok, Pid};
        {error, Error} ->
            ?THUMP(error, "error during init_channel: ~p", [Error]),
            {error, Error}
    catch
        X:Y ->
            ?THUMP(error, "exception during init_channel: ~p ~p", [X,Y]),
            {error, {X, Y}}
    end.

close_channel(Channel) ->
    ?THUMP(info, "close_channel for channel: ~p", [Channel]),
    try amqp_channel:close(Channel) of
        closing ->
            ?THUMP(error, "channel already closing", []),
            ok;
        blocked ->
            ?THUMP(error, "channel is blocked", []),
            ok;
        ok ->
            ok;
        Error ->
            ?THUMP(error, "error during close_channel: ~p", [Error]),
            Error
    catch
        X:Y ->
            ?THUMP(error, "exception during close_channel: ~p ~p", [X,Y]),
            {error, {X, Y}}
    end.

declare_queue(Channel, QueueProplist) ->
    QueueRecord = #'queue.declare'{},
    Keys = proplists:get_keys(QueueProplist),
    Queue = lists:foldl(
        fun(Key, Record) -> 
                Value = proplists:get_value(Key, QueueProplist), 
                update_queue_declare_record(Record, Key, Value) 
        end, QueueRecord, Keys),
    QueueName = Queue#'queue.declare'.queue,
    ?THUMP(info, "declare queue: ~p", [QueueName]),
    try amqp_channel:call(Channel, Queue) of
        #'queue.declare_ok'{} ->
            {ok, QueueName};
        Error ->
            ?THUMP(error, "Failed to declare queue: ~p error ~p", [QueueName, Error]),
            {error, Error}
    catch
        X:Y ->
            ?THUMP(error,"caught exception: ~p ~p", [X,Y]),
            {error, {X, Y}}
    end.

delete_queue(Channel, Queue) when is_list(Queue) ->
    delete_queue(Channel, iolist_to_binary(Queue));
delete_queue(Channel, Queue) when is_binary(Queue) ->
    QueueRecord = #'queue.delete'{queue=Queue},
    ?THUMP(info, "delete queue: ~p", [Queue]),
    try amqp_channel:call(Channel, QueueRecord) of
        #'queue.delete_ok'{} ->
            ok;
        Error ->
            ?THUMP(error, "Failed to delete queue: ~p error ~p", [Queue, Error]),
            {error, Error}
    catch
        X:Y ->
            ?THUMP(error,"caught exception: ~p ~p", [X,Y]),
            {error, {X, Y}}
    end.
    
bind_queue(Channel, QueueName, ExchangeName, RoutingKey) when is_list(QueueName) ->
    bind_queue(Channel, list_to_binary(QueueName), ExchangeName, RoutingKey);
bind_queue(Channel, QueueName, ExchangeName, RoutingKey) when is_list(ExchangeName) ->
    bind_queue(Channel, QueueName, list_to_binary(ExchangeName), RoutingKey);
bind_queue(Channel, QueueName, ExchangeName, RoutingKey) ->
    Binding = #'queue.bind'{queue=subs(QueueName), exchange=subs(ExchangeName),
        routing_key=subs(RoutingKey)},
    ?THUMP(info, "bind_queue ~p ~p ~p ~p", [Channel, Binding#'queue.bind'.queue,
            Binding#'queue.bind'.exchange, Binding#'queue.bind'.routing_key]),
    try amqp_channel:call(Channel, Binding) of
        #'queue.bind_ok'{} ->
            {ok, Binding};
        Error ->
            ?THUMP(error, "Failed to bind queue ~p to exchange ~p with routing key ~p: error ~p", [QueueName, ExchangeName, RoutingKey, Error]),
            {error, Error}
    catch
        X:Y ->
            ?THUMP(error, "bind_queue exception: ~p ~p", [X,Y]),
            {error, {X, Y}}
    end.

unbind_queue(Channel, QueueName, ExchangeName, RoutingKey) when is_list(QueueName) ->
    unbind_queue(Channel, list_to_binary(QueueName), ExchangeName, RoutingKey);
unbind_queue(Channel, QueueName, ExchangeName, RoutingKey) when is_list(ExchangeName) ->
    unbind_queue(Channel, QueueName, list_to_binary(ExchangeName), RoutingKey);
unbind_queue(Channel, QueueName, ExchangeName, RoutingKey) ->
    Unbinding = #'queue.unbind'{queue=subs(QueueName), exchange=subs(ExchangeName),
        routing_key=subs(RoutingKey)},
    try amqp_channel:call(Channel, Unbinding) of
        #'queue.unbind_ok'{} ->
            {ok, Unbinding};
        Error ->
            ?THUMP(error, "Failed to unbind queue ~p to exchange ~p with routing key ~p: error ~p", [QueueName, ExchangeName, RoutingKey, Error]),
            {error, Error}
    catch
        X:Y ->
            ?THUMP(error, "unbind_queue exception: ~p ~p", [X,Y]),
            {error, {X, Y}}
    end.

declare_exchange(Channel, ExchangeProplist) ->
    ExchangeRecord = #'exchange.declare'{},
    Keys = proplists:get_keys(ExchangeProplist),
    Exchange = lists:foldl(
        fun(Key, Record) -> 
                Value = proplists:get_value(Key, ExchangeProplist), 
                update_exchange_declare_record(Record, Key, Value) 
        end, ExchangeRecord, Keys),
    ExchangeName = Exchange#'exchange.declare'.exchange,
    ?THUMP(info, "declare exchange: ~p", [ExchangeName]),
    try amqp_channel:call(Channel, Exchange) of
        #'exchange.declare_ok'{} ->
            {ok, ExchangeName};
        Error ->
            ?THUMP(error, "Failed to declare exchange: ~p error ~p", [ExchangeName, Error]),
            {error, Error}
    catch
        X:Y ->
            ?THUMP(error,"caught exception: ~p ~p", [X,Y]),
            {error, {X, Y}}
    end.

bind_exchange(Channel, Destination, Source, RoutingKey) when is_list(Destination) ->
    bind_exchange(Channel, list_to_binary(Destination), Source, RoutingKey);
bind_exchange(Channel, Destination, Source, RoutingKey) when is_list(Source) ->
    bind_exchange(Channel, Destination, list_to_binary(Source), RoutingKey);
bind_exchange(Channel, Destination, Source, RoutingKey) ->
    Binding = #'exchange.bind'{destination=subs(Destination),
        source=subs(Source), routing_key=subs(RoutingKey)},
    try amqp_channel:call(Channel, Binding) of
        #'exchange.bind_ok'{} ->
            {ok, Binding};
        Error ->
            ?THUMP(error, "Failed to bind destination exchange ~p to isource exchange ~p with routing key ~p: error ~p", [Destination, Source, RoutingKey, Error]),
            {error, Error}
    catch
        X:Y ->
            ?THUMP(error, "bind_exchange exception: ~p ~p", [X,Y]),
            {error, {X, Y}}
    end.

unbind_exchange(Channel, Destination, Source, RoutingKey) when is_list(Destination) ->
    unbind_exchange(Channel, list_to_binary(Destination), Source, RoutingKey);
unbind_exchange(Channel, Destination, Source, RoutingKey) when is_list(Source) ->
    unbind_exchange(Channel, Destination, list_to_binary(Source), RoutingKey);
unbind_exchange(Channel, Destination, Source, RoutingKey) ->
    Unbinding = #'exchange.unbind'{destination=subs(Destination),
        source=subs(Source), routing_key=subs(RoutingKey)},
    try amqp_channel:call(Channel, Unbinding) of
        #'exchange.unbind_ok'{} ->
            {ok, Unbinding};
        Error ->
            ?THUMP(error, "Failed to unbind destination exchange ~p from isource exchange ~p with routing key ~p: error ~p", [Destination, Source, RoutingKey, Error]),
            {error, Error}
    catch
        X:Y ->
            ?THUMP(error, "unbind_exchange exception: ~p ~p", [X,Y]),
            {error, {X, Y}}
    end.

% This clunky stuff is a consequence of the fact that erlang does not allow this:
% Variable = field, 
% State#record_name.Variable

update_queue_declare_record(Record, arguments, undefined) ->
    Record#'queue.declare'{arguments=undefined};

update_queue_declare_record(Record, arguments, Value) when is_list(Value) ->
    Value_ = lists:map(
        fun({Key, Type, Val}) ->
                {Key, Type, subs(Val)};
            (X) ->
                X
        end, Value),
    Record#'queue.declare'{arguments=Value_};

update_queue_declare_record(Record, nowait, Value) ->
    Record#'queue.declare'{nowait=Value};

update_queue_declare_record(Record, exclusive, Value) ->
    Record#'queue.declare'{exclusive=Value};

update_queue_declare_record(Record, durable, Value) ->
    Record#'queue.declare'{durable=Value};

update_queue_declare_record(Record, auto_delete, Value) ->
    Record#'queue.declare'{auto_delete=Value};

update_queue_declare_record(Record, passive, Value) ->
    Record#'queue.declare'{passive=Value};

update_queue_declare_record(Record, queue, Value) when is_list(Value) ->
    update_queue_declare_record(Record, queue, list_to_binary(Value));
update_queue_declare_record(Record, queue, Value) ->
    Record#'queue.declare'{queue=subs(Value)}.

update_exchange_declare_record(Record, arguments, Value) ->
    Record#'exchange.declare'{arguments=Value};

update_exchange_declare_record(Record, nowait, Value) ->
    Record#'exchange.declare'{nowait=Value};

update_exchange_declare_record(Record, passive, Value) ->
    Record#'exchange.declare'{passive=Value};

update_exchange_declare_record(Record, auto_delete, Value) ->
    Record#'exchange.declare'{auto_delete=Value};

update_exchange_declare_record(Record, durable, Value) ->
    Record#'exchange.declare'{durable=Value};

update_exchange_declare_record(Record, type, Value) ->
    Record#'exchange.declare'{type=Value};

update_exchange_declare_record(Record, exchange, Value) ->
    Record#'exchange.declare'{exchange=subs(Value)}.

pid_guard(Pid, {M, F, A}) ->
    case ?IsPidAlive(Pid) of
        true ->
            apply(M, F, A);
        _ ->
            {error, pid_not_alive, Pid}
    end.

monitor(Pid) ->
    Ref = erlang:monitor(process, Pid),
    ?THUMP(info, "thumper monitor: ~p ~p", [Pid, Ref]),
    Ref.

get_thumper_svr(BrokerName) ->
    case BrokerName of
        default -> thumper;
        _ -> ?Atom("thumper_"++?Str(BrokerName))
    end.

subs(S) when is_binary(S) ->
    substitute_all(load_sub_rules(), S);
subs(S) ->
    S.

load_sub_rules() ->
    CustomRules = application:get_env(thumper, substitution_rules, []),
    % Overwrite built in rules with custom rules
    lists:foldl(fun(T={K, _V}, Rules) ->
                lists:keystore(K, 1, Rules, T)
        end,
        ?BuiltInSubRules,
        CustomRules).

substitute_all([], String) ->
    String;
substitute_all([{Var, Sub} | Rest], String) ->
    case substitute(String, Var, Sub) of
        {error, Error} ->
            {error, Error};
        NewValue ->
            substitute_all(Rest, NewValue)
    end.

substitute(S, Var, Sub) ->
    Pattern = iolist_to_binary(["{{(\\s)*",?Str(Var),"(\\s)*}}"]),
    case re:run(S, Pattern, [{capture, none}]) of
        match ->
            case get_substitution_val(Sub) of
                {ok, Val} ->
                    re:replace(S, Pattern, Val, [{return, binary}, global]);
                _ ->
                    {error, bad_substitution_value}
            end;
        _ ->
            S
    end.

get_substitution_val({M, F, A}) ->
    case erlang:apply(M, F, A) of
        {ok, Val} ->
            {ok, Val};
        {error, E} ->
            {error, E};
        Val ->
            {ok, Val}
    end;
get_substitution_val(X) when is_binary(X) ->
    {ok, X};
get_substitution_val(X) when is_list(X) ->
    {ok, X}.

basic_get(Channel, QueueName) ->
    case amqp_channel:call(Channel, #'basic.get'{queue=QueueName}) of
        {#'basic.get_ok'{delivery_tag=DeliveryTag, routing_key=RoutingKey},
            #amqp_msg{payload=Payload}} ->
            {ok, {DeliveryTag, RoutingKey, Payload}};
        #'basic.get_empty'{} ->
            ok
    end.

%% Tests 
thumper_subs_test_() ->
     [
        ?_assert(test1() =:= ok),
        ?_assert(test2() =:= ok)
    ].

test_var_value() ->
    {ok, "test_value"}.

test1() ->
    Str = <<"api.{{ host }}.event">>,
    {ok, Host} = inet:gethostname(),
    Expected = iolist_to_binary(["api.", Host, ".event"]),
    Expected = subs(Str),
    ok.

test2() ->
    application:set_env(thumper, substitution_rules, [{test, {thumper_utils, test_var_value, []}}]),
    Str = <<"api.{{ test }}.{{ host }}.event">>,
    {ok, Host} = inet:gethostname(),
    {ok, Test} = test_var_value(),
    Expected = iolist_to_binary(["api.", Test, ".", Host, ".event"]),
    Expected = subs(Str),
    application:unset_env(thumper, substitution_rules),
    ok.
