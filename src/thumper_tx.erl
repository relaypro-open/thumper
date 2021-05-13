-module(thumper_tx).
-export([consult/1, consult_run/1, run/1, run/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("../include/thumper.hrl").

-record(thumper_tx_state, {rabbitmq_config, broker_handle, channel}).

consult({callback, {M, F, A}}) ->
    case apply(M,F,A) of
        {ok, Config} ->
            {ok, Config};
        E ->
            E
    end;
consult(File) ->
    case file:consult(File) of
        {ok, [Config]} ->
            {ok, Config};
        E ->
            E
    end.

consult_run(File) ->
    case file:consult(File) of
        {ok, [Config]} ->
            run(Config);
        E ->
            E
    end.

run(Config) ->
    {Result, State_} = case setup(Config, #thumper_tx_state{}) of
        {ok, State} ->
            run(Config, State);
        {E, State} ->
            {E, State}
    end,
    teardown(Config, State_),
    Result.

run(Config, State=#thumper_tx_state{}) ->
    Result = case start_transaction(State) of
        ok ->
            Tx = proplists:get_value(tx, Config, []),
            case fill_transaction(Tx, State) of
                ok ->
                    commit_transaction(State);
                E ->
                    rollback_transaction(State),
                    E
            end;
        E ->
            E
    end,
    {Result, State};
run(Config, Channel) ->
    run(Config, #thumper_tx_state{channel=Channel}).

start_transaction(#thumper_tx_state{channel=Channel}) ->
    case amqp_channel:call(Channel, #'tx.select'{}) of
        {'tx.select_ok'} ->
            ok;
        E ->
            {error, E}
    end.

fill_transaction(_Tx=[], _State) ->
    ok;
fill_transaction(_Tx=[{TxItem, TxConfig}|TxRemain], State) ->
    case tx_item(TxItem, TxConfig, State) of
        ok ->
            fill_transaction(TxRemain, State);
        E ->
            E
    end.

tx_item('exchange.declare', Config, #thumper_tx_state{channel=Channel}) ->
    case thumper_utils:declare_exchange(Channel, Config) of
        {ok, _ExchangeName} ->
            ok;
        E ->
            E
    end;
tx_item('exchange.bind', Config, #thumper_tx_state{channel=Channel}) ->
    Destination = proplists:get_value(destination, Config),
    Source = proplists:get_value(source, Config),
    RK = proplists:get_value(routing_key, Config),
    case thumper_utils:bind_exchange(Channel, Destination, Source, RK) of
        {ok, _Binding} ->
            ok;
        E ->
            E
    end;
tx_item('exchange.unbind', Config, #thumper_tx_state{channel=Channel}) ->
    Destination = proplists:get_value(destination, Config),
    Source = proplists:get_value(source, Config),
    RK = proplists:get_value(routing_key, Config),
    case thumper_utils:unbind_exchange(Channel, Destination, Source, RK) of
        {ok, _Binding} ->
            ok;
        E ->
            E
    end;
tx_item('queue.declare', Config, #thumper_tx_state{channel=Channel}) ->
    case thumper_utils:declare_queue(Channel, Config) of
        {ok, _QueueName} ->
            ok;
        E ->
            E
    end;
tx_item('queue.bind', Config, #thumper_tx_state{channel=Channel}) ->
    Queue = proplists:get_value(queue, Config),
    Exchange = proplists:get_value(exchange, Config),
    RK = proplists:get_value(routing_key, Config),
    case thumper_utils:bind_queue(Channel, Queue, Exchange, RK) of
        {ok, _Binding} ->
            ok;
        E ->
            E
    end;
tx_item('queue.delete', Config, #thumper_tx_state{channel=Channel}) ->
    Queue = proplists:get_value(queue, Config),
    thumper_utils:delete_queue(Channel, Queue);
tx_item('queue.unbind', Config, #thumper_tx_state{channel=Channel}) ->
    Queue = proplists:get_value(queue, Config),
    Exchange = proplists:get_value(exchange, Config),
    RK = proplists:get_value(routing_key, Config),
    case thumper_utils:unbind_queue(Channel, Queue, Exchange, RK) of
        {ok, _Binding} ->
            ok;
        E ->
            E
    end;
tx_item('thumper.queue_condition', Config, #thumper_tx_state{rabbitmq_config=RabbitMqConfig}) ->
    Queue = proplists:get_value(queue, Config),
    MessagesCondition = proplists:get_value(messages, Config),
    PollInterval = proplists:get_value(poll_interval, Config, 100),
    Timeout = proplists:get_value(timeout, Config, 1000),
    queue_messages_condition(RabbitMqConfig, Queue, MessagesCondition, PollInterval, Timeout*1000, 0);
tx_item(Item, _Config, _State) ->
    {error, Item, not_implemented}.

queue_messages_condition(_RabbitMqConfig, _Queue, _MessagesCondition,
        _PollInterval, Timeout, ElapsedTime) when ElapsedTime >= Timeout ->
    ?THUMP(error, "'thumper.queue_condition' FAIL timed out at ~p msec", [ElapsedTime div 1000]),
    throw(messages_condition_not_met);

queue_messages_condition(RabbitMqConfig, Queue, MessagesCondition,
        PollInterval, Timeout, ElapsedTime) ->
    Tick = os:timestamp(),
    case rabbitmq_api_queue:messages(RabbitMqConfig, Queue) of
        {ok, MessagesProplist} ->
            Messages = proplists:get_value(messages, MessagesProplist),
            if
                Messages =< MessagesCondition ->
                    ?THUMP(info, "'thumper.queue_condition' PASS messages ~p, condition ~p", [Messages, MessagesCondition]),
                    ok;
                true ->
                    ?THUMP(info, "'thumper.queue_condition' PENDING messages ~p, condition ~p", [Messages, MessagesCondition]),
                    timer:sleep(PollInterval),
                    Tock = os:timestamp(),
                    ThisElapsed = timer:now_diff(Tock, Tick),
                    queue_messages_condition(RabbitMqConfig, Queue,
                        MessagesCondition, PollInterval, Timeout, ElapsedTime + ThisElapsed)
            end;
        Error ->
            ?THUMP(error, "'thumper.queue_condition' FAIL api failure ~p", [Error]),
            throw(api_failure)
    end.

commit_transaction(#thumper_tx_state{channel=Channel}) ->
    case amqp_channel:call(Channel, #'tx.commit'{}) of
        {'tx.commit_ok'} ->
            ok;
        E ->
            {error, E}
    end.

rollback_transaction(#thumper_tx_state{channel=Channel}) ->
    % Note: rollback doesn't work for most operations.
    % e.g. queue declares, binds, etc do not rollback
    % appropriately
    case amqp_channel:call(Channel, #'tx.rollback'{}) of
        {'tx.rollback_ok'} ->
            ok;
        E ->
            {error, E}
    end.

setup(Config, State) ->
    RabbitMqConfig = proplists:get_value(rabbitmq_config, Config),
    case broker_connection:do_connect(thumper_tx, RabbitMqConfig) of
        {ok, Handle=#broker_handle{publish_channel_ref=ChanRef,
                connection_ref=ConnRef,
                connection=Connection}} ->
            ?Demonitor(ChanRef),
            ?Demonitor(ConnRef),
            Handle2 = Handle#broker_handle{publish_channel_ref=undefined,
                connection_ref=undefined},
            case thumper_utils:init_channel(Connection) of
                {ok, Channel} ->
                    {ok, State#thumper_tx_state{
                            rabbitmq_config=RabbitMqConfig,
                            broker_handle=Handle2,
                            channel=Channel}};
                E ->
                    {E, State#thumper_tx_state{
                            rabbitmq_config=RabbitMqConfig,
                            broker_handle=Handle2}}
            end;
        E ->
            {E, State}
    end.

teardown(Config, State=#thumper_tx_state{broker_handle=Handle}) when Handle =/= undefined ->
    broker_connection:close(Handle),
    teardown(Config, State#thumper_tx_state{broker_handle=undefined});
teardown(_Config, State) ->
    {ok, State}.
