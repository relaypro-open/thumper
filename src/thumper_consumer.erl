-module(thumper_consumer).
-behaviour(gen_server).

-export([start_link/4, start_link/5, stop/1]).

-export([get_state/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3]).

-define(BatchDef, 1).
-define(RecvTimeoutDef, infinity).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("../include/thumper.hrl").

-define(ST, #{broker=> undefined,
             channel=> undefined,
             consumer_tag=> undefined,
             queue=> undefined,
             consume_arguments=> [],
             max_batch_size=> ?BatchDef,
             receive_timeout=> ?RecvTimeoutDef,
             callback=> undefined,
             batch=> {[], 0},
             thumper_svr_mon=> undefined}).

-define(ThumperWait, 5000).

start_link(SrvRef, Broker, QueueName, Callback) ->
    start_link(SrvRef, Broker, QueueName, Callback, []).

start_link(undefined, Broker, QueueName, Callback, Opts) ->
    gen_server:start_link(?MODULE, [Broker, QueueName, Callback, Opts], []);
start_link(SrvRef, Broker, QueueName, Callback, Opts) ->
    gen_server:start_link({local, SrvRef}, ?MODULE, [Broker, QueueName, Callback, Opts], []).

stop(SrvRef) ->
    gen_server:cast(SrvRef, stop).

get_state(SrvRef, Timeout) ->
    case sys:get_state(SrvRef, Timeout) of
        St=#{} ->
            maps:to_list(St);
        _ ->
            {error, bad_state}
    end.

init([Broker, QueueName, Callback, Opts]) ->
    Self = self(),
    spawn(fun() -> thumper_loop(Self, Broker) end),
    State = ?ST,
    {ok, State#{broker => Broker,
            queue => QueueName,
            consume_arguments => proplists:get_value(consume_arguments, Opts, []),
            max_batch_size => proplists:get_value(max_batch_size, Opts, ?BatchDef),
            receive_timeout => proplists:get_value(recv_timeout, Opts, ?RecvTimeoutDef),
            callback => Callback}}.

handle_call(_Req, _From, St=#{receive_timeout := Timeout}) ->
    {reply, ok, St, Timeout}.

handle_cast({thumper_svr, Pid}, St=#{broker := Broker, queue := Queue, consume_arguments := Args, receive_timeout := Timeout}) ->
    Self = self(),
    case is_process_alive(Pid) of
        true ->
            try thumper:subscribe_external(Broker, Queue, Args, Self) of
                {ok, {Channel, ConsumerTag}} ->
                    ?THUMP(info, "consumer for queue ~p detected broker ~p is up, tag ~p", [Queue, Broker, ConsumerTag]),
                    Ref = erlang:monitor(process, Pid),
                    {noreply, St#{channel => Channel,
                                    consumer_tag => ConsumerTag,
                                    thumper_svr_mon => Ref}, Timeout};
                R ->
                    ?THUMP(error, "unexpected subscribe result ~p for queue ~p on broker ~p", [R, Queue, Broker]),
                    spawn(fun() -> timer:sleep(?ThumperWait), thumper_loop(Self, Broker) end),
                    {noreply, St#{thumper_svr_mon => undefined}, Timeout}
                catch _:_:ST ->
                    ?THUMP(error, "caught exception subscribing ~p", [ST]),
                    spawn(fun() -> timer:sleep(?ThumperWait), thumper_loop(Self, Broker) end),
                    {noreply, St#{thumper_svr_mon => undefined}, Timeout}
            end;
        _ ->
            spawn(fun() -> thumper_loop(Self, Broker) end),
            {noreply, St#{thumper_svr_mon => undefined}, Timeout}
    end;
handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({'DOWN', Ref, process, _Pid, _Reason}, St=#{broker := Broker,
                                                          queue := Queue,
                                                          receive_timeout := Timeout,
                                                          thumper_svr_mon := Ref}) ->
    ?THUMP(warning, "consumer for queue ~p detected broker ~p is down", [Queue, Broker]),
    Self = self(),
    spawn(fun() -> thumper_loop(Self, Broker) end),
    {noreply, St#{thumper_svr_mon => undefined}, Timeout};

handle_info(#'basic.consume_ok'{}, St=#{receive_timeout := Timeout}) ->
    {noreply, St, Timeout};

handle_info(#'basic.cancel'{}, St) ->
    {stop, normal, St};

handle_info(#'basic.cancel_ok'{}, St=#{receive_timeout := Timeout}) ->
    % see thumper:unsubscribe_wait
    receive
        unsubscribed ->
            {stop, normal, St}
    after
        100 ->
            {noreply, St, Timeout}
    end;

handle_info({#'basic.deliver'{consumer_tag=ConsumerTag,
                             delivery_tag=DeliveryTag,
                             routing_key=RoutingKey},
                         #amqp_msg{payload=Payload}}, St=#{consumer_tag := ConsumerTag,
                                                             batch := {Batch, Size},
                                                             max_batch_size := MaxBatchSize,
                                                             receive_timeout := Timeout}) ->
    Batch2 = [{DeliveryTag, RoutingKey, Payload}|Batch],
    BatchSize2 = Size+1,
    St2 = St#{batch => {Batch2, BatchSize2}},
    if
        BatchSize2 >= MaxBatchSize ->
            {noreply, consume_batch(St2), Timeout};
        true ->
            {noreply, St2, Timeout}
    end;

handle_info({new_channel, NewChannel}, St=#{broker := Broker,
                                              queue := Queue,
                                              channel := Channel,
                                              receive_timeout := Timeout}) ->
    ?THUMP(info, "consumer for queue ~p received new channel from ~p (channel ~p -> ~p)",
        [Queue,
         Broker,
         Channel,
         NewChannel]),
    {noreply, St#{channel := NewChannel}, Timeout};

handle_info({new_consumer_tag, NewConsumerTag}, St=#{broker := Broker,
                                                    queue := Queue,
                                                    consumer_tag := ConsumerTag,
                                                    receive_timeout := Timeout}) ->
    ?THUMP(info, "consumer for queue ~p received new consumer tag from ~p (tag ~p -> ~p)",
        [Queue,
         Broker,
         ConsumerTag,
         NewConsumerTag]),
    {noreply, St#{consumer_tag => NewConsumerTag}, Timeout};

handle_info({new_callback, NewCallback}, St=#{receive_timeout := Timeout}) ->
    % This clause is executed if the thumper svr sends
    % a new callback to the subscriber. Could probably be
    % removed. Much more convenient to set a new callback
    % via this gen_server directly.
    {noreply, St#{callback => NewCallback}, Timeout};

handle_info(timeout, St=#{receive_timeout := Timeout}) ->
    {noreply, consume_batch(St), Timeout};
handle_info(_Info, St=#{receive_timeout := Timeout}) ->
    ?THUMP(debug, "unhandled info ~p", [_Info]),
    ?THUMP(debug, "with state ~p", [St]),
    {noreply, St, Timeout}.

terminate(_Reason, _St) ->
    ok.

code_change(_OldVsn, St, _Extras) ->
    {ok, St}.

thumper_loop(RecvPid, Broker) ->
    try thumper_loop_(RecvPid, Broker) of
            ok ->
                ok;
            _ ->
                timer:sleep(?ThumperWait),
                thumper_loop(RecvPid, Broker)
        catch _:_ ->
            timer:sleep(?ThumperWait),
            thumper_loop(RecvPid, Broker)
    end.

thumper_loop_(RecvPid, Broker) ->
    case whereis(thumper_utils:get_thumper_svr(Broker)) of
        undefined ->
            {error, no_svr};
        Pid when is_pid(Pid) ->
            gen_server:cast(RecvPid, {thumper_svr, Pid}),
            ok
    end.

consume_batch(St=#{batch := {[], _}}) ->
    St;
consume_batch(St=#{channel := Channel,
                     callback := Callback,
                     batch := {Batch, Size}}) ->
    {DeliveryTag, RoutingKey, Payload} = lists:last(Batch),
    try make_callback_batch(Callback, Batch) of
        ack ->
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag=DeliveryTag});
        {ack, Multiple} ->
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag=DeliveryTag, multiple=Multiple});
        nack ->
            amqp_channel:cast(Channel, #'basic.nack'{delivery_tag=DeliveryTag, requeue=false});
        {nack, Multiple, Requeue} when is_boolean(Multiple), is_boolean(Requeue) ->
            amqp_channel:cast(Channel, #'basic.nack'{delivery_tag=DeliveryTag, multiple=Multiple, requeue=Requeue});
        Response ->
            ?THUMP(info, "unknown callback response ~p~n routing_key: ~p~n payload: ~p batch size: ~p",
                [Response, RoutingKey, Payload, Size]),
            ok
        catch X:Y:Stacktrace ->
            ?THUMP(error, "caught exception during message callback: ~p ~p~nStacktrace: ~p~nrouting_key: ~p~n payload: ~p batch size: ~p",
                [X, Y, Stacktrace,RoutingKey, Payload, Size]),
            ok
    end,
    St#{batch => {[], 0}}.

make_callback_batch(Callback, [{DT, RK, Payload}]) ->
    make_callback(Callback, DT, RK, Payload);
make_callback_batch({M, F, A}, Batch) ->
    apply(M, F, lists:append([Batch], A));
make_callback_batch(Callback, Batch) when is_function(Callback) ->
    Callback(Batch);
make_callback_batch(_, _) ->
    {error, invalid_callback}.

make_callback(Callback, DeliveryTag, RoutingKey, Payload) when is_function(Callback) ->
    Callback(DeliveryTag, RoutingKey, Payload);
make_callback({M, F}, DeliveryTag, RoutingKey, Payload) ->
    apply(M, F, [DeliveryTag, RoutingKey, Payload]);
make_callback({M, F, A}, DeliveryTag, RoutingKey, Payload) when is_list(A) ->
    Args = lists:append([DeliveryTag, RoutingKey, Payload], A),
    apply(M, F, Args);
make_callback(_, _, _, _) ->
    {error, invalid_callback}.
