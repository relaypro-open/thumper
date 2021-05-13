%% ------------------------------------------------------------------
%% @doc 
%% The queuepool manages a list of queues with active bindings,
%% allowing a client to publish to exactly one queue with
%% determinism.
%%
%% The queuepool uses the RabbitMQ management API to query for queues
%% that meet a given regex pattern and that have at least one binding
%% to an exchange that meets the given regex pattern.
%%
%% Publishes to the queuepool require a serialization id, which is
%% used to choose among the pool of bindings. The choice is provided
%% to client client via a callback.
%% @end
%% ------------------------------------------------------------------
-module(queuepool).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-include("../include/thumper.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/6]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([set_bindings/2, publish/6]).

-export([binding/2]).

-record(qpstate, {broker,
                config,
                queue_pattern,
                src_exchange_pattern,
                publisher,
                opts,
                bindings,
                returns}).
-record(binding, {source, queue, routing_key}).

-define(MaxNow, {99999,0,0}). %% {{5138,11,4},{20,0,0}}

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% @doc 
%% Ref
%% gen_server reference (e.g. a unique atom name)
%% 
%% Broker
%% the atom that refers to the broker config used to access
%% the RabbitMQ management API. Publishes can be to a
%% different broker, if desired, in the callback.
%%
%% QueuePattern
%% regex pattern that matches a queue name that should
%% be included in the pool
%%
%% SrcExchangePattern
%% regex pattern than matches an exchange name
%% that has a binding to the given queue
%%
%% Publisher
%% an atom that refers to a module that implements the
%% necessary callbacks (note Binding can be undefined)
%%    make_route_to_binding(Id, Binding, Message, Exchange, RoutingKey, Opts)
%%    publish_to_binding(Id, Binding, Message, Exchange, RoutingKey, Opts)
%%
%% Opts
%% options, including
%%      returns_expiration          - maximum age for a return callback
%%      query_bindings_interval     - interval used to check for new bindings
%%      returns_expiration_interval - interval used to check for old return callbacks
%%      filter_bindings             - function used to filter the bindings list
%%      partition_bindings          - function used to split bindings up into distinct
%%                                    pools. The queuepool will publish to each partition
%% @end
%% ------------------------------------------------------------------
start_link(Ref,
          Broker,
          QueuePattern,
          SrcExchangePattern,
          Publisher,
          Opts) ->
    gen_server:start_link({local, Ref}, ?MODULE, [Broker,
            QueuePattern, SrcExchangePattern, Publisher, Opts], []).

%% ------------------------------------------------------------------
%% @doc 
%% Ref
%% gen_server reference (e.g. a unique atom name)
%% 
%% Id
%% Serialization id for choosing binding, any term
%%
%% Message
%% The binary payload to be published
%%
%% Exchange
%% Default exchange to use (in case there's no binding)
%%
%% RoutingKey
%% Default routing key to use (in case there's no binding)
%%
%% Opts
%% Unused by queuepool, but provided to the callback
%% @end
%% ------------------------------------------------------------------
publish(Ref, Id, Message, Exchange, RoutingKey, Opts) ->
    gen_server:call(Ref, {publish, [Id, Message, Exchange, RoutingKey, Opts]}, infinity).

binding(queue, #binding{queue=Queue}) -> Queue;
binding(source, #binding{source=Source}) -> Source;
binding(routing_key, #binding{routing_key=RK}) -> RK;
binding(_, _) -> undefined.

set_bindings(Ref, Bindings) ->
    gen_server:cast(Ref, {set_bindings, [Bindings]}).

init([Broker, QueuePattern, SrcExchangePattern, Publisher, Opts]) ->
    schedule_bindings_check(Opts),
    schedule_returns_expiration(Opts),
    {ok, #qpstate{broker=Broker,
        config=mgmt_config(Broker),
        queue_pattern=QueuePattern,
        src_exchange_pattern=SrcExchangePattern,
        publisher=Publisher,
        returns=gb_trees:empty(),
        opts=Opts}}.

mgmt_config(Broker) ->
    case broker_connection:rabbitmq_config(Broker) of
        undefined ->
            undefined;
        Config ->
            rabbitmq_api:to_admin_config(Config)
    end.

map_id_to_bindings(Id, #qpstate{bindings=Bindings}) ->
    orddict:fold(fun(_PartitionKey, PartitionBindings, Acc) ->
                case map_id_to_binding(Id, PartitionBindings) of
                    {ok, B} ->
                        [B|Acc];
                    _ ->
                        [undefined|Acc]
                end
        end, [], Bindings).

map_id_to_binding(_Id, undefined) ->
    {error, no_bindings};
map_id_to_binding(_Id, []) ->
    {error, no_bindings};
map_id_to_binding(Id, Bindings) ->
    N = length(Bindings),
    Pos = erlang:phash2(Id, N)+1,
    {ok, lists:nth(Pos, Bindings)}.

handle_call({publish, Args}, _From, State=#qpstate{bindings=undefined}) ->
    State2 = do_bindings_check(State),
    handle_call({publish, Args}, _From, State2);
handle_call({publish, Req=[Id, _Message, _Exchange, _RoutingKey, _Opts]}, _From, State=#qpstate{}) ->
    Bindings = map_id_to_bindings(Id, State),
    {Reply, State2} = lists:foldl(fun(Binding, {Res, StateIn}) ->
                    {Res2, St2} = publish_1_binding(Req, Binding, StateIn),
                    {[Res2|Res], St2}
            end, {[], State}, Bindings),
    {reply, Reply, State2};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

publish_1_binding([Id, Message, Exchange, RoutingKey, Opts], undefined, State=#qpstate{publisher=Publisher}) ->
    Res = publisher_publish_to_binding(Publisher, Id, undefined, Message, Exchange, RoutingKey, Opts),
    {Res, State};
publish_1_binding([Id, Message, Exchange, RoutingKey, Opts], Binding, State=#qpstate{broker=Broker, returns=Returns, publisher=Publisher}) ->
    case publisher_make_route_to_binding(Publisher, Id, Binding, Message, Exchange, RoutingKey, Opts) of
        {ActualExchange, ActualRoutingKey} when ActualExchange =/= error ->
            Now = os:timestamp(),
            thumper:register_return_callback(Broker, ActualExchange, ActualRoutingKey,
                fun({_X, _RK, _Reason}, _Content) ->
                        thumper:unregister_return_callback(Broker, ActualExchange, ActualRoutingKey),
                        publisher_publish_to_binding(Publisher, Id, undefined, Message, Exchange, RoutingKey, Opts)
                end),
            Returns2 = aged_gb_trees:insert(Now, {ActualExchange, ActualRoutingKey}, Returns),
            State_ = State#qpstate{returns=Returns2},
            Res = publisher_publish_to_binding(Publisher, Id, Binding, Message, ActualExchange, ActualRoutingKey, Opts),
            {Res, State_};
        Err={error,_} ->
            {Err, State};
        Err ->
            {{error, Err}, State}
    end.

num_bindings(Bindings) ->
    length(lists:flatten(element(2, lists:unzip(orddict:to_list(Bindings))))).

handle_cast({set_bindings, [Bindings]}, State=#qpstate{queue_pattern=QPat}) ->
    ?THUMP(info, "detected change in bindings for queue pattern ~p, we now have ~p bindings among ~p partitions",
        [QPat, num_bindings(Bindings), orddict:size(Bindings)]),
    {noreply, State#qpstate{bindings=Bindings}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(bindings_check, State=#qpstate{bindings=OrigBindings, opts=Opts}) ->
    Pid = self(),
    spawn(fun() ->
                #qpstate{bindings=NewBindings} = do_bindings_check(State),
                if
                    OrigBindings =/= NewBindings ->
                        set_bindings(Pid, NewBindings);
                    true ->
                        ok
                end
        end),
    schedule_bindings_check(Opts),
    {noreply, State};
handle_info(expire_returns, State=#qpstate{broker=Broker, returns=Returns, opts=Opts}) ->
    Now = os:timestamp(),
    Expiry = proplists:get_value(returns_expiration, Opts, timer:seconds(3)),
    ExpirationTime = aged_gb_trees:micros_to_timestamp(timer:now_diff(Now, {0, 0, Expiry*1000})),
    Iter = aged_gb_trees:iterator_before(ExpirationTime, Returns),
    Tick = os:timestamp(),
    aged_gb_trees:iter_foreach(fun({_K, {Exchange, RoutingKey}}) ->
                thumper:unregister_return_callback(Broker, Exchange, RoutingKey)
        end, Iter),
    Size1 = aged_gb_trees:size(Returns),
    Returns2 = aged_gb_trees:iter_remove(Iter, Returns),
    Size2 = aged_gb_trees:size(Returns2),
    Tock = os:timestamp(),
    if
        Size2 < Size1 ->
            ?THUMP(debug, "queuepool pruned ~p return callbacks (~p usec)", [Size1-Size2, timer:now_diff(Tock, Tick)]);
        true ->
            ok
    end,
    schedule_returns_expiration(Opts),
    {noreply, State#qpstate{returns=Returns2}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ----------- Callbacks ----------------------------------------
publisher_make_route_to_binding(Publisher, Id, Binding, Message, Exchange, RoutingKey, Opts) ->
    try Publisher:make_route_to_binding(Id, Binding, Message, Exchange, RoutingKey, Opts) of
        Res ->
            Res
        catch _:_ ->
            ?THUMP(error, "caught exception calling make_route_to_binding ~p", [Id]),
            ?THUMP(error, "~p", [erlang:get_stacktrace()]),
            {error, exception}
    end.

publisher_publish_to_binding(Publisher, Id, Binding, Message, Exchange, RoutingKey, Opts) ->
    try Publisher:publish_to_binding(Id, Binding, Message, Exchange, RoutingKey, Opts) of
        Res ->
            Res
        catch _:_ ->
            ?THUMP(error, "caught exception calling publish_to_binding ~p", [Id]),
            ?THUMP(error, "~p", [erlang:get_stacktrace()]),
            {error, exception}
    end.
%% --------------------------------------------------------------

do_bindings_check(State=#qpstate{config=Config, queue_pattern=QPat,
        src_exchange_pattern=SrcPat, opts=Opts}) ->
    case rabbitmq_api:find_queue_bindings(Config, QPat, SrcPat) of
        {ok, Bindings} ->
            ?THUMP(debug, "checking for new bindings with pattern ~p", [QPat]),
            Bindings2 = lists:map(fun({struct, B}) ->
                        Src = proplists:get_value(<<"source">>, B),
                        Queue = proplists:get_value(<<"destination">>, B),
                        RK = proplists:get_value(<<"routing_key">>, B),
                        #binding{source=Src, queue=Queue, routing_key=RK}
                end, Bindings),
            Filter = proplists:get_value(filter_bindings, Opts, fun(_) -> true end),
            Partition = proplists:get_value(partition_bindings, Opts, fun(_) -> undefined end),
            Bindings3 = lists:filter(Filter, lists:usort(Bindings2)),
            MapPartitions = lists:zip(lists:map(Partition, Bindings3), Bindings3),
            Bindings4 = lists:foldl(fun({Key, Binding}, Dict) ->
                        orddict:append_list(Key, [Binding], Dict)
                end, orddict:new(), MapPartitions),
            ?THUMP(debug, "found ~p bindings total, among ~p paritions", [length(Bindings3), orddict:size(Bindings4)]),
            State#qpstate{bindings=Bindings4};
        Err ->
            ?THUMP(error, "failed to get bindings for ~p ~p: ~p", [QPat, SrcPat, Err]),
            % Important: make sure bindings is a list, not undefined
            Bindings2 = case State#qpstate.bindings of undefined -> orddict:new(); _ -> State#qpstate.bindings end,
            State#qpstate{bindings=Bindings2}
    end.

schedule_bindings_check(Opts) ->
    Timeout = proplists:get_value(query_bindings_interval, Opts, timer:minutes(5)),
    erlang:send_after(Timeout, self(), bindings_check).

schedule_returns_expiration(Opts) ->
    Timeout = proplists:get_value(returns_expiration_interval, Opts, timer:seconds(5)),
    erlang:send_after(Timeout, self(), expire_returns).
