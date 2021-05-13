-module(thumper).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("../include/thumper.hrl").

-define(DefaultConnectionConfig, "rabbitmq.config").
-define(RECONNECT_DELAY, 5000).

-define(ReconnectCloseCode, 204).
-define(ReconnectCloseText, <<"client_reconnect_requested">>).

-define(ReturnCallbacksMax, 10000).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
        ensure/1,
        start_link/0, start_link/1,
        publish/3, publish/4, publish_to/4, publish_to/5,
        cast_publish/3, cast_publish_to/4,
        register_return_callback/4, unregister_return_callback/3, def_return_callback/1,
        subscribe/4, subscribe/5, subscribe_link/4, subscribe_link/5,
        subscribe_external/4,
        unsubscribe/1, unsubscribe/2,
        get_state/0, get_state/1, get_state/2,
        recover/0, recover/1,
        reconnect/0, reconnect/1,
        declare_exchange/1, declare_exchange/2,
        set_callback/2,
        diag/0, diag/1
    ]).

-export([find_subscriber_channel/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

%% Deprecated
start_link() ->
    start_link(default).

start_link(ConfigName) ->
    gen_server:start_link({local, thumper_utils:get_thumper_svr(ConfigName)}, ?MODULE, [ConfigName], []).

ensure(ConfigName) ->
    thumper_dynamic_brokers:ensure(ConfigName).

%% Deprecated
publish(Message, ExchangeName, RoutingKey) when is_list(ExchangeName) ->
    publish(Message, list_to_binary(ExchangeName), RoutingKey);
publish(Message, ExchangeName, RoutingKey) when is_binary(ExchangeName) ->
    publish_to(default, Message, ExchangeName, RoutingKey, []).

%% Deprecated
cast_publish(Message, ExchangeName, RoutingKey) when is_list(ExchangeName) ->
    cast_publish(Message, list_to_binary(ExchangeName), RoutingKey);
cast_publish(Message, ExchangeName, RoutingKey) when is_binary(ExchangeName) ->
    cast_publish_to(default, Message, ExchangeName, RoutingKey).

%% Deprecated
publish(Message, ExchangeName, RoutingKey, MessageOptions) when is_list(ExchangeName) ->
    publish(Message, list_to_binary(ExchangeName), RoutingKey, MessageOptions);
publish(Message, ExchangeName, RoutingKey, MessageOptions) ->
    publish_to(default, Message, ExchangeName, RoutingKey, MessageOptions).

publish_to(ConfigName, Message, ExchangeName, RoutingKey) ->
    publish_to(ConfigName, Message, ExchangeName, RoutingKey, []).

publish_to(ConfigName, Message, ExchangeName, RoutingKey, MessageOptions) ->
    Request = {publish, Message, ExchangeName, RoutingKey, MessageOptions},
    broker_call(ConfigName, Request).

normalize_message_options(Boolean) when is_boolean(Boolean) ->
    %% Backward compatibility
    ?THUMP(debug, "please change your calls to thumper:publish and thumper:publish_to to use the proplist message options [{mandatory, boolean()}]", []),
    [{mandatory, Boolean}];
normalize_message_options(Opts) when is_list(Opts) ->
    Opts;
normalize_message_options(_) ->
    [].

cast_publish_to(ConfigName, Message, ExchangeName, RoutingKey) ->
    Request = {publish, Message, ExchangeName, RoutingKey, []},
    broker_cast(ConfigName, Request).

%% Register a callback either 1-arity function or MFA-style, that will
%% be executed upon the basic.return of a message that matches the given
%% routing key. Such messages must be published with
%% message option mandatory==true to engage the callback.
register_return_callback(ConfigName, Exchange, RoutingKey, Callback) ->
    Request = {register_return_callback, Exchange, RoutingKey, Callback},
    broker_cast(ConfigName, Request).

%% Unregister a return callback. (see register_return_callback)
unregister_return_callback(ConfigName, Exchange, RoutingKey) ->
    Request = {unregister_return_callback, Exchange, RoutingKey},
    broker_cast(ConfigName, Request).

%% A default return callback, which can be used as an MFA-style
%% return callback: e.g.
%% register_return_callback(default, <<"X">>, <<"RK">>, {thumper, def_return_callback, []}).
def_return_callback({Exchange, RoutingKey, Reason}) ->
    ?THUMP(warning, "return callback, exchange=~p, routing_key=~p, reason=~p", [Exchange, RoutingKey, Reason]).

%%
%% Subscribe a callback function that will always have the following 
%% values passed as the first three arguments (in this order):
%%
%%  DeliveryTag, RoutingKey, Payload
%%
%% Multiple forms of Callback are allowed:
%%
%% {M, F} - implies fun M:F/3
%%
%% {M, F, A} when is_list(A) - implies fun M:F/N where N is length(A) + 3
%%
%% F when is_function(F) - can be exported (fun M:F/3) or anonymous
%%
%% WARNING: Using an anonymous function is dangerous if you plan to reload
%% the module that declared it.  This will cause the subscription to
%% fail.
%%

%% Deprecated
subscribe(ExchangeName, QueueName, RoutingKey, Callback) ->
    call_subscribe_link(default, ExchangeName, QueueName, RoutingKey, Callback, false).

subscribe(BrokerName, ExchangeName, QueueName, RoutingKey, Callback) ->
    call_subscribe_link(BrokerName, ExchangeName, QueueName, RoutingKey, Callback, false).

subscribe_external(BrokerName, QueueName, Args, Pid) ->
    broker_call(BrokerName, {subscribe_external, QueueName, Args, Pid}).

%% Deprecated
subscribe_link(ExchangeName, QueueName, RoutingKey, Callback) ->
    subscribe_link(default, ExchangeName, QueueName, RoutingKey, Callback).

subscribe_link(BrokerName, ExchangeName, QueueName, RoutingKey, Callback) ->
    call_subscribe_link(BrokerName, ExchangeName, QueueName, RoutingKey, Callback, true).

%% Deprecated
unsubscribe(SubscriptionPid) ->
    unsubscribe(default, SubscriptionPid).

unsubscribe(BrokerName, SubscriptionPid) ->
    gen_server:call(thumper_utils:get_thumper_svr(BrokerName), {unsubscribe, SubscriptionPid}).

%% Deprecated
reconnect() ->
    reconnect(default).

% Reconnects the broker to RabbitMQ, starting with a reload of the default config.
reconnect(BrokerName) ->
    gen_server:cast(thumper_utils:get_thumper_svr(BrokerName), reconnect).

%% Deprecated
declare_exchange(ConfigProplist) ->
    declare_exchange(default, ConfigProplist).

declare_exchange(BrokerName, ConfigProplist) ->
    gen_server:call(thumper_utils:get_thumper_svr(BrokerName), {declare_exchange, ConfigProplist}).

%% Deprecated
get_state() ->
    get_state(default).

get_state(BrokerName) ->
    get_state(BrokerName, undefined).

get_state(BrokerName, BrokerType) -> % BrokerType :: default | undefined
    do_get_state(BrokerName, BrokerType).

%% Deprecated
recover() ->
    recover(default).

recover(false) ->
    ?THUMP(error, "Sorry, RabbitMQ does not support recover with requeue=false.", []),
    {error, unimplemented};
recover(true) ->
    recover(default);
recover(BrokerName) ->
    gen_server:call(thumper_utils:get_thumper_svr(BrokerName), {recover, true}).

set_callback(SubscriberPid, Callback) ->
    SubscriberPid ! {new_callback, Callback}.

%% Deprecated
diag() ->
    diag(default).

diag(BrokerName) ->
    case do_get_state(BrokerName, undefined) of
        #state{}=State ->
            ?THUMP(info, "~nthumper state: ~n~p~n", [State]);
        _ ->
            ?THUMP(info, "~nthumper state not available~n", [])
    end,
    dump_table(?SubscriberTableName),
    dump_table(?ChannelTableName).

find_subscriber_channel(Broker, QueueName) ->
    case ets:select(?SubscriberTableName, [{
                #subscriber{broker_name='$1', queue_name='$2', _='_'},
                [{'=:=', '$1', Broker}, {'=:=', '$2', QueueName}],
                ['$_']
            }]) of
        [#subscriber{channel=Channel}] ->
            {ok, Channel};
        [] ->
            {error, not_found};
        Subs ->
            {error, multiple, [X||#subscriber{channel=X}<-Subs]}
    end.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([ConfigName]) ->

    process_flag(trap_exit, true),

    JournalOptsPL = case application:get_env(thumper, queuejournal) of
                            {ok, Proplist} ->
                                Proplist;
                            _ ->
                                ?QueueJournalOptsDefault
                       end,
    
    case init_queuejournal_opts(ConfigName, JournalOptsPL) of
        {error, Reason} ->
            {stop, {queuejournal, Reason}};

        QueueJournalOpts ->

            InitialState = #state{broker_name=ConfigName,
                                  subscriber_tid=?SubscriberTableName,
                                  channel_tid=?ChannelTableName},

            HaveSubscribers = evaluate_subscriber_state(ConfigName, InitialState),

            case broker_connection:open(ConfigName) of
                {ok, BrokerHandle} ->

                    % Flush any outstanding messages from disk
                    {ok, {NewQueue, NewQueueJournalOpts}} = 
                        flush_publish_queue(BrokerHandle, array:new(), QueueJournalOpts),

                    State = InitialState#state{
                              broker_handle=BrokerHandle,
                              status=available,
                              publish_queue=NewQueue,
                              queuejournal_opts=NewQueueJournalOpts},

                    case HaveSubscribers of
                        true ->
                            case resubscribe_all(State) of
                                ok ->
                                    {ok, State};
                                {error, Errors} ->
                                    ?THUMP(error, "~p init resubscribe errors ~p", [ConfigName, Errors]),
                                    State = schedule_reconnect(State#state{status=unavailable}),
                                    {ok, State}
                            end;
                        _ ->
                            {ok, State}
                    end;

                {error, conn_error} ->
                    ?THUMP(warning, "default connection failed, reconnecting", []),
                    State = schedule_reconnect(InitialState),
                    {ok, State#state{queuejournal_opts=QueueJournalOpts}};
                {error, config_error} ->
                    ?THUMP(warning, "detected config error, reconnecting", []),
                    State = schedule_reconnect(InitialState),
                    {ok, State#state{queuejournal_opts=QueueJournalOpts}};
                {error, Error} ->
                    {stop, Error}
            end
    end.

% If we died with reason kill, the channel table needs to be cleaned 
% before we can resubscribe everyone.  We may also have been terminated
% by thumper_sup, which will leave an empty channel table with a non-empty
% subscriber table.  In any case, returns true if subscribers were found.
evaluate_subscriber_state(BrokerName, #state{subscriber_tid=SubscriberTableName, channel_tid=ChannelTableName}) ->
    ChannelCount = count_channels(ChannelTableName, BrokerName),
    case ChannelCount of
        undefined ->
            false;
        Size ->
            SubCount = case count_subscribers(SubscriberTableName, BrokerName) of
                Size ->
                    ?THUMP(info, "found ~p existing subscribers", [Size]),
                    Size;
                Size2 ->
                    ?THUMP(warning, "table size inconsistency: channels ~p subscribers ~p", [Size, Size2]),
                    Size2
            end,
            case SubCount of
                0 ->
                    false;
                _ ->
                    Channels = take_channels(ChannelTableName, BrokerName),
                    clean_channel(Channels, SubscriberTableName, ChannelTableName),
                    true
            end
    end.

take_channels(ChannelTableName, BrokerName) ->
    Select = [{
            {'$1', '$2', '$3'},
            [{'=:=', '$3', BrokerName}],
            [{{'$1', '$2'}}]
        }],

    % The select takes the first and second elements from the tuple
    % which is the channel pid and subscriber pid. See clean_channel
    Channels = ets:select(ChannelTableName, Select),

    Delete = {'_', '_', BrokerName},
    ets:match_delete(ChannelTableName, Delete),

    Channels.

count_channels(ChannelTableName, BrokerName) ->
    ets:select_count(ChannelTableName, [{
                {'_', '_', '$1'},
                [{'=:=', '$1', BrokerName}],
                [true]
            }]).

count_subscribers(SubscriberTableName, BrokerName) ->
    ets:select_count(SubscriberTableName, [{
                #subscriber{broker_name='$1', _='_'},
                [{'=:=', '$1', BrokerName}],
                [true]
            }]).

clean_channel([], _, _) ->
    ok;
clean_channel([E={ChannelPid, SubPid}|R], SubscriberTableName, ChannelTableName) ->
    case ?IsPidAlive(ChannelPid) of
        true ->
            case ets:lookup(SubscriberTableName, SubPid) of
                [] ->
                    ?THUMP(error, "no subscriber for channel table entry: ~p", [E]);
                [#subscriber{ref=Ref}=Sub] ->
                    ?THUMP(info, "closing defunct channel: ~p", [ChannelPid]),
                    close_subscriber_channel(SubscriberTableName, ChannelTableName, Sub),
                    % The previous thumper process should have created a reference to this subscriber.
                    ?Demonitor(Ref)
            end;
        _ ->
            ok
    end,
    ets:delete(ChannelTableName, ChannelPid),
    clean_channel(R, SubscriberTableName, ChannelTableName).

init_queuejournal_opts(ConfigName, QueueJournalOpts) ->
    % Ensure that the journal directory exists
    BaseDir = proplists:get_value(dir, QueueJournalOpts, ?QueueJournalDirDefault),
    BrokerDir = BaseDir ++ "/" ++ ?Str(ConfigName),
    DR = case filelib:ensure_dir(BrokerDir ++ "/") of
        ok ->
            ok;
        {error, eexist} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end,

    case DR of 
        {error, Reason2} ->
            {error, Reason2};
        ok ->
            Enabled = proplists:get_value(enabled, QueueJournalOpts, true),
            case Enabled of
                true ->
                    clear_basedir_files(BaseDir);
                _ ->
                    ok
            end,
            % Ensure that the options proplist contains the check flag
            lists:foldl(fun(T={Key, _}, Opts) ->
                        lists:keystore(Key, 1, Opts, T)
                end, QueueJournalOpts,
                [{check_journal, true},
                 {enabled, Enabled},
                 {dir, BrokerDir}])
    end.

clear_basedir_files(BaseDir) ->
    try filelib:fold_files(BaseDir, ".*",
        false, % Do not recurse
        fun(X, A) ->
                case filelib:is_file(X) of
                    true ->
                        file:delete(X),
                        [X|A];
                    _ ->
                        A
                end
        end, [])
    catch Class:Reason ->
        ?THUMP(warning, "failed to clear base dir files with exception ~p:~p",
            [Class, Reason])
    end.

handle_call({unsubscribe, SubscriptionPid}, _From, State=#state{subscriber_tid=SubTid, channel_tid=ChanTid}) ->
    Reply = do_unsubscribe(SubscriptionPid, SubTid, ChanTid),
    {reply, Reply, State};

handle_call({subscribe_link, ExchangeName, QueueName, RoutingKey, Callback}, _From, 
        State=#state{broker_name=Name, broker_handle=BrokerHandle, subscriber_tid=SubTid, channel_tid=ChanTid}) ->
    Subscriber = #subscriber{broker_name=Name,
                             exchange_name=ExchangeName,
                             queue_name=QueueName,
                             routing_key=RoutingKey,
                             queue_config=[{queue, QueueName}],
                             create=false},
    Reply = do_subscribe_link(undefined, BrokerHandle, Subscriber, Callback, SubTid, ChanTid),
    {reply, Reply, State};

handle_call({subscribe_link_with_create, ExchangeName, QueueConfig, RoutingKey, Callback}, _From, 
        State=#state{broker_name=Name, broker_handle=BrokerHandle, subscriber_tid=SubTid, channel_tid=ChanTid}) ->
    QueueName = proplists:get_value(queue, QueueConfig),
    Subscriber = #subscriber{broker_name=Name,
                             exchange_name=ExchangeName,
                             queue_name=QueueName,
                             routing_key=RoutingKey,
                             queue_config=QueueConfig,
                             create=true},
    Reply = do_subscribe_link_with_create(BrokerHandle, Subscriber, Callback, SubTid, ChanTid),
    {reply, Reply, State};

handle_call({subscribe_external, QueueName, Args, Pid}, _From, State) ->
    Name = State#state.broker_name,
    Subscriber = #subscriber{broker_name=Name,
                             queue_name=QueueName,
                             queue_config=[{queue, QueueName}, {arguments, Args}],
                             pid=Pid,
                             create=false},
    BrokerHandle = State#state.broker_handle,
    SubTid = State#state.subscriber_tid,
    ChanTid = State#state.channel_tid, 
    Connection = BrokerHandle#broker_handle.connection,
    case thumper_utils:init_channel(Connection) of
        {ok, Channel} ->
            case do_subscribe(Channel, Subscriber, Pid) of
                {ok, ConsumerTag} ->
                    Ref = ?Monitor(Pid),
                    NewSubscriber = Subscriber#subscriber{ref=Ref,
                                                          channel=Channel,
                                                          consumer_tag=ConsumerTag},
                    add_subscriber(SubTid, ChanTid, NewSubscriber),
                    {reply, {ok, {Channel, ConsumerTag}}, State};
                {error, SubError} ->
                    {reply, {error, SubError}, State}
            end;
        {error, ChanError} ->
            {reply, {error, ChanError}, State}
    end;

handle_call({declare_exchange, ConfigProplist}, _From, State=#state{broker_handle=BrokerHandle}) ->
    Channel = BrokerHandle#broker_handle.publish_channel,
    case thumper_utils:declare_exchange(Channel, ConfigProplist) of
        {ok, _ExchangeName} ->
            {reply, ok, State};
        {error, Error} ->
            {reply, {error, Error}, State}
    end;

handle_call({publish, Message, ExchangeName, RoutingKey, MessageOptions}, _From, 
        State=#state{status=unavailable, publish_queue=PublishQueue, queuejournal_opts=QueueJournalOpts}) ->
    case proplists:get_value(enabled, QueueJournalOpts, true) of
        true ->
            % Empty broker handle, meaning we're currently trying to establish a connection
            {NewPublishQueue, NewQueueJournalOpts} = 
                add_publish_queue_item(ExchangeName, RoutingKey, Message, MessageOptions, PublishQueue, QueueJournalOpts),
            NewState = State#state{publish_queue=NewPublishQueue,queuejournal_opts=NewQueueJournalOpts},
            {reply, ok, NewState}; 
        false ->
            ?THUMP(info, "*** skipped 1", []),
            {reply, skipped, State}
    end;

handle_call({publish, Message, ExchangeName, RoutingKey, MessageOptions}, _From, 
        State=#state{broker_handle=BrokerHandle, publish_queue=PublishQueue, queuejournal_opts=QueueJournalOpts}) ->
    % Publish this message
    {PublishResult, NewPQ2, NewQJ2} = 
        case do_publish(BrokerHandle, ExchangeName, RoutingKey, Message, MessageOptions) of
            ok ->
                {ok, PublishQueue, QueueJournalOpts};
            _Error ->
                case proplists:get_value(enabled, QueueJournalOpts, true) of
                    true ->
                        {X,Y} = add_publish_queue_item(ExchangeName, RoutingKey, Message, MessageOptions, PublishQueue, QueueJournalOpts),
                        {queuejournal, X, Y};
                    false ->
                        ?THUMP(info, "*** skipped 2", []),
                        {skipped, PublishQueue, QueueJournalOpts}
                end
        end,

    NewState = State#state{publish_queue=NewPQ2,queuejournal_opts=NewQJ2},
    {reply, PublishResult, NewState};

handle_call(get_state, _From, State) ->
    {reply, State, State};

handle_call({recover, Requeue}, _From, State=#state{broker_name=BrokerName, subscriber_tid=SubTid}) ->
    Fun = fun(Subscriber, Errors) -> basic_recover_subscriber(Subscriber, Requeue, Errors) end,
    Reply = subscriber_fold(BrokerName, SubTid, Fun),
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

% Reconnect requested by a client.  Terminate the current connection first.
handle_cast(reconnect, State=#state{broker_name=BrokerName, broker_handle=BrokerHandle, subscriber_tid=SubTid, channel_tid=ChanTid}) ->
    ?THUMP(info, "reconnect from client: ~p", [BrokerName]),
    Fun = fun(Subscriber, Errors) -> 
            close_subscriber_channel(SubTid, ChanTid, Subscriber, ?ReconnectCloseCode, ?ReconnectCloseText), 
            Errors 
    end,
    subscriber_fold(BrokerName, SubTid, Fun),
    NewHandle = teardown_broker_handle(BrokerHandle),
    State2 = State#state{broker_handle=NewHandle},
    do_handle_reconnect(State2);

handle_cast({resubscribe_all_other_broker, BrokerName}, State) ->
    resubscribe_all_other_broker(BrokerName, State),
    {noreply, State};

handle_cast(Publish, State) when is_tuple(Publish) andalso
                                 element(1, Publish) =:= publish ->
    {reply, _PublishResult, NewState} = handle_call(Publish, self(), State),
    {noreply, NewState};

handle_cast({register_return_callback, Exchange, RoutingKey, Callback}, State=#state{return_callbacks=Reg}) ->
    MaxCallbacks = application:get_env(thumper, return_callbacks_max, ?ReturnCallbacksMax),
    Size = gb_trees:size(Reg),
    Reg2 = if Size >= MaxCallbacks ->
            {_,_,R2} = gb_trees:take_largest(Reg),
            R2;
        true ->
            Reg
    end,
    ?THUMP(debug, "inserting return callback X ~p RK ~p", [Exchange, RoutingKey]),
    Reg3 = try gb_trees:insert({Exchange, RoutingKey}, Callback, Reg2) of
        R3 ->
            R3
        catch error:{key_exists, _} ->
            gb_trees:update({Exchange, RoutingKey}, Callback, Reg2)
    end,
    {noreply, State#state{return_callbacks=Reg3}};

handle_cast({unregister_return_callback, Exchange, RoutingKey}, State=#state{return_callbacks=Reg}) ->
    Reg2 = gb_trees:delete_any({Exchange, RoutingKey}, Reg),
    {noreply, State#state{return_callbacks=Reg2}};

handle_cast(_, State) ->
    {noreply, State}.

teardown_broker_handle(BrokerHandle) ->
    Channel = BrokerHandle#broker_handle.publish_channel,
    case Channel of
        undefined ->
            ok;
        ChannelPid ->
            thumper_utils:pid_guard(ChannelPid, {amqp_channel, close, [Channel, ?ReconnectCloseCode, ?ReconnectCloseText]})
    end,
    Connection = BrokerHandle#broker_handle.connection,
    case Connection of
        undefined ->
            ok;
        ConnPid ->
            thumper_utils:pid_guard(ConnPid, {amqp_connection, close, [Connection, ?ReconnectCloseCode, ?ReconnectCloseText]})
    end,
    BrokerHandle#broker_handle{publish_channel=undefined,
                               publish_channel_ref=undefined,
                               connection=undefined,
                               connection_ref=undefined}.

% Reconnect requested by schedule_reconnect.
handle_info(schedule_reconnect, #state{broker_name=BrokerName}=State) ->
    ?THUMP(info, "schedule_reconnect reconnect ~p", [BrokerName]),
    do_handle_reconnect(State);

handle_info({'DOWN', _, process, _, unsubscribed}, State) ->
    {noreply, State};
handle_info({'DOWN', Ref, process, Pid2, Reason}, State) ->
    ?THUMP(warning, "DOWN notification: ~p ~p ~p", [Ref, Pid2, Reason]),
    case do_handle_down(Ref, Pid2, Reason, State) of
        {ok, NewState} ->
            {noreply, NewState};
        {error, Error} ->
            {stop, Error, State}
    end;

handle_info({_BasicReturn=#'basic.return'{reply_text=ReplyText, exchange=X, routing_key=RK}, Content}, State=#state{return_callbacks=Return}) ->
    ?THUMP(debug, "got returned message X ~p, RK ~p", [X, RK]),
    case gb_trees:lookup({X, RK}, Return) of
        none ->
            ?THUMP(warning, "returned message with reply ~p on exchange ~p", [ReplyText, X]);
        {value, Callback} when is_function(Callback) ->
            spawn(fun() ->
                try Callback({X, RK, ReplyText}, Content) 
                catch A:B ->
                    ?THUMP(error, "return callback exception from X ~p, RK ~p, ~p:~p", [X, RK, A, B]),
                    ?THUMP(error, "~p", [erlang:get_stacktrace()])
                end
            end);
        {value, {M,F,A}} ->
            spawn(fun() ->
                try apply(M, F, A ++ [{X, RK, ReplyText}, Content])
                catch A:B ->
                    ?THUMP(error, "return callback exception from X ~p, RK ~p, ~p:~p, ~p:~p", [X, RK, M, F, A, B]),
                    ?THUMP(error, "~p", [erlang:get_stacktrace()])
                end
            end)
    end,
    {noreply, State};

handle_info(#'channel.flow'{active = false}, State) ->
    ?THUMP(warning, "throttled by the broker", []),
    {noreply, State};

handle_info(#'channel.flow'{active = true}, State) ->
    ?THUMP(warning, "no longer throttled by the broker", []),
    {noreply, State};

handle_info(Msg, State) ->
    ?THUMP(info, "handle_info unhandled: ~p", [Msg]),
    {noreply, State}.

% TODO: do we have terminate eunit tests?
terminate(Reason, #state{broker_name=BrokerName,
                         broker_handle=BrokerHandle,
                         subscriber_tid=SubTid,
                         channel_tid=ChanTid,
                         publish_queue=PublishQueue,
                         queuejournal_opts=QueueJournalOpts}) ->
    ?THUMP(warning, "thumper ~p received terminate: ~p", [BrokerName, Reason]),
    case proplists:get_value(enabled, QueueJournalOpts, true) of
        true -> write_queue_to_journal(PublishQueue, QueueJournalOpts);
        false -> ok
    end,
    % Do not unsubscribe anyone because we would like to resubscribe them after a restart.
    Fun = fun(Subscriber, Errors) -> close_subscriber_channel(SubTid, ChanTid, Subscriber), Errors end,
    subscriber_fold(BrokerName, SubTid, Fun),
    broker_connection:close(BrokerHandle),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

broker_call(ConfigName, Request) ->
    Ref = thumper_utils:get_thumper_svr(ConfigName),
    do_broker_call(Ref, Request).

broker_cast(ConfigName, Request) ->
    Ref = thumper_utils:get_thumper_svr(ConfigName),
    do_broker_cast(Ref, Request).

do_broker_call(Ref, Request) ->
    try gen_server:call(Ref, Request, 600000) of
        Result ->
            Result
    catch
        Type:Ex ->
            ?THUMP(error, "exception calling thumper server ~p : ~p ~p", [Ref, Type, Ex]),
            {error, {Type, Ex}}
    end.

do_broker_cast(Ref, Request) ->
    try gen_server:cast(Ref, Request) of
        Result ->
            Result
    catch
        Type:Ex ->
            ?THUMP(error, "exception casting thumper server ~p : ~p ~p", [Ref, Type, Ex]),
            {error, {Type, Ex}}
    end.

do_get_state(ConfigName, undefined) ->
    broker_call(ConfigName, get_state);
do_get_state(ConfigName, default) ->
    Ref = thumper_utils:get_thumper_svr(ConfigName),
    do_broker_call(Ref, get_state).

call_subscribe_link(BrokerName, ExchangeName, QueueName, RoutingKey, Callback, Link) 
        when is_binary(QueueName), is_function(Callback) ->
    Result = broker_call(BrokerName, {subscribe_link, ExchangeName, QueueName, RoutingKey, Callback}),
    do_link(Link, Result);

call_subscribe_link(BrokerName, ExchangeName, QueueName, RoutingKey, Callback={_M, _F}, Link) 
        when is_binary(QueueName) ->
    Result = broker_call(BrokerName, {subscribe_link, ExchangeName, QueueName, RoutingKey, Callback}),
    do_link(Link, Result);
    
call_subscribe_link(BrokerName, ExchangeName, QueueName, RoutingKey, Callback={_M, _F, A}, Link) 
        when is_binary(QueueName), is_list(A) ->
    Result = broker_call(BrokerName, {subscribe_link, ExchangeName, QueueName, RoutingKey, Callback}),
    do_link(Link, Result);
    
call_subscribe_link(BrokerName, ExchangeName, QueueName, RoutingKey, Callback={_M, _F, A, Opts}, Link) 
        when is_binary(QueueName), is_list(A), is_list(Opts) ->
    Result = broker_call(BrokerName, {subscribe_link, ExchangeName, QueueName, RoutingKey, Callback}),
    do_link(Link, Result);

call_subscribe_link(BrokerName, ExchangeName, QueueConfig, RoutingKey, Callback, Link) 
        when is_list(QueueConfig), is_function(Callback) ->
    Result = broker_call(BrokerName, {subscribe_link_with_create, ExchangeName, QueueConfig, RoutingKey, Callback}),
    do_link(Link, Result);

call_subscribe_link(BrokerName, ExchangeName, QueueConfig, RoutingKey, Callback={_M, _F}, Link) 
        when is_list(QueueConfig) ->
    Result = broker_call(BrokerName, {subscribe_link_with_create, ExchangeName, QueueConfig, RoutingKey, Callback}),
    do_link(Link, Result);

call_subscribe_link(BrokerName, ExchangeName, QueueConfig, RoutingKey, Callback={_M, _F, A}, Link) 
        when is_list(QueueConfig), is_list(A) ->
    Result = broker_call(BrokerName, {subscribe_link_with_create, ExchangeName, QueueConfig, RoutingKey, Callback}),
    do_link(Link, Result);

call_subscribe_link(BrokerName, ExchangeName, QueueConfig, RoutingKey, Callback={_M, _F, A, Opts}, Link) 
        when is_list(QueueConfig), is_list(A), is_list(Opts) ->
    Result = broker_call(BrokerName, {subscribe_link_with_create, ExchangeName, QueueConfig, RoutingKey, Callback}),
    do_link(Link, Result);

call_subscribe_link(_, _, _, _, _, _) ->
    {error, invalid_callback}.

do_link(true, {ok, Pid}) ->
    ?THUMP(info, "link subscriber ~p to ~p", [Pid, self()]),
    link(Pid),
    {ok, Pid};
do_link(true, Error) ->
    ?THUMP(error, "failed to link subscriber to ~p : ~p", [self(), Error]),
    Error;
do_link(false, Result) ->
    Result.

enforce_active_broker(#broker_handle{}=Handle, State) ->
    State#state{broker_handle=Handle, status=available};
enforce_active_broker(reconnecting, State) ->
    schedule_reconnect(State#state{status=unavailable});
enforce_active_broker(Handle, State) ->
    ?THUMP(error, "enforce_active_broker unrecognized handle ~p", [Handle]),
    State.

schedule_reconnect(#state{broker_handle=Handle}=State) ->
    do_schedule_reconnect(Handle, State).

do_schedule_reconnect(undefined, State) ->
    do_schedule_reconnect(#broker_handle{}, State);
do_schedule_reconnect(#broker_handle{reconnect_timer_ref=undefined}=Handle, #state{broker_name=BrokerName}=State) ->
    case timer:send_after(?RECONNECT_DELAY, schedule_reconnect) of
        {ok, Ref} ->
            ?THUMP(info, "schedule reconnect ~p after ~p ms", [BrokerName, ?RECONNECT_DELAY]),
            Handle2 = Handle#broker_handle{reconnect_timer_ref=Ref},
            State#state{broker_handle=Handle2};
        {error, Error} ->
            ?THUMP(error, "failed to send reconnect timer request: ~p", [Error]),
            State
    end;
do_schedule_reconnect(#broker_handle{}, State) ->
    ?THUMP(info, "skip schedule reconnect - timer already set", []),
    State;
do_schedule_reconnect(Handle, #state{broker_name=BrokerName}=State) ->
    ?THUMP(error, "invalid broker handle ~p for ~p - schedule reconnect failed", [Handle, BrokerName]),
    State.

do_handle_reconnect(#state{broker_handle=Handle}=State) ->
    Handle2 = case Handle#broker_handle.reconnect_timer_ref of
        undefined ->
            Handle;
        Ref ->
            case timer:cancel(Ref) of
                {ok, cancel} ->
                    ?THUMP(info, "reconnect timer cancelled", []),
                    Handle#broker_handle{reconnect_timer_ref=undefined};
                {error, Err} ->
                    ?THUMP(error, "failed to cancel reconnect timer: ~p", [Err]),
                    Handle
            end
    end,
    State2 = State#state{broker_handle=Handle2},
    case handle_reconnect(State2) of
        {ok, NewState} ->
            {noreply, NewState};
        {retry, NewState} ->
            {noreply, NewState};
        {error, Error} ->
            ?THUMP(error, "handle_reconnect returned error: ~p", [Error]),
            {noreply, State2};
        {stop, Error} ->
            ?THUMP(error, "handle_reconnect returned fatal error: ~p", [Error]),
            {stop, Error, State2}
    end.

handle_reconnect(#state{broker_name=BrokerName}=State) ->
    ?THUMP(info, "reconnect ~p", [BrokerName]),
    HaveSubscribers = evaluate_subscriber_state(BrokerName, State),
    case broker_connection:open(BrokerName) of
        {ok, NewHandle} ->
            State2 = enforce_active_broker(NewHandle, State),

            case HaveSubscribers of
                true ->
                    ?THUMP(info, "resubscribing", []),
                    case resubscribe_all(State2) of
                        ok ->
                            {ok, State2};
                        {error, Errors} ->
                            ?THUMP(error, "~p init resubscribe errors ~p", [BrokerName, Errors]),
                            State3 = schedule_reconnect(State2#state{status=unavailable}),
                            {ok, State3}
                    end;
                _ ->
                    {ok, State2}
            end;
        {error, conn_error} ->
            ?THUMP(error, "reconnect connection error", []),
            State2 = enforce_active_broker(reconnecting, State),
            {retry, State2};
        {error, config_error} ->
            ?THUMP(error, "reconnect config error", []),
            State2 = enforce_active_broker(reconnecting, State),
            {retry, State2};
        {error, Error} ->
            ?THUMP(error, "reconnect other error: ~p", [Error]),
            {stop, Error}
    end.

is_ref_down(_Ref, undefined) ->
    false;
is_ref_down(Ref, #broker_handle{publish_channel_ref=ChannelRef, connection_ref=ConnectionRef}) ->
    case Ref of
        ChannelRef ->
            true;
        ConnectionRef ->
            true;
        _ ->
            false
    end.

schedule_reconnect_after_error(_, _, #state{broker_name=BrokerName, broker_handle=undefined}) ->
    ?THUMP(error, "schedule_reconnect_after_error called with inactive broker: ~p", [BrokerName]),
    {error, invalid_state};
schedule_reconnect_after_error(Ref, Pid, 
        State=#state{broker_name=BrokerName, broker_handle=BrokerHandle, subscriber_tid=SubTable, channel_tid=ChanTable}) ->
    case complete_reconnect_after_error(is_broker_ref(Ref, BrokerHandle), BrokerHandle, State) of
        {ok, State2} ->
            ?THUMP(info, "reconnect_after_error complete for active broker ~p (ref)", [BrokerName]),
            {ok, State2};
        _ ->
            ShouldReconnectActive = case ets:lookup(SubTable, Pid) of
                [#subscriber{}|_] ->
                    ?THUMP(info, "reconnect_after_error for ~p (sub)", [BrokerName]),
                    true;
                _ ->
                    case ets:lookup(ChanTable, Pid) of
                        [{Pid, _SubPid}|_] ->
                            ?THUMP(info, "reconnect_after_error for ~p (chan)", [BrokerName]),
                            true;
                        _ ->
                            false
                    end
            end,
            case ShouldReconnectActive of
                true ->
                    case complete_reconnect_after_error(true, BrokerHandle, State) of
                        {ok, State2} ->
                            ?THUMP(info, "reconnect_after_error complete for ~p", [BrokerName]),
                            {ok, State2};
                        _ ->
                            {ok, State}
                    end;
                _ ->
                    {ok, State}
            end
    end.

complete_reconnect_after_error(true, #broker_handle{connection_ref=ConnRef}=Handle, State) ->
    ?Demonitor(ConnRef),
    NewHandle = Handle#broker_handle{connection=undefined, connection_ref=undefined, publish_channel=undefined, publish_channel_ref=undefined},
    ResetState = State#state{broker_handle=NewHandle},
    State2 = enforce_active_broker(reconnecting, ResetState), % TODO: use a different value from reconnecting here?
    {ok, State2};
complete_reconnect_after_error(_, _, _) ->
    false.

is_broker_ref(_, undefined) ->
    false;
is_broker_ref(Ref, #broker_handle{connection_ref=ConnRef, publish_channel_ref=ChanRef}) ->
    case Ref of
        ConnRef ->
            true;
        ChanRef ->
            true;
        _ ->
            false
    end.

do_handle_down(_, _, {shutdown, {app_initiated_close, ?ReconnectCloseCode, ?ReconnectCloseText}}, State) ->
    % Client requested reconnect. No DOWN handling needed.
    {ok, State};
do_handle_down(Ref, Pid, {shutdown, {connection_closing, _Reason}}, State) ->
    % The connection is closing due to an error.  Reconnect later.
    ?THUMP(error, "connection_closing error", []),
    schedule_reconnect_after_error(Ref, Pid, State#state{status=unavailable});
do_handle_down(_Ref, _Pid, {infrastructure_died, _}, State) ->
    % A channel died in an unexpected way.  This will also take down the connection,
    % so wait for the connection_closing event before scheduling a reconnect.
    ?THUMP(error, "infrastructure_died channel error", []),
    {ok, State};
do_handle_down(Ref, Pid, Reason, #state{broker_handle=BrokerHandle}=State) ->
    case is_ref_down(Ref, BrokerHandle) of
        true ->
            do_handle_down(Ref, State);
        _ ->
            do_handle_subscriber_down(Pid, Reason, State)
    end.

do_handle_down(_Ref, State=#state{broker_name=Name, broker_handle=BrokerHandle}) ->
    ?Demonitor(BrokerHandle#broker_handle.connection_ref),
    ResetState = State#state{broker_handle=BrokerHandle#broker_handle{connection=undefined, connection_ref=undefined}},
    case broker_connection:reopen(Name, BrokerHandle) of
        {ok, new_connection, NewHandle} ->
            State2 = enforce_active_broker(NewHandle, ResetState),
            {ok, State2};
        {ok, NewHandle} ->
            State2 = enforce_active_broker(NewHandle, ResetState),
            {ok, State2};
        {error, _Error} ->
            State2 = enforce_active_broker(reconnecting, ResetState),
            {ok, State2}
    end.

do_handle_subscriber_down(Pid, Reason, State=#state{broker_name=BrokerName, broker_handle=BrokerHandle, subscriber_tid=SubTid, channel_tid=ChanTid}) ->
    case ets:lookup(SubTid, Pid) of
        SubList when length(SubList) == 1 ->
            ?THUMP(info, "subscriber terminated: ~p ~p", [Pid, Reason]),
            %remove_subscriber(SubTid, ChanTid, Pid);
            Sub = lists:nth(1, SubList),
            do_unsubscribe(Sub, SubTid, ChanTid);
        [] ->
            % TODO: This wad doesn't make sense anymore - if we think the sub is down, why would we try to resubscribe?
            % We would need to restart the process first which requires us to know if it was being supervised and potentially
            % restarted by some supervisor.
            case ets:lookup(ChanTid, Pid) of
                [{_ChannelPid, SubscriberPid}] ->
                    ?THUMP(info, "subscriber channel pid down: ~p (subscriber pid: ~p)", [Pid, SubscriberPid]),
                    case ets:lookup(SubTid, SubscriberPid) of
                        [Subscriber] ->
                            do_resubscribe(Subscriber, SubTid, ChanTid, BrokerHandle, BrokerName);
                        _ ->
                            ?THUMP(error, "subscriber pid not found: ~p", [SubscriberPid])
                    end;
                _ ->
                    ?THUMP(warning, "monitor detected on unknown pid: ~p", [Pid])
            end;
        Subs ->
            ?THUMP(error, "found multiple subscribers for pid: ~p : ~p", [Pid, Subs])
    end,
    {ok, State}.

resubscribe_all_other_broker(OtherBrokerName, #state{broker_name=BrokerName, broker_handle=BrokerHandle, subscriber_tid=SubTid, channel_tid=ChanTid}) ->
    ?THUMP(info, "broker ~p resubscribe all from broker ~p", [BrokerName, OtherBrokerName]), 
    resubscribe_all(OtherBrokerName, BrokerName, BrokerHandle, SubTid, ChanTid).

resubscribe_all(#state{broker_name=BrokerName, broker_handle=BrokerHandle, subscriber_tid=SubTid, channel_tid=ChanTid}) ->
    resubscribe_all(BrokerName, BrokerName, BrokerHandle, SubTid, ChanTid).

resubscribe_all(FoldBrokerName, BrokerName, BrokerHandle, SubTid, ChanTid) ->
    Fun = fun(Subscriber, Errors) -> 
                  try do_resubscribe(Subscriber, SubTid, ChanTid, BrokerHandle, BrokerName) of
                      {error, conn_error} ->
                          [conn_error|Errors];
                        _ ->
                          Errors
                  catch
                      X:Y ->
                          ?THUMP(error, "caught exception ~p ~p during do_resubscribe of ~p", [X, Y, Subscriber]),
                          [{X, Y}|Errors]
                  end
          end,
    subscriber_fold(FoldBrokerName, SubTid, Fun).

% TODO: Is it appropriate to call remove_subscriber() everywhere in this function?
% If we cannot resubscribe, we remove the sub from the table.  Maybe keep him but indicate this state in his record.
do_resubscribe(
      Sub=#subscriber{
             pid=SubscriberPid, ref=Ref, channel=PrevChannel,
             exchange_name=ExchangeName, queue_name=QueueName, queue_config=QueueConfig, routing_key=RoutingKey},
      SubTid,
      ChanTid,
      #broker_handle{connection=Connection},
      BrokerName) ->

    ?THUMP(info, "do_resubscribe to queue: ~p pid: ~p", [QueueName, SubscriberPid]),
    case ?IsPidAlive(PrevChannel) of
        true ->
            ?THUMP(info, "close active previous channel ~p for subscriber ~p", [PrevChannel, SubscriberPid]),
            close_subscriber_channel(SubTid, ChanTid, Sub);
        _ ->
            ok
    end,
    case thumper_utils:init_channel(Connection) of
        
        {ok, Channel} ->
            case do_resubscribe_declare(Channel, Sub) of
                ok ->
                    case do_resubscribe_bind(Channel, QueueName, ExchangeName, RoutingKey) of
                        ok ->
                            case do_subscribe(Channel, Sub, SubscriberPid) of
                                {ok, ConsumerTag} ->
                                    % Cleanup existing subscriber channel.
                                    remove_subscriber(SubTid, ChanTid, SubscriberPid),
                                    case ?IsPidAlive(SubscriberPid) of
                                        true ->
                                            Ref2 = case Ref of
                                                       undefined ->
                                                           Ref2_ = ?Monitor(SubscriberPid),
                                                           ?THUMP(info, "new monitor of resubscribed pid: ~p ~p", [SubscriberPid, Ref2_]),
                                                           Ref2_;
                                                       _ ->
                                                           Ref
                                                   end,
                                            SubscriberPid ! {new_channel, Channel},
                                            SubscriberPid ! {new_consumer_tag, ConsumerTag},
                                            ChannelRef = ?Monitor(Channel),
                                            NewSub = Sub#subscriber{
                                                       ref=Ref2, 
                                                       channel=Channel, 
                                                       channel_ref=ChannelRef, 
                                                       consumer_tag=ConsumerTag, 
                                                       broker_name=BrokerName},
                                            Result = add_subscriber(SubTid, ChanTid, NewSub),
                                            ?THUMP(info, "resubscribe successful: subscriber ~p, channel ~p (previous channel ~p) (consumer_tag ~p) (broker ~p)", 
                                                  [SubscriberPid, Channel, PrevChannel, ConsumerTag, BrokerName]),
                                            Result;
                                        _ ->
                                            ?THUMP(error, "resubscribe failed: subscriber is no longer alive: ~p", [SubscriberPid]),
                                            remove_subscriber(SubTid, ChanTid, SubscriberPid),
                                            {error, not_alive}
                                    end;
                                {error, DoSubError} ->
                                    ?THUMP(info, "do_subscribe error: ~p", [DoSubError]),
                                    try amqp_channel:close(Channel)
                                        catch Class:Reason ->
                                            ?THUMP(error, "channel close exception ~p:~p", [Class, Reason])
                                    end,
                                    remove_subscriber(SubTid, ChanTid, SubscriberPid)
                            end;
                        _Error ->
                            try amqp_channel:close(Channel)
                                catch Class:Reason ->
                                    ?THUMP(error, "channel close exception ~p:~p", [Class, Reason])
                            end,
                            remove_subscriber(SubTid, ChanTid, SubscriberPid)
                    end;
                {error, remove} ->
                    try amqp_channel:close(Channel)
                        catch Class:Reason ->
                            ?THUMP(error, "channel close exception ~p:~p", [Class, Reason])
                    end,
                    remove_subscriber(SubTid, ChanTid, SubscriberPid)
            end;

        {error, closing} ->
            ?THUMP(error, "connection closing prevents resubscribe: ~p", [QueueConfig]),
            {error, conn_error};

        {error, Error} ->
            ?THUMP(error, "init channel error ~p for resubscribe: ~p", [Error, QueueConfig]),
            % Do not call remove_subscriber in this case because the connection may be down.
            %remove_subscriber(SubTid, ChanTid, SubscriberPid),
            {error, conn_error}
    end.

do_resubscribe_bind(_Channel, _QueueName, undefined, undefined) ->
    ok;
do_resubscribe_bind(Channel, QueueName, Exchange, RoutingKey) ->
    case thumper_utils:bind_queue(Channel, QueueName, Exchange, RoutingKey) of
        {ok, _Binding} ->
            ok;
        {error, Error} ->
            ?THUMP(error, "bind queue error ~p for resubscribe: ~p", [Error, QueueName]),
            {error, remove}
    end.

do_resubscribe_declare(Channel, #subscriber{create=true, queue_config=QueueConfig}) ->
    case thumper_utils:declare_queue(Channel, QueueConfig) of
        {ok, _QueueName} ->
            ok;
        {error, Error} ->
            ?THUMP(error, "declare queue error ~p for resubscribe: ~p", [Error, QueueConfig]),
            {error, remove}
    end;
do_resubscribe_declare(_Channel, #subscriber{create=false}) ->
    ok.

add_subscriber(SubTid, ChanTid, Subscriber=#subscriber{pid=SubscriberPid, broker_name=BrokerName, channel=Channel}) ->
    ets:insert(SubTid, Subscriber),
    case Channel of
        undefined ->
            ?THUMP(info, "skipped insert of undefined channel for subscriber: ~p", [SubscriberPid]),
            ok;
        _ ->
            ets:insert(ChanTid, {Channel, SubscriberPid, BrokerName})
    end.

remove_subscriber(SubTid, ChanTid, SubscriberPid) ->
    case ets:lookup(SubTid, SubscriberPid) of
        [#subscriber{ref=SubscriberRef, channel=Channel, channel_ref=ChannelRef}] ->
            ?Demonitor(ChannelRef),
            ets:delete(ChanTid, Channel),
            ?Demonitor(SubscriberRef),
            ets:delete(SubTid, SubscriberPid);
        _ ->
            ok
    end.
    % TODO: close the channel (not in all cases...)

%cancel_all(SubTid, ChanTid) ->
%    Fun = fun(Subscriber, _Errors) -> do_unsubscribe(Subscriber, SubTid, ChanTid), [] end,
%    subscriber_fold(SubTid, Fun).

do_unsubscribe(SubscriberPid, SubTid, ChanTid) when is_pid(SubscriberPid) ->
    case ets:lookup(SubTid, SubscriberPid) of
        [Sub] ->
            do_unsubscribe(Sub, SubTid, ChanTid);
        _ ->
            ?THUMP(error, "cannot unsubscribe unknown pid: ~p", [SubscriberPid]),
            {error, unknown_subscriber_pid}
    end;
do_unsubscribe(#subscriber{pid=SubscriberPid, channel=Channel, consumer_tag=ConsumerTag}, SubTid, ChanTid) ->
    case erlang:process_info(SubscriberPid) of

        undefined ->
            case is_pid(Channel) and is_process_alive(Channel) of
                true ->
                    try amqp_channel:close(Channel)
                        catch Class:Reason ->
                            ?THUMP(error, "channel close exception ~p:~p", [Class, Reason])
                    end,
                    ?THUMP(info, "cancelled channel ~p", [Channel]),
                    remove_subscriber(SubTid, ChanTid, SubscriberPid);
                false ->
                    ok
            end;

        _Info ->
            try amqp_channel:call(Channel, #'basic.cancel'{consumer_tag=ConsumerTag}) of
                #'basic.cancel_ok'{} ->
                    ?THUMP(info, "unsubscribed: ~p (channel: ~p)", [SubscriberPid, Channel]),
                    SubscriberPid ! unsubscribed, % See unsubscribe_wait()
                    remove_subscriber(SubTid, ChanTid, SubscriberPid),
                    ok;
                Error ->
                    ?THUMP(error, "basic.cancel failed for: ~p (channel: ~p)", [SubscriberPid, Channel]),
                    Error
            catch
                X:Y ->
                    ?THUMP(error, "basic.cancel exception for channel ~p : ~p ~p : backtrace ~p", [Channel, X, Y, erlang:get_stacktrace()]),
                    {error, {X, Y}}
            end,
            try amqp_channel:close(Channel)
                catch Class:Reason ->
                    ?THUMP(error, "channel close exception ~p:~p", [Class, Reason])
            end,
            ?THUMP(info, "cancelled channel ~p", [Channel])
    end;
do_unsubscribe(Sub, _, _) ->
    ?THUMP(error, "do_unsubscribe by unhandled type: ~p", [Sub]),
    {error, badarg}.

do_subscribe_link_with_create(BrokerHandle, Subscriber, Callback, SubTid, ChanTid) ->
    Connection = BrokerHandle#broker_handle.connection,
    case thumper_utils:init_channel(Connection) of
        {ok, Channel} ->
            ChannelRef = ?Monitor(Channel),
            case thumper_utils:declare_queue(Channel, Subscriber#subscriber.queue_config) of
                {ok, _QueueName} ->
                    NewSubscriber = Subscriber#subscriber{channel_ref=ChannelRef},
                    do_subscribe_link(Channel, BrokerHandle, NewSubscriber, Callback, SubTid, ChanTid);
                {error, Error} ->
                    ?Demonitor(ChannelRef),
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.

do_subscribe_link(Channel, _BrokerHandle, Subscriber, Callback, SubTid, ChanTid) when is_pid(Channel) ->
    case thumper_utils:bind_queue(Channel, Subscriber#subscriber.queue_name, Subscriber#subscriber.exchange_name, Subscriber#subscriber.routing_key) of
        {ok, Binding} ->
            SubscriberPid = spawn(fun() -> consumer_loop(Channel, Binding, Callback, undefined, undefined) end),
            case do_subscribe(Channel, Subscriber, SubscriberPid) of
                {ok, ConsumerTag} ->
                    Ref = ?Monitor(SubscriberPid),
                    ?THUMP(info, "new subscriber: ~p (channel: ~p) (ref: ~p) (consumer_tag: ~p)", [SubscriberPid, Channel, Ref, ConsumerTag]),
                    NewSubscriber = Subscriber#subscriber{pid=SubscriberPid, ref=Ref, channel=Channel, consumer_tag=ConsumerTag},
                    add_subscriber(SubTid, ChanTid, NewSubscriber),
                    {ok, SubscriberPid};
                {error, Error} ->
                    ?THUMP(error, "do_subscribe failed: ~p", [Error]),
                    % TODO: kill the pid here?
                    {error, Error}
            end;
        Error ->
            ?THUMP(error, "bind_queue failure: ~p", [Error]),
            ?Demonitor(Subscriber#subscriber.channel_ref),
            Error
    end;

do_subscribe_link(_, BrokerHandle, Subscriber, Callback, SubTid, ChanTid) ->
    Connection = BrokerHandle#broker_handle.connection,
    case thumper_utils:init_channel(Connection) of
        {ok, Channel} ->
            ChannelRef = ?Monitor(Channel),
            NewSubscriber = Subscriber#subscriber{channel=Channel, channel_ref=ChannelRef},
            do_subscribe_link(Channel, BrokerHandle, NewSubscriber, Callback, SubTid, ChanTid);
        {error, Error} ->
            {error, Error}
    end.

do_subscribe(Channel, #subscriber{queue_name=QueueName, queue_config=Config}, SubscriberPid) ->
    case ?IsPidAlive(SubscriberPid) of
        false ->
            {error, pid_not_alive};
        _ ->
            try amqp_channel:call(Channel, #'basic.qos'{prefetch_count=2000})
            catch Class:Reason ->
                ?THUMP(error, "exception setting prefetch ~p:~p", [Class, Reason])
            end,
            Args = proplists:get_value(arguments, Config, []),
            Subscription = #'basic.consume'{queue=QueueName, arguments=Args},
            try amqp_channel:subscribe(Channel, Subscription, SubscriberPid) of
                #'basic.consume_ok'{consumer_tag=ConsumerTag} ->
                    {ok, ConsumerTag};
                Error ->
                    {error, Error}
            catch
                exit:{{shutdown, {server_initiated_close, 404, _BinaryMsg}}, _} ->
                    % The subscriber is referring to a non-existent queue.  This can happen
                    % to a subscriber of an auto_delete queue after a connection failure.
                    {error, not_found};
                X:Y ->
                    ?THUMP(error, "caught exception: ~p ~p", [X,Y]),
                    {error, {X, Y}}
            end
    end.

% Allows us to skip 'DOWN' handling when subscriber has been programmatically unsubscribed.
unsubscribe_wait() ->
    receive
        unsubscribed ->
            exit(unsubscribed)
    after
        100 ->
            ok
    end.

-record(consumer_state, 
    {
        batch={[], 0},     % Keep track of list size as optimization
        max_batch_size,    % max batch size (default 1)
        receive_timeout    % receive timeout value (integer millis or 'infinity')
    }).

consumer_loop(Channel, Binding, Callback, ConsumerTag, undefined) ->
    {Callback_, Opts} = case Callback of
        {M, F, A, O} ->
            {{M, F, A}, O};
        _ ->
            {Callback, []}
    end,
    consumer_loop(Channel, Binding, Callback_, ConsumerTag,
        #consumer_state{
            max_batch_size=proplists:get_value(max_batch_size, Opts, 1),
            receive_timeout=proplists:get_value(receive_timeout, Opts, infinity)
        });

consumer_loop(Channel, Binding, Callback, ConsumerTag,
    State=#consumer_state{batch={Batch,BatchSize},
                         max_batch_size=MaxBatchSize,
                         receive_timeout=Timeout}) ->
    receive

        #'basic.consume_ok'{consumer_tag=SubscriberTag} ->
            ?THUMP(debug, "subscriber received basic.consume_ok: ~p (consumer_tag ~p)", [self(), SubscriberTag]),
            consumer_loop(Channel, Binding, Callback, SubscriberTag, State);

        #'basic.cancel'{} ->
            ?THUMP(debug, "subscriber received basic.cancel: ~p", [self()]),
            ok;

        #'basic.cancel_ok'{} ->
            ?THUMP(debug, "subscriber received basic.cancel_ok: ~p", [self()]),
            unsubscribe_wait();

        {#'basic.deliver'{delivery_tag=DeliveryTag, routing_key=RoutingKey, consumer_tag=DeliveryConsumerTag}, 
            #amqp_msg{payload=Payload}} ->

            case DeliveryConsumerTag of
                ConsumerTag ->
                    %?THUMP(info, "Received: routing_key: ~p~n ~p~n", [RoutingKey, Payload]),
                    %?THUMP(info, "Received: routing_key: ~p~n ~p~n", [RoutingKey, binary_to_term(Payload)]),

                    Batch_ = [{DeliveryTag, RoutingKey, Payload}|Batch],
                    BatchSize_ = BatchSize+1,

                    if
                        BatchSize_ >= MaxBatchSize ->

                            consume_batch(Channel, Callback, lists:reverse(Batch_)),
                            consumer_loop(Channel, Binding, Callback, ConsumerTag,
                                State#consumer_state{
                                    batch={[],0}});
                        true ->
                            consumer_loop(Channel, Binding, Callback, ConsumerTag,
                                State#consumer_state{
                                    batch={Batch_, BatchSize_}})
                    end;
                _ ->
                    ?THUMP(error, "ignore delivery from unknown consumer_tag ~p", [DeliveryConsumerTag]),
                    consumer_loop(Channel, Binding, Callback, ConsumerTag, State)
            end;

        {new_channel, NewChannel} ->
            ?THUMP(debug, "subscriber received new channel: ~p", [NewChannel]),
            consumer_loop(NewChannel, Binding, Callback, ConsumerTag,
                State#consumer_state{batch={[], 0}});

        {new_consumer_tag, NewConsumerTag} ->
            ?THUMP(debug, "subscriber received new consumer tag: ~p", [NewConsumerTag]),
            consumer_loop(Channel, Binding, Callback, NewConsumerTag,
                State#consumer_state{batch={[], 0}});

        {new_callback, NewCallback} ->
            consumer_loop(Channel, Binding, NewCallback, ConsumerTag, undefined);

        Message ->
            ?THUMP(info, "unrecognized message: ~p", [Message]),
            consumer_loop(Channel, Binding, Callback, ConsumerTag, State)
    after
        Timeout ->
            ?THUMP(debug, "timeout triggered batch consume", []),
            consume_batch(Channel, Callback, lists:reverse(Batch)),
            consumer_loop(Channel, Binding, Callback, ConsumerTag,
                State#consumer_state{batch={[], 0}})
    end.

consume_batch(_, _, []) ->
    ok;
consume_batch(Channel, Callback, Batch) when is_list(Batch) ->
    {DeliveryTag, RoutingKey, Payload} = lists:last(Batch),
    try make_callback_batch(Callback, Batch) of
        ack ->
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag=DeliveryTag});
        {ack, Multiple} when is_boolean(Multiple) ->
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag=DeliveryTag, multiple=Multiple});
        nack ->
            amqp_channel:cast(Channel, #'basic.nack'{delivery_tag=DeliveryTag, requeue=false});
        {nack, Multiple, Requeue} when is_boolean(Multiple), is_boolean(Requeue) ->
            amqp_channel:cast(Channel, #'basic.nack'{delivery_tag=DeliveryTag, multiple=Multiple, requeue=Requeue});
        Response ->
            ?THUMP(info, "unknown callback response: ~p~n routing_key: ~p~n payload: ~p batch size: ~p",
                [Response, RoutingKey, Payload, length(Batch)])
    catch
        X:Y:Stacktrace ->
            ?THUMP(error, "caught exception during message callback: ~p ~p~nStacktrace: ~p~nrouting_key: ~p~n payload: ~p batch size: ~p",
                [X, Y, Stacktrace, RoutingKey, Payload, length(Batch)])
    end;
consume_batch(_Channel, _Callback, Batch) ->
    ?THUMP(error, "unexpected batch data structure ~p", [Batch]).

make_callback_batch(Callback, [{DT, RK, Payload}]) ->
    make_callback(Callback, DT, RK, Payload);
make_callback_batch({M, F, A}, Batch) ->
    apply(M, F, lists:append([Batch], A));
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

basic_recover_subscriber(Subscriber=#subscriber{channel=Channel}, Requeue, Errors) ->
    case ?IsPidAlive(Channel) of
        true ->
            case basic_recover(Channel, Requeue) of
                ok ->
                    Errors;
                Error ->
                    [Error|Errors]
            end;
        _ ->
            [{channel_not_pid, Subscriber}|Errors]
    end.

basic_recover(Channel, Requeue) ->
    try amqp_channel:call(Channel, #'basic.recover'{requeue=Requeue}) of
        #'basic.recover_ok'{} ->
            ok;
        Error ->
            Error
    catch
        X:Y ->
            ?THUMP(error, "basic.recover exception for channel ~p : ~p ~p : backtrace ~p", [Channel, X, Y, erlang:get_stacktrace()]),
            {error, {X, Y}}
    end.

subscriber_fold(BrokerName, SubTid, Fun) ->
    Fun_ = fun
        (X=#subscriber{broker_name=B}, A) when B =:= BrokerName ->
            Fun(X, A);
        (_, A) ->
            A
    end,
    case ets:foldl(Fun_, [], SubTid) of
        [] ->
            ok;
        Errors ->
            {error, Errors}
    end.

close_subscriber_channel(SubTid, ChanTid, Subscriber) ->
    close_subscriber_channel(SubTid, ChanTid, Subscriber, undefined, undefined).

close_subscriber_channel(SubTid, ChanTid, Subscriber=#subscriber{channel=Channel}, Code, Text) ->
    close_subscriber_channel(SubTid, ChanTid, Subscriber, Channel, Code, Text).

close_subscriber_channel(_SubTid, _ChanTid, _Subscriber, undefined, _Code, _Text) ->
    ok;
close_subscriber_channel(SubTid, ChanTid, Subscriber=#subscriber{pid=SubscriberPid}, Channel, Code, Text) ->
    try case erlang:process_info(Channel) of
        undefined ->
            ok;
        _ ->
            case {Code, Text} of
                {undefined, undefined} ->
                    amqp_channel:close(Channel);
                _ ->
                    amqp_channel:close(Channel, Code, Text)
            end
    end
    catch Class:Reason ->
        ?THUMP(error, "exception closing channel ~p:~p", [Class, Reason])
    end,
    remove_subscriber(SubTid, ChanTid, SubscriberPid),
    NewSubscriber = Subscriber#subscriber{channel=undefined, channel_ref=undefined},
    add_subscriber(SubTid, ChanTid, NewSubscriber).

add_publish_queue_item(ExchangeName, RoutingKey, Message, MessageOptions, Queue, QueueJournalOpts) ->

    NewQueue = array:set(array:size(Queue),
                        [{exchange,    ExchangeName},
                         {routing_key, RoutingKey},
                         {message,     Message},
                         {mandatory,   proplists:get_value(mandatory, MessageOptions, false)}, %% backward compat
                         {message_options, MessageOptions}],
                        Queue),

    MemQueueMax = proplists:get_value(memqueue_max, QueueJournalOpts, ?MemQueueMaxDefault),
    case array:size(NewQueue) of
        Len when Len < MemQueueMax ->
            { NewQueue, QueueJournalOpts };
        _Len ->
            write_queue_to_journal(NewQueue, QueueJournalOpts),
            { array:new(), set_check_journal(true, QueueJournalOpts)}
    end.

set_check_journal(CheckJournalVal, QueueJournalOpts) ->
    proplists:delete(check_journal, QueueJournalOpts) ++ [{check_journal, CheckJournalVal}].

publish_queue_item(BrokerHandle, [_|_]=Item) ->
    ExchangeName = proplists:get_value(exchange, Item),
    RoutingKey = proplists:get_value(routing_key, Item),
    Message = proplists:get_value(message, Item),
    MessageOptions = proplists:get_value(message_options, Item, []),

    %% backward compat
    MandatoryDef = proplists:get_value(mandatory, Item, false),
    MessageOptions2 = case proplists:get_value(mandatory, MessageOptions) of
                          undefined ->
                              MessageOptions ++ [{mandatory, MandatoryDef}];
                          _ ->
                              MessageOptions
                      end,

    case do_publish(BrokerHandle, ExchangeName, RoutingKey, Message, MessageOptions2) of
        ok ->
            ok;
        Error ->
            ?THUMP(error, "Received error=~p, when flushing queue item ~p",
                            [Error, [{exchange,    ExchangeName},
                                     {routing_key, RoutingKey},
                                     {message,     Message}]]),
            ok
    end;
publish_queue_item(_Handle, Item) ->
    ?THUMP(error, "Unable to publish queue item ~p",[Item]),
    ok.

do_publish(undefined, _ExchangeName, _RoutingKey, _Message, _MessageOptions) ->
    {error, undefined};
do_publish(#broker_handle{reconnect_timer_ref=Ref}, _ExchangeName, _RoutingKey, _Message, _MessageOptions) when Ref =/= undefined ->
    {error, not_active};
do_publish(BrokerHandle, ExchangeName, RoutingKey, Message, MessageOptions) ->
    Channel = BrokerHandle#broker_handle.publish_channel,

    Mandatory = proplists:get_value(mandatory, MessageOptions, false),
    % Mandatory:
    %   This flag tells the server how to react if a message cannot be
    %   routed to a queue. Specifically, if mandatory is set and after
    %   running the bindings the message was placed on zero queues then
    %   the message is returned to the sender (with a basic.return). If
    %   mandatory had not been set under the same circumstances the server
    %   would silently drop the message.

    AmqpMsg = #amqp_msg{payload=Message},
    AmqpMsg2 = case proplists:get_value(headers, MessageOptions) of
                  undefined ->
                      AmqpMsg;
                  Headers ->
                      AmqpMsg#amqp_msg{props = #'P_basic'{headers=Headers}}
              end,
    Publish = #'basic.publish'{exchange=ExchangeName, routing_key=RoutingKey, mandatory=Mandatory},
    amqp_channel:cast(Channel, Publish, AmqpMsg2).

flush_publish_queue(BrokerHandle, MemQueue, QueueJournalOpts) ->
    case proplists:get_value(check_journal, QueueJournalOpts, false) of
        true ->
            QueueJournalDir = proplists:get_value(dir, QueueJournalOpts, ?QueueJournalDirDefault),
            case flush_queue_journal(BrokerHandle, QueueJournalDir) of
                {ok, JournalRemainingFlag} ->
                    {ok, { flush_publish_memqueue(BrokerHandle, MemQueue),
                            set_check_journal(JournalRemainingFlag, QueueJournalOpts)}
                        };
                {error, Reason} ->
                    {error, Reason}
            end;
        false ->
            {ok, {flush_publish_memqueue(BrokerHandle, MemQueue), QueueJournalOpts} }
    end.

flush_publish_memqueue(BrokerHandle, Queue) ->
    case array:size(Queue) of
        0 ->
            Queue;
        Num ->
            ?THUMP(info, "Flushing ~p items from the queue", [Num]),
            array:map(fun(_Index,QueueItem) ->
                         publish_queue_item(BrokerHandle, QueueItem)
                      end,
                      Queue),
            array:new()
    end.

flush_queue_journal(BrokerHandle, QueueJournalDir) ->
    case pop_queue_from_journal(QueueJournalDir) of
        {ok, Queue} ->
            flush_publish_memqueue(BrokerHandle, Queue),
            flush_queue_journal(BrokerHandle, QueueJournalDir);
        {error, empty} ->
            {ok, false};
        {error, Reason} ->
            {error, Reason}
    end.

write_queue_to_journal(Queue, QueueJournalOpts) ->
    case array:size(Queue) of
        0 ->
            ok;
        _ ->
            QueueJournalDir = proplists:get_value(dir, QueueJournalOpts, ?QueueJournalDirDefault),
            Filepath = make_now_filepath(QueueJournalDir),
            file:write_file(Filepath, term_to_binary(Queue))
    end.

pop_queue_from_journal(QueueJournalDir) ->
    {ok, List} = file:list_dir(QueueJournalDir),
    case length(List) of
        0 ->
            {error, empty};
        _ ->
            SortedList = lists:sort(fun(A,B)->A<B end, List),
            File = QueueJournalDir ++ "/" ++ hd(SortedList),
            case file:read_file(File) of
                {ok, <<>>} ->
                    file:delete(File),
                    {error, empty};
                {ok, B} ->
                    Queue = binary_to_term(B),
                    file:delete(File),
                    {ok, Queue};
                {error, eacces} ->
                    ?THUMP(error, "Insufficient access privileges to ~p",[QueueJournalDir]),
                    {error, eacces};
                {error, Reason} ->
                    ?THUMP(warning, "Unable to read queue journal file ~p with reason ~p. Deleting file and continuing", [File, Reason]),
                    file:delete(File),
                    pop_queue_from_journal(QueueJournalDir)
            end
    end.

make_now_filepath(QueueJournalDir) ->
    {Mega, Secs, Micro} = ?NOW(),
    lists:flatten(
                    QueueJournalDir ++
                    "/" ++
                    io_lib:format("~w", [node()]) ++
                    io_lib:format("~6..0B", [Mega]) ++ 
                    io_lib:format("~6..0B", [Secs]) ++
                    io_lib:format("~6..0b", [Micro]) ++
                    "thumper_queuejournal"
                  ).

dump_table(T) ->
    L = ets:tab2list(T),
    ?THUMP(info, "~n~p (~p entries): ~n~p~n", [T, length(L), L]).
