-module(broker_connection).

-export([
        open/1,
        reopen/2,
        close/1,
        do_connect/2,
        rabbitmq_config/1
    ]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("../include/thumper.hrl").

-define(DefaultConfig, [{host, "localhost"},
                        {port, 5672},
                        {virtual_host, <<"/">>},
                        {user, <<"guest">>},
                        {password, <<"guest">>}]).

close(undefined) ->
    ok;
close(#broker_handle{publish_channel=Channel, connection=Connection}) ->
    case Channel of
        undefined ->
            ok;
        ChannelPid ->
            thumper_utils:pid_guard(ChannelPid, {amqp_channel, close, [Channel]})
    end,
    case Connection of
        undefined ->
            ok;
        ConnectionPid ->
            thumper_utils:pid_guard(ConnectionPid, {amqp_connection, close, [Connection]})
    end;
close(_) ->
    ?THUMP(error, "close called with invalid argument", []),
    {error, invalid_argument}.

reopen(Name, Handle=#broker_handle{publish_channel=ChannelPid, connection=ConnectionPid}) ->
    case ?IsPidAlive(ConnectionPid) of
        false ->
            reopen_connection(Name, Handle);
        _ ->
            case ?IsPidAlive(ChannelPid) of
                false ->
                    reopen_channel(Handle);
                _ ->
                    {error, broker_alive}
            end
    end;
reopen(_Name, _) ->
    ?THUMP(error, "reopen called with invalid argument", []),
    {error, invalid_argument}.

reopen_connection(Name, #broker_handle{network=Network}) ->
    case init_connection(Name, Network) of
        {ok, Handle} ->
            {ok, new_connection, Handle};
        {error, Error} ->
            {error, Error}
    end.

reopen_channel(Handle=#broker_handle{publish_channel=ChannelPid, connection=ConnectionPid}) ->
    ?THUMP(info, "publish channel down: ~p", [ChannelPid]),
    ConnectionPid = Handle#broker_handle.connection,
    case thumper_utils:init_channel(ConnectionPid) of
        {ok, NewChannelPid} ->
            NewChannelRef = ?Monitor(NewChannelPid),
            NewHandle = Handle#broker_handle{publish_channel_ref=NewChannelRef, publish_channel=NewChannelPid},
            {ok, NewHandle};
        {error, closing} ->
            {error, connection_closing};
        {error, ChannelError} ->
            ?THUMP(error, "unknown channel error: ~p", [ChannelError]),
            {error, ChannelError};
        Other ->
            ?THUMP(error, "unknown response from init_channel: ~p", [Other]),
            {error, undefined}
    end.

% This function allows the multiple broker connections to reference the
% same connection config. e.g.:
% 
%        {brokers, [
%            {default, [
%                        {rabbitmq_config, [
%                            {host, "------------------"},
%                            {port, 5673},
%                            {virtual_host, <<"/">>},
%                            {user, <<"-------------">>},
%                            {password, <<"">>}, 
%                            {heartbeat, 0},
%                            {ssl_options, [{cacertfile, "----------------"},
%                                           {certfile, "-----------------"},
%                                           {keyfile, "--------------"},
%                                           {verify, verify_peer},
%                                           {fail_if_no_peer_cert, true}
%                                          ]}
%                        ]}
%                ]},
%            {consume_call, [{rabbitmq_config, default}]},      %% Note: these configs reference
%            {consume_message, [{rabbitmq_config, default}]},   %% the same set of connection config
%            {publish, [{rabbitmq_config, default}]}            %% above but create separate broker connections
%            ]},
rabbitmq_config(_, undefined) ->
    ?DefaultConfig;
rabbitmq_config(ThumperConfig, BrokerConfig) ->
    case proplists:get_value(rabbitmq_config, BrokerConfig) of
        undefined ->
            ?DefaultConfig;
        Broker when is_atom(Broker) ->
            case proplists:get_value(Broker, ThumperConfig) of
                undefined ->
                    ?DefaultConfig;
                Config ->
                    rabbitmq_config(ThumperConfig, Config)
            end;
        Proplist when is_list(Proplist) ->
            Proplist
    end.

rabbitmq_config(Name) when is_atom(Name) ->
    ?THUMP(debug, "loading broker config: ~p", [Name]),
    case application:get_env(thumper, brokers) of
        {ok, Proplist} ->
            case proplists:get_value(Name, Proplist) of
                undefined ->
                    ?THUMP(error, "broker config not found", []),
                    undefined;
                Config ->
                    rabbitmq_config(Proplist, Config)
            end;
        _ ->
            undefined
    end.

open(Name) when is_atom(Name) ->
    case rabbitmq_config(Name) of
        undefined ->
            do_connect(default, ?DefaultConfig);
        Config ->
            do_connect(Name, Config)
    end;
open(_) ->
    ?THUMP(error, "open called with invalid argument", []),
    {error, invalid_argument}.

do_connect(Name, undefined) ->
    ?THUMP(error, "rabbitmq_config not found", []),
    do_connect(Name, ?DefaultConfig);
do_connect(Name, Config) ->
    case load_config(Config) of
        {ok, Network, BrokerConfig} ->
            case init_connection(Name, Network) of
                {ok, Handle} ->
                    case init_broker(Handle, BrokerConfig) of
                        ok ->
                            {ok, Handle};
                        BrokerError ->
                            close(Handle),
                            ?THUMP(error, "broker error: ~p", [BrokerError]),
                            {error, config_error}
                    end;
                ConnError ->
                    ?THUMP(error, "conn error: ~p", [ConnError]),
                    {error, conn_error}
            end;
        ConfigError ->
            ?THUMP(error, "config error: ~p", [ConfigError]),
            {error, config_error}
    end.

load_config(Proplist) ->
    case load_rabbitmq_config(Proplist) of
        {ok, Network} ->
            case load_broker_config(Proplist) of
                    {ok, Broker} ->
                {ok, Network, Broker};
                {error, Error} ->
                    {error, Error}
            end;
        {error, RabbitError} ->
            {error, RabbitError}
    end.

load_broker_config(Proplist) ->
    case proplists:get_value(broker_config, Proplist) of
        undefined ->
            case application:get_env(thumper, broker_config) of
                {ok, Config} ->
                    {ok, Config};
                _ ->
                    ?THUMP(error, "broker config not found", []),
                    {error, broker_config_not_found}
            end;
        PropConfig ->
            ?THUMP(info, "found broker config in connection's config", []),
            {ok, PropConfig}
    end.

load_rabbitmq_config(Proplist) ->
    OriginalSslOptions = case proplists:get_value(ssl_options, Proplist) of
        undefined ->
            none;
        Opts ->
            Opts
    end,
    PrivDir = get_priv_dir(),
    SslOptions = get_ssl_options(OriginalSslOptions, PrivDir),
    Network = #amqp_params_network{
        username = proplists:get_value(user, Proplist),
        password = proplists:get_value(password, Proplist),
        port = proplists:get_value(port, Proplist),
        virtual_host = proplists:get_value(virtual_host, Proplist),
        host = proplists:get_value(host, Proplist),
        ssl_options = SslOptions,
        heartbeat = proplists:get_value(heartbeat, Proplist, 30), % this default is required for connection via ELB
        connection_timeout=proplists:get_value(connection_timeout, Proplist, 10000)
    },
    {ok, Network}.

connection_id(Name, Network) ->
    erlang:phash2({Name, Network}).

init_connection(Name, Network) ->
    ?THUMP(info, "init connection to host: ~p", [Network#amqp_params_network.host]),
    Id = connection_id(Name, Network),
    case ets:lookup(?ConnectionTableName, Id) of
        [#broker_connection{pid=undefined}] ->
            ets:delete(?ConnectionTableName, Id),
            init_new_connection(Name, Network);
        [#broker_connection{pid=ConnectionPid, channel_pid=ChannelPid}=Conn] ->
            case is_process_alive(ConnectionPid) of
                true ->
                    Keys = amqp_connection:info_keys(ConnectionPid),
                    ?THUMP(info, "attempt to reuse existing connection", []),
                    Values = amqp_connection:info(ConnectionPid, Keys),
                    %[?THUMP(info, "~p = ~p", [Key, Value]) || {Key, Value} <- Values],
                    case proplists:get_value(is_closing, Values) of
                        true ->
                            ?THUMP(info, "existing connection is closing; create a new one", []),
                            ets:delete(?ConnectionTableName, Id),
                            init_new_connection(Name, Network);
                        _ ->
                            ConnectionRef = ?Monitor(ConnectionPid),
                            ?THUMP(info, "connection ref: ~p", [ConnectionRef]),
                            case ?IsPidAlive(ChannelPid) of
                                true ->
                                    ChannelRef = ?Monitor(ChannelPid),
                                    Handle = #broker_handle{
                                                network=Network,
                                                connection=ConnectionPid,
                                                connection_ref=ConnectionRef,
                                                publish_channel_ref=ChannelRef,
                                                publish_channel=ChannelPid},
                                    {ok, Handle};
                                 _ ->
                                    case thumper_utils:init_channel(ConnectionPid) of
                                        {ok, NewChannelPid} ->
                                            ChannelRef = ?Monitor(ChannelPid),
                                            Handle = #broker_handle{
                                                        network=Network,
                                                        connection=ConnectionPid,
                                                        connection_ref=ConnectionRef,
                                                        publish_channel_ref=ChannelRef,
                                                        publish_channel=NewChannelPid},
                                            ets:insert(?ConnectionTableName, Conn#broker_connection{channel_pid=NewChannelPid}),
                                            {ok, Handle};
                                        {error, ChannelError} ->
                                            {error, ChannelError}
                                    end
                            end
                    end;
                _ ->
                    ets:delete(?ConnectionTableName, Id),
                    init_new_connection(Name, Network)
            end;
        _ ->
            init_new_connection(Name, Network)
    end.

init_new_connection(Name, Network) ->
    try amqp_connection:start(Network) of
        {ok, ConnectionPid} ->
            ConnectionRef = ?Monitor(ConnectionPid),
            ?THUMP(info, "connection ref: ~p", [ConnectionRef]),
            case thumper_utils:init_channel(ConnectionPid) of
                {ok, ChannelPid} ->
                    ?THUMP(info, "connection pid: ~p", [ConnectionPid]),
                    ?THUMP(info, "publish channel pid: ~p", [ChannelPid]),
                    ChannelRef = ?Monitor(ChannelPid),
                    ?THUMP(info, "publish channel ref: ~p", [ChannelRef]),
                    Handle = #broker_handle{
                                network=Network,
                                connection=ConnectionPid,
                                connection_ref=ConnectionRef,
                                publish_channel_ref=ChannelRef,
                                publish_channel=ChannelPid},
                    Id = connection_id(Name, Network),
                    ets:insert(?ConnectionTableName, #broker_connection{id=Id, pid=ConnectionPid, channel_pid=ChannelPid}),
                    {ok, Handle};
                {error, ChannelError} ->
                    {error, ChannelError}
            end;
        {error, ConnError} ->
            ?THUMP(error, "error during init_connection: ~p", [ConnError]),
            {error, ConnError}
    catch
        X:Y ->
            ?THUMP(error, "exception during init_connection: ~p ~p", [X, Y]),
            ?THUMP(error, "~p", [erlang:get_stacktrace()]),
            {error, {X, Y}}
    end.

get_priv_dir() ->
    case code:priv_dir(?MODULE) of
		{error, bad_name} ->
    		EbinDir = filename:dirname(code:which(?MODULE)),
			AppPath = filename:dirname(EbinDir),
			filename:join(AppPath, "priv");
		Path ->
		    Path
    end.

get_ssl_options(none, _) ->
    none;

get_ssl_options(SslOptions, PrivDir) ->
    % If the certs paths are relative, make them absolute from the app root.
    ParentDir = filename:dirname(PrivDir),
    PathKeys = [cacertfile, certfile, keyfile],
    NewSslOptions = lists:foldl(fun(Elem, Acc) -> make_full_path(Elem, SslOptions, Acc, ParentDir) end, [], PathKeys),
    % Add the original non-path keys into the new options proplist.
    AllKeys = proplists:get_keys(SslOptions),
    NonPathKeys = lists:subtract(AllKeys, PathKeys),
    lists:foldl(fun(Key, Acc) -> Value = proplists:get_value(Key, SslOptions), [{Key, Value}|Acc] end, NewSslOptions, NonPathKeys).

init_broker(_Handle, {thumper_tx, []}) ->
    ok;
init_broker(Handle, {thumper_tx, [H|T]}) when is_list(H) ->
    case init_broker(Handle, {thumper_tx, H}) of
        ok ->
            init_broker(Handle, {thumper_tx, T});
        E ->
            E
    end;
init_broker(#broker_handle{connection=Connection}, {thumper_tx, File}) ->
    case thumper_tx:consult(File) of
        {ok, Config} ->
            case thumper_utils:init_channel(Connection) of
                {ok, Channel} ->
                    case thumper_tx:run(Config, Channel) of
                        {ok, _} ->
                            thumper_utils:close_channel(Channel),
                            ok;
                        E ->
                            thumper_utils:close_channel(Channel),
                            E
                    end;
                E ->
                    E
            end;
        E ->
            E
    end;
init_broker(Handle, Proplist) ->
    Channel = Handle#broker_handle.publish_channel,
    ExchangeList = proplists:get_all_values(exchange, Proplist),
    case init_exchange(Channel, ExchangeList) of
        ok ->
            QueueList = proplists:get_all_values(queue, Proplist),
            case init_queue(Channel, QueueList) of
                ok ->
                    BindingList = proplists:get_all_values(binding, Proplist),
                    init_binding(Channel, BindingList);
                QErr ->
                    QErr
            end;
        XErr ->
            XErr
    end.

init_binding(_Channel, []) ->
    ok;
init_binding(Channel, [PL|T]) ->
    Exchange = proplists:get_value(exchange, PL),
    Queue = proplists:get_value(queue, PL),
    RK = proplists:get_value(routing_key, PL),
    try thumper_utils:bind_queue(Channel, Queue, Exchange, RK) of
        {ok, _Binding} ->
            ?THUMP(info, "created binding from queue ~p to exchange ~p with routing key ~p", [Queue, Exchange, RK]),
            ok;
        _ ->
            ?THUMP(error, "error creating binding from ~p", [PL]),
            ok
    catch
        A:B ->
            ?THUMP(error, "exception creating binding from ~p : ~p ~p", [PL, A, B]),
            ok
    end,
    init_binding(Channel, T);
init_binding(_Channel, B) ->
    ?THUMP(error, "cannot init binding from invalid config: ~p", [B]),
    ok.

init_queue(Channel, [QueueProplist|T]) ->
    case thumper_utils:declare_queue(Channel, QueueProplist) of
        {ok, _QName} ->
            init_queue(Channel, T);
        QErr ->
            QErr
    end;

init_queue(_Channel, []) ->
    ok;

init_queue(_Channel, undefined) ->
    ok;

init_queue(_Channel, _) ->
    {error, invalid_queue_config}.

init_exchange(Channel, [ExchangeProplist|T]) ->
    case thumper_utils:declare_exchange(Channel, ExchangeProplist) of
        {ok, _XName} ->
            init_exchange(Channel, T);
        XErr ->
            XErr
    end;

init_exchange(_Channel, []) ->
    ok;

init_exchange(_Channel, undefined) ->
    ok;

init_exchange(_Channel, _) ->
    {error, invalid_exchange_config}.

make_full_path(Key, SslOptions, Acc, ParentDir) ->
    case proplists:get_value(Key, SslOptions) of
        undefined ->
            Acc;
        Path ->
            AbsPath = get_abs_path(ParentDir, Path),
            [{Key, AbsPath}|Acc]
    end.

get_abs_path(ParentDir, Path) ->
    case lists:nth(1, Path) of
        47 -> % forward slash
            % Already an absolute path.  No modification necessary.
            Path;
        _ ->
            filename:join(ParentDir, Path)
    end.

