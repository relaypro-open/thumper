-module(thumper_tests).
-include_lib("eunit/include/eunit.hrl").

-define(Exchange, <<"exchange">>).
-define(Queue, <<"queue">>).
-define(ConsumeTimeout, 60000).

-include_lib("thumper/include/thumper.hrl").

-define(_assertSeqPubsub(Broker, Exchange, RK, Queue, N), begin
    PubOk = lists:duplicate(N, ok),
    [?_assertMatch(PubOk, seqpub(Broker, Exchange, RK, N)),
     ?_assertMatch({ok, N}, consume_n_count(Broker, Queue, N))]
                                                          end).

pubsub_test_() -> {setup, fun setup/0, fun cleanup/1,
                   fun(Data) ->
                           [
                            {timeout, 600, lists:flatten(pubsub(Data))}
                           ]
                   end}.

setup() ->
    application:load(thumper),
    application:set_env(thumper, thumper_svrs, []),

    {ok, _} = application:ensure_all_started(erlexec),
    {ok, _} = application:ensure_all_started(thumper),

    {ok, Server} = rmqs:start_link(#{port => 0}),
    unlink(Server),
    Port = rmqs:get_port(Server),
    true = tcp_reachable(Port, 60000),

    RmqConfig = [{host, "127.0.0.1"},
                 {port, Port},
                 {user, <<"guest">>},
                 {password, <<"guest">>},
                 {virtual_host, <<"/">>}],

    application:set_env(thumper, queuejournal, [{enabled, true},
                                                {memqueue_max, 10000},
                                                {check_journal, true},
                                                {dir, rmqs:queuejournal_dir()}]),

    application:set_env(thumper, brokers, [{default, [{rabbitmq_config, RmqConfig}]},
                                           {local, [{rabbitmq_config, RmqConfig}]}]),

    ok = thumper_tx:run(
           [{rabbitmq_config, RmqConfig},
            {tx, [
                  {'exchange.declare', [
                                        {exchange, ?Exchange},
                                        {type, <<"fanout">>},
                                        {durable, false}
                                       ]},
                  {'queue.declare', [
                                     {queue, ?Queue},
                                     {durable, false},
                                     {auto_delete, false}
                                    ]},
                  {'queue.bind', [
                                  {queue, ?Queue},
                                  {exchange, ?Exchange},
                                  {routing_key, <<>>}
                                 ]}
                 ]
            }]),

    supervisor:start_child(thumper_sup, thumper_sup:child_spec(default)),
    supervisor:start_child(thumper_sup, thumper_sup:child_spec(local)),

    timer:sleep(1000),

    %% make sure we have connections
    #state{broker_handle=#broker_handle{}} = thumper:get_state(default),
    #state{broker_handle=#broker_handle{}} = thumper:get_state(local),

    #{rmqs => Server}.

cleanup(#{rmqs := Server}) ->
    case is_process_alive(Server) of 
        true ->
            ok = rmqs:stop(Server);
        _ ->
            ok
    end.

pubsub(#{rmqs := Server}) ->
    [?_assertSeqPubsub(local, ?Exchange, <<>>, ?Queue, 10)].

seqpub(Broker, Exchange, RK, N) ->
    Messages = [ #{x => X, t => erlang:monotonic_time(microsecond)} || X <- lists:seq(1, N) ],
    [ thumper:publish_to(Broker, term_to_binary(M), Exchange, RK) || M <- Messages ].

consume_n_count(Broker, Queue, N) ->
    case consume_n(Broker, Queue, N) of
        {ok, Messages} ->
            {ok, length(Messages)};
        Err ->
            Err
    end.

consume_n(Broker, Queue, N) ->
    Ref = make_ref(),
    Me = self(),
    Callback = fun(DT, RK, Payload) ->
                       Me ! {Ref, {DT, RK, Payload}},
                       ack
               end,
    {ok, ConsumerPid} = thumper_consumer:start_link(undefined, Broker, Queue, Callback),
    ?debugFmt("created consumer ~p", [ConsumerPid]),
    fun Rcv(0, Acc) ->
            unlink(ConsumerPid),
            thumper_consumer:stop(ConsumerPid),
            {ok, lists:reverse(Acc)};
        Rcv(N0, Acc) ->
            receive
                {Ref, {_DT, _RK, Payload}} ->
                    Rcv(N0-1, [binary_to_term(Payload)|Acc])
            after ?ConsumeTimeout ->
                      {error, timeout}
            end
    end(N, []).

%% =========================
tcp_reachable(_, T) when T =< 0 ->
    false;
tcp_reachable(Port, Timeout) ->
    Host = "127.0.0.1",
    case gen_tcp:connect(Host, Port, []) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            true;
        _ ->
            timer:sleep(1000),
            tcp_reachable(Port, Timeout-1000)
    end.
