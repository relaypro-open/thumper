
-define(SubscriberTableName, thumper_subscriber_table). % stores each subscriber record by pid
-define(ChannelTableName, thumper_channel_table). % lookup of channel pid to subscriber pid
-define(ConnectionTableName, thumper_connection_table). % tracks connections beyond thumper gen_server crashes

-record(subscriber, {pid,
                     ref,
                     channel,
                     channel_ref,
                     consumer_tag,
                     broker_name=default,
                     queue_name,
                     exchange_name,
                     routing_key,
                     queue_config,
                     create=false}).

-define(QueueJournalDirDefault, "/tmp/thumper/queuejournal"). % TODO: Multiple apps on the same box must point to different directories to avoid collisions
-define(MemQueueMaxDefault, 10000).
-define(QueueJournalOptsDefault, [{enabled, false}, {dir, ?QueueJournalDirDefault},{memqueue_max, ?MemQueueMaxDefault},{check_journal,true}]).

-ifdef(EUNIT). 
-include_lib("eunit/include/eunit.hrl").
-define(THUMP(Level, Msg), ?debugFmt("[~p] "++Msg, [Level])).
-define(THUMP(Level, Msg, Args), ?debugFmt("[~p] "++Msg, [Level|Args])).
-else.
-define(THUMP(Level, Msg), io:format("[~p] "++Msg++"~n", [Level])).
-define(THUMP(Level, Msg, Args), io:format("[~p] "++Msg++"~n", [Level|Args])).
-endif.

-define(NOW(), os:timestamp()).

-define(Str(X), thumper_utils:str(X)).
-define(Bin(X), thumper_utils:bin(X)).
-define(Atom(X), thumper_utils:atom(X)).

-record(broker_handle, {network,
                        connection, 
                        connection_ref, 
                        publish_channel, 
                        publish_channel_ref,
                        reconnect_timer_ref}).

-record(state, {broker_name=default,
                broker_handle=undefined,
                status=unavailable :: available | unavailable,
                subscriber_tid=undefined,
                channel_tid=undefined,
                publish_queue=array:new(),
                queuejournal_opts=?QueueJournalOptsDefault,
                return_callbacks=gb_trees:empty()
            }).

-define(IsPidAlive(Pid), 
        case Pid of
            undefined ->
                false;
            _ ->
                is_pid(Pid) and is_process_alive(Pid)
        end).

-define(Demonitor(Ref),
        case is_reference(Ref) of
            true ->
                erlang:demonitor(Ref);
            _ ->
                ok
        end).

-define(Monitor(Pid), thumper_utils:monitor(Pid)).

-record(broker_connection, {
          id, % portable hash of the amqp_params_network record (using phash2)
          pid :: undefined | pid(),
          channel_pid :: undefined | pid()
         }).

