-module(rabbitmq_api_queue).

-export([messages_ready/2, messages/2]).

messages_ready(Config, Queue) ->
    AConfig = rabbitmq_api:to_admin_config(Config),
    case rabbitmq_api:get_queue(AConfig, Queue) of
        {struct, R} ->
            Ready = proplists:get_value("messages_ready", R, 0),
            {struct, Details} = proplists:get_value("messages_ready_details", R, {struct, []}),
            Details2 = [ {list_to_atom(X), Y} || {X,Y} <- Details],
            {ok, [{messages_ready, Ready}|Details2]};
        {error, Error} ->
            {error, Error};
        Error ->
            {error, Error}
    end.

messages(Config, Queue) ->
    AConfig = rabbitmq_api:to_admin_config(Config),
    case rabbitmq_api:get_queue(AConfig, Queue) of
        {struct, R} ->
            Ready = proplists:get_value("messages", R, 0),
            {struct, Details} = proplists:get_value("messages_details", R, {struct, []}),
            Details2 = [ {list_to_atom(X), Y} || {X,Y} <- Details],
            {ok, [{messages, Ready}|Details2]};
        {error, Error} ->
            {error, Error};
        Error ->
            {error, Error}
    end.
