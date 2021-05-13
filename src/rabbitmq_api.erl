-module(rabbitmq_api).

-export([to_admin_config/1, create_exchange/3, delete_exchange/2, delete_queue/2, get_queue/2,
         get_all_queues/1, get_queue_bindings/2, find_queues/2, find_queue_bindings/3]).
-export([get/2, get/3, delete/2, delete/3, put/3, put/4, post/3, post/4]).

-record(url,{protocol,user,password,host,port,path}). 

-include("../include/thumper.hrl").

to_admin_config(Config) ->
    proplists:delete(ssl_options, Config).

create_exchange(ConfigName, Name, Args) when is_atom(ConfigName) ->
    Config = get_config(ConfigName),
    create_exchange(Config, Name, Args);

create_exchange(Config, Name, Args) when is_list(Config) ->
    VHost = vhost(Config),
    Path = "/api/exchanges/" ++ ibrowse_lib:url_encode(VHost) ++ "/" ++ binary_to_list(Name),
    Headers = [{"Content-Type", "application/json"}],
    case put(Config, Path, Args, Headers) of
        {ok, "204", _, _} ->
            ok;
        Error ->
            ?THUMP(error, "Failed to create exchange: ~p", [Error]),
            {error, create_failed}
    end.

delete_exchange(ConfigName, Name) when is_binary(Name), is_atom(ConfigName) ->
    Config = get_config(ConfigName),
    delete_exchange(Config, Name);

delete_exchange(Config, Name) when is_binary(Name), is_list(Config) ->
    VHost = binary_to_list(proplists:get_value(virtual_host, Config)),
    Path = "/api/exchanges/" ++ ibrowse_lib:url_encode(VHost) ++ "/" ++ binary_to_list(Name),
    api_delete_path(Config, Path);

delete_exchange(_Config, _Name) ->
    {error, invalid_arg}.

delete_queue(ConfigName, Name) when is_binary(Name), is_atom(ConfigName) ->
    Config = get_config(ConfigName),
    delete_queue(Config, Name);

delete_queue(Config, Name) when is_binary(Name), is_list(Config) ->
    VHost = binary_to_list(proplists:get_value(virtual_host, Config)),
    Path = "/api/queues/" ++ ibrowse_lib:url_encode(VHost) ++ "/" ++ binary_to_list(Name),
    api_delete_path(Config, Path);

delete_queue(_Config, _Name) ->
    {error, invalid_arg}.

get_queue(Config, Name) ->
    VHost = vhost(Config),
    Path = "/api/queues/" ++  ibrowse_lib:url_encode(VHost) ++ "/" ++ binary_to_list(Name),
    Headers = [{"Content-Type", "application/json"}],
    case get(Config, Path, Headers) of
        {ok, "200", _, Response} ->
            {ok, Response};
        GetError ->
            GetError
    end.

get_all_queues(Config) ->
    VHost = vhost(Config),
    Path = "/api/queues/" ++ ibrowse_lib:url_encode(VHost),
    Headers = [{"Content-Type", "application/json"}],
    case get(Config, Path, Headers) of
        {ok, "200", _, Response} ->
            {ok, Response};
        {ok, Code, _, _} ->
            {error, Code};
        GetError={error,_} ->
            GetError;
        Else ->
            {error, Else}
    end.

get_queue_bindings(Config, Queue) ->
    VHost = vhost(Config),
    Path = "/api/queues/" ++ ibrowse_lib:url_encode(VHost) ++ "/" ++
            ibrowse_lib:url_encode(Queue) ++ "/bindings",
    Headers = [{"Content-Type", "application/json"}],
    case get(Config, Path, Headers) of
        {ok, "200", _, Response} ->
            {ok, Response};
        {ok, Code, _, _} ->
            {error, Code};
        GetError={error,_} ->
            GetError;
        Else ->
            {error, Else}
    end.

vhost(Config) ->
    binary_to_list(proplists:get_value(virtual_host, Config)).

api_delete_path(Config, Path) ->
    case rabbitmq_api:get(Config, Path) of
        {ok, "404", _, _} ->
            ?THUMP(info, "cannot delete nonexistent resource: ~p", [Path]),
            ok;
        {ok, "200", _, _} ->
            ?THUMP(info, "deleting resource: ~p", [Path]),
            case rabbitmq_api:delete(Config, Path) of
                {ok,"204", _, _} ->
                    ok;
                DeleteError ->
                    ?THUMP(error, "unexpected DELETE response: ~p~n  path: ~p~nconfig: ~p", 
                        [DeleteError, Path, Config]),
                    DeleteError
            end;
        % Handle permanent redirect so we can run this code on RabbitMQ 2.8.* and 3.*.
        % Set an api_port value based on the redirect location. 
        {ok, "301", ResponseHeaders, _} ->
            Location = proplists:get_value("Location", ResponseHeaders),
            #url{port=Port} = parse_url(Location),
            ?THUMP(info, "delete request redirected... set new api_port value to ~s", [Port]),
            Config2 = proplists:delete(api_port, Config),
            Config3 = lists:append(Config2, [{api_port, list_to_integer(Port)}]),
            api_delete_path(Config3, Path);
            %api_request_url(Config, Location, [], get, "");
        GetError ->
            ?THUMP(error, "unexpected GET response: ~p~n config: ~p", [GetError, Config]),
            GetError
    end.

post(Config, Path, Body) ->
    post(Config, Path, Body, []).

post(Config, Path, Body, Headers) ->
    api_request(Config, Path, Headers, post, Body).

put(Config, Path, Body) ->
    put(Config, Path, Body, []).

put(Config, Path, Body, Headers) ->
    api_request(Config, Path, Headers, put, Body).

delete(Config, Path) ->
    delete(Config, Path, []).

delete(Config, Path, Headers) ->
    api_request(Config, Path, Headers, delete, "").

get(Config, Path) ->
    get(Config, Path, []).

get(Config, Path, Headers) ->
    api_request(Config, Path, Headers, get, "").

api_request(Config, Path, Headers, Method, Data) when is_atom(Config) ->
    case application:get_env(thumper, Config) of
        {ok, Proplist} ->
            api_request(Proplist, Path, Headers, Method, Data);
        _ ->
            ?THUMP(error, "config not found: ~p", [Config]),
            {error, config_not_found}
    end;

api_request(Config, Path, Headers, Method, Data) when is_list(Config) ->
    Protocol = case proplists:get_value(ssl_options, Config) of
        undefined ->
            "http";
        _ ->
            "https"
    end,
    Port = case proplists:get_value(api_port, Config) of
        undefined ->
            55672;
        Value ->
            Value
    end,
    Host = proplists:get_value(host, Config),
    Url = Protocol ++ "://" ++ Host ++ ":" ++ ?Str(Port) ++ Path,
    api_request_url(Config, Url, Headers, Method, Data);

api_request(_Config, _Path, _Headers, _Method, _Data) ->
    {error, invalid_config}.

api_request_url(Config, Url, Headers, Method, Data) ->
    Username = binary_to_list(proplists:get_value(user, Config)),
    Password = binary_to_list(proplists:get_value(password, Config)),
    UserPass = Username ++ ":" ++ Password,
    BasicAuth = "Basic " ++ binary_to_list(base64:encode(UserPass)),
    AllHeaders = [{"Authorization", BasicAuth} | Headers],
    ibrowse:send_req(Url, AllHeaders, Method, Data).

get_config(Name) ->
    case application:get_env(thumper, Name) of
        {ok, Proplist} ->
            Proplist;
        _ ->
            ?THUMP(error, "config not found", []),
            {error, config_not_found}
    end.

% http://www.trapexit.org/forum/viewtopic.php?p=64377&sid=6f5d785e74d8daa516170c042291d682
parse_url(Url) -> 
    {ok,RE}= re:compile("^(http[s]?://)?([a-z0-9]+(:[^@]+)?@)?([a-z0-9\.\-]+(:[0-9]+)?)([/].*)?$",[caseless]), 
    case re:run(Url, RE, [global, {capture, all, list}]) of 
        {match,[[_Match,Scheme,UserPass,Password,Host]]} -> 
            Protocol = re:replace(Scheme,"://","",[{return,list}]), 
            User = re:replace(UserPass,Password++"@","",[{return,list}]), 
            #url{protocol=Protocol,user=User,password=Password,host=Host,port="80",path=""}; 
        {match,[[_Match,Scheme,UserPass,Password,UrlHost,UrlPort]]} -> 
            Protocol = re:replace(Scheme,"://","",[{return,list}]), 
            User = re:replace(UserPass,Password++"@","",[{return,list}]), 
            Host = re:replace(UrlHost,":[0-9]+$","",[{return,list}]), 
            Port = re:replace(UrlPort,":","",[{return,list}]), 
            #url{protocol=Protocol,user=User,password=Password,host=Host,port=Port,path=""}; 
        {match,[[_Match,Scheme,UserPass,Password,UrlHost,UrlPort,Path]]} -> 
            Protocol = re:replace(Scheme,"://","",[{return,list}]), 
            User = re:replace(UserPass,Password++"@","",[{return,list}]), 
            Host = re:replace(UrlHost,":[0-9]+$","",[{return,list}]), 
            Port = re:replace(UrlPort,":","",[{return,list}]), 
            #url{protocol=Protocol,user=User,password=Password,host=Host,port=Port,path=Path}; 
        nomatch -> 
            #url{} 
    end.

find_queues(Config, QueuePattern) ->
    case get_all_queues(Config) of
        {ok, Queues} ->
            Queues2 = lists:filter(fun({struct, Q}) ->
                        Name = proplists:get_value(<<"name">>, Q, <<"">>),
                        case re:run(Name, QueuePattern, [{capture, none}, dotall]) of
                            match -> true;
                            nomatch -> false
                        end
                end, Queues),
            {ok, Queues2};
        Err ->
            Err
    end.

find_queue_bindings(Config, QueuePattern, SrcExchangePattern) ->
    case find_queues(Config, QueuePattern) of
        {ok, Queues} ->
            BindingMatches = filtermap(
                fun({struct, Q}) ->
                    Name = proplists:get_value(<<"name">>, Q, <<"undefined">>),
                    case get_queue_bindings(Config, binary_to_list(Name)) of
                        {ok, Bindings} ->
                            Bindings2 = lists:filter(fun({struct, B}) ->
                                        Src = proplists:get_value(<<"source">>, B, <<"">>),
                                        case re:run(Src, SrcExchangePattern, [{capture, none}, dotall]) of
                                            match -> true;
                                            nomatch -> false
                                        end
                                end, Bindings),
                            {true, Bindings2};
                        _ ->
                            false
                    end
            end, Queues),
            {ok, lists:append(BindingMatches)};
        Err ->
            Err
    end.

%% ugly hack for backwards compatibility
filtermap(Fun, List1) ->
    lists:foldr(fun(Elem, Acc) ->
                       case Fun(Elem) of
                           false -> Acc;
                           true -> [Elem|Acc];
                           {true,Value} -> [Value|Acc]
                       end
                end, [], List1).
