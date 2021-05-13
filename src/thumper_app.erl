-module(thumper_app).

-behaviour(application).

-export([start/0, stop/0, start/2, stop/1]).

start() ->
    application:ensure_all_started(thumper).

stop() ->
    application:stop(thumper).

start(_StartType, _StartArgs) ->
    thumper_app_sup:start_link().

stop(_State) ->
    ok.
