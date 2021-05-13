-module(thumper_sup).

-behaviour(supervisor).

-export([start_link/0, init/1, child_spec/1]).

-include("../include/thumper.hrl").

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(THUMPER_CHILD(I, ConfigName, Type), {?Atom(?Str(I)++"_"++?Str(ConfigName)), {I, start_link, [ConfigName]}, permanent, 5000, Type, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ets:new(?SubscriberTableName, [named_table, public, set, {keypos, 2}]),
    ets:new(?ChannelTableName, [named_table, public, set]),
    ets:new(?ConnectionTableName, [named_table, public, set, {keypos, 2}]),
    DefaultSvr = ?THUMPER_CHILD(thumper, default, worker),
    Svrs = case application:get_env(thumper, thumper_svrs) of
        {ok, V} -> [ ?THUMPER_CHILD(thumper, X, worker) || X <- V ];
        _ -> [DefaultSvr]
    end,
    {ok, { {one_for_one, 5, 10}, Svrs} }.

child_spec(ConfigName) ->
    ?THUMPER_CHILD(thumper, ConfigName, worker).
