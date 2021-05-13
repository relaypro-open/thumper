-module(thumper_state).

-export([status/1]).
-include("../include/thumper.hrl").

status(ConfigName) ->
    Ref = thumper_utils:get_thumper_svr(ConfigName),
    try grab_state(Ref) of
        {ok, #state{status=Status}} ->
            {ok, Status};
        {ok, _} ->
            {error, unknown_state};
        Er ->
            Er
        catch _:_ ->
            {error, exception}
    end.

grab_state(Ref) ->
    case sys:get_status(Ref) of
        {status, _Pid, _Module, [_PDict, _SysState, _Parent, _Dbg, Misc]} ->
            case proplists:get_all_values(data, Misc) of
                DataLists = [[_|_]|_] ->
                    DataList = lists:flatten(DataLists),
                    case proplists:get_value("State", DataList) of
                        undefined ->
                            {error, no_state};
                        State ->
                            {ok, State}
                    end;
                _ ->
                    {error, no_data}
            end;
        _ ->
            {error, status_failure}
    end.
