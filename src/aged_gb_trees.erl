-module(aged_gb_trees).

-export([empty/0,
        insert/3,
        iterator_before/2,
        next/1,
        size/1,
        iter_remove/2,
        iter_foreach/2,
        iter_fold/3,
        micros_to_timestamp/1]).

-define(MaxNow, {99999,0,0}).

-compile(nowarn_export_all).
-compile([export_all]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Note: taken from OTP Erlang version 18.0
iterator_from(S, {_, T}) ->
    iterator_1_from(S, T).

iterator_1_from(S, T) ->
    iterator_from(S, T, []).

iterator_from(S, {K, _, _, T}, As) when K < S ->
    iterator_from(S, T, As);
iterator_from(_, {_, _, nil, _} = T, As) ->
    [T | As];
iterator_from(S, {_, _, L, _} = T, As) ->
    iterator_from(S, L, [T | As]);
iterator_from(_, nil, As) ->
    As.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

empty() ->
    gb_trees:empty().

insert(Timestamp, Term, GB) ->
    Key = timestamp_to_expkey(Timestamp),
    gb_trees:insert(Key, Term, GB).

iterator_before(Timestamp, GB) ->
    ExpKey = timestamp_to_expkey(Timestamp),
    iterator_from(ExpKey, GB).

next(Iter) ->
    gb_trees:next(Iter).

size(GB) ->
    gb_trees:size(GB).

iter_foreach(Fun, Iter) ->
    case gb_trees:next(Iter) of
        none ->
            ok;
        {K, V, Iter2} ->
            Fun({K, V}),
            iter_foreach(Fun, Iter2)
    end.

iter_fold(Fun, Accum, Iter) ->
    case gb_trees:next(Iter) of
        none ->
            Accum;
        {K, V, Iter2} ->
            Accum2 = Fun({K, V}, Accum),
            iter_fold(Fun, Accum2, Iter2)
    end.

iter_remove(Iter, GB) ->
    Keys = lists:reverse(iter_fold(fun({K,_V}, A) -> [K|A] end, [], Iter)),
    lists:foldl(fun gb_trees:delete_any/2, GB, Keys).

micros_to_timestamp(M) ->
    Mega = M div (1000*1000*1000*1000),
    Rem1 = M rem (1000*1000*1000*1000),
    Sec = Rem1 div (1000*1000),
    Rem2 = Rem1 rem (1000*1000),
    Micros = Rem2,
    {Mega, Sec, Micros}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

timestamp_to_expkey(Ts) ->
    Micros = timer:now_diff(?MaxNow, Ts),
    Micros.

expkey_to_timestamp(Key) ->
    Ts = micros_to_timestamp(Key),
    Micros = timer:now_diff(?MaxNow, Ts),
    micros_to_timestamp(Micros).
