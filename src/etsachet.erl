%%% @author Vladimir G. Sekissov <eryx67@gmail.com>
%%% @copyright (C) 2013, Vladimir G. Sekissov
%%% @doc
%%% Simple ETS based cache. Inspired by Cadfaerl, git://github.com/ddossot/cadfaerl.git
%%% @end
%%% Created :  1 Jul 2013 by Vladimir G. Sekissov <eryx67@gmail.com>

-module(etsachet).

-behaviour(gen_server).

-export([start_link/1, start_link/2, stop/1,
         put/3, put_ttl/4,
         get/2, get/3,
         remove/2,
         cache_size/1, reset/1
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_TTL, 356000 * 86400).
-define(STATE_MOD(CN), list_to_atom(atom_to_list(CN) ++ "_config$")).

-ifdef(TEST).
-define(EXPIRE_SIZE_THRESHOLD, 1).
-else.
-define(EXPIRE_SIZE_THRESHOLD, 1000).
-endif.

-record(cache_state, {name, data_store, expire_store, max_size}).
-record(state, {cache_state, cleaner_pid}).

-type undefined() :: 'undefined'.

%% @doc Start a cache with no maximum size.
-spec start_link(atom()) -> {ok, pid()} | ignore | {error, term()}.
start_link(CacheName) when is_atom(CacheName) ->
    start_link(CacheName, undefined).

%% @doc Start a cache with a defined maximum size.
-spec start_link(atom(), integer()) -> {ok, pid()} | ignore | {error, term()}.
start_link(CacheName, MaxSize) when is_atom(CacheName)
                                    andalso (MaxSize =:= undefined
                                             orelse is_integer(MaxSize) andalso MaxSize > 0) ->
    gen_server:start_link({local, CacheName}, ?MODULE, {CacheName, MaxSize}, []).

%% @doc Stop a cache.
-spec stop(atom()) -> ok.
stop(CacheName) when is_atom(CacheName) ->
    gen_server:cast(CacheName, stop).

%% @doc Put a non-expirabled value.
-spec put(atom(), term(), term()) -> ok.
put(CacheName, Key, Value) when is_atom(CacheName) ->
    put_ttl(CacheName, Key, Value, undefined).

%% @doc Put an expirable value with a time to live in seconds.
-spec put_ttl(atom(), term(), term(), integer() | undefined()) -> ok.
put_ttl(CacheName, Key, Value, TTL) when is_atom(CacheName),
                                         (TTL == undefined orelse is_integer(TTL)) ->
    State = find_state(CacheName),
    put_data(Key, Value, expire_at(TTL), State),
    ok.

%% @doc Get a value, returning undefined if not found.
-spec get(atom(), term()) -> term() | undefined().
get(CacheName, Key) when is_atom(CacheName) ->
    Ref = make_ref(),
    case get(CacheName, Key, Ref) of
        Ref ->
            undefined;
        Val ->
            {ok, Val}
    end.

%% @doc Get a value, returning the specified default value if not found.
-spec get(atom(), term(), term()) -> term().
get(CacheName, Key, Default) when is_atom(CacheName) ->
    State = find_state(CacheName),
    get_data(Key, Default, State).

remove(CacheName, Key) when is_atom(CacheName) ->
    State = find_state(CacheName),
    del_data(Key, State),
    ok.

%% @doc Size of the cache, in number of stored elements.
-spec cache_size(atom()) -> integer().
cache_size(CacheName) when is_atom(CacheName) ->
    State = find_state(CacheName),
    data_size(State).

%% @doc Reset the cache, losing all its content.
-spec reset(atom()) -> ok.
reset(CacheName) when is_atom(CacheName) ->
    State = find_state(CacheName),
    data_reset(State),
    ok.

%% gen_server callbacks
init({Name, MaxSize}) ->
    DataTbl = data_table(Name),
    ExpTbl = expire_table(Name),
    ets:new(DataTbl, [set, named_table, public,
                      {read_concurrency, true},
                      {write_concurrency, true}]),
    ets:new(ExpTbl, [ordered_set, named_table, public]),

    CacheState = #cache_state{name=Name, data_store=DataTbl, expire_store=ExpTbl,
                              max_size=MaxSize},
    ConfMod = ?STATE_MOD(Name),
    {ok, ConfMod, Bin} = etsachet_gen_config:generate(ConfMod, CacheState),
    case code:which(hipe) of
        non_existing ->
            ok;
        _ ->
            [hipe:compile(ConfMod, [], Bin, [o3, load]) || code:is_module_native(?MODULE)]
    end,

    {ok, #state{cache_state=CacheState}}.

handle_call({expire_size}, _From, S=#state{cleaner_pid=undefined, cache_state=CS}) ->
    #cache_state{max_size=MS} = CS,
    DS = data_size(CS),
    S1 = if (DS - ?EXPIRE_SIZE_THRESHOLD) >= MS ->
                 S#state{cleaner_pid=spawn_cleaner(CS)};
            true ->
                 S
         end,
    {reply, ok, S1};
handle_call({cache_state}, _From, State=#state{cache_state=CS}) ->
    {reply, {ok, CS}, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _, _, Pid, normal}, S=#state{cleaner_pid=Pid}) ->
    {noreply, S#state{cleaner_pid=undefined}};
handle_info({'DOWN', _, _, Pid, Reason}, S=#state{cleaner_pid=Pid}) ->
    ?LOG_ERROR("abnormal worker termination, reason ~w", [Reason]),
    {noreply, S#state{cleaner_pid=undefined}};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_, _S=#state{cache_state=#cache_state{name=N}}) ->
    code:purge(?STATE_MOD(N)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal functions
spawn_cleaner(#cache_state{data_store=DS, expire_store=ES}) ->
    CleanF = fun () ->
                     {Recs, _} = ets:select(ES, ets:fun2ms(fun (Rec) -> Rec end),
                                            ?EXPIRE_SIZE_THRESHOLD),
                     lists:foreach(fun ({ExpKey, DataKey}) ->
                                           ets:delete(ES, ExpKey),
                                           ets:delete(DS, DataKey)
                                   end, Recs)
             end,
    {Pid, _} = spawn_monitor(CleanF),
    Pid.

find_state(CacheName) ->
    Mod = ?STATE_MOD(CacheName),
    apply(Mod, config, []).

get_data(Key, Default, _S=#cache_state{data_store=DS,
                                       expire_store=ES}) ->
    Now = timestamp(),
    case ets:lookup(DS, Key) of
        [{_, _, ExpAt}] when ExpAt < Now ->
            ets:delete(ES, ExpAt),
            ets:delete(DS, Key),
            Default;
        [{_, Val, _}] ->
            Val;
        [] ->
            Default
    end.

put_data(Key, Value, ExpireAt, S=#cache_state{name=Name,
                                              max_size=MS,
                                              data_store=DS,
                                              expire_store=ES}) ->
    Data = {Key, Value, ExpireAt},
    case ets:insert_new(DS, Data) of
        false ->
            try
                OldExp = ets:lookup_element(DS, Key, 3),
                ets:delete(ES, OldExp),
                ets:insert(DS, Data)
            catch
                exit:badarg ->
                    ok
            end;
        true ->
            case MS of
                undefined ->
                    ok;
                _ ->
                    DSize = data_size(S),
                    if (DSize - ?EXPIRE_SIZE_THRESHOLD) >= MS ->
                            gen_server:call(Name, {expire_size});
                       true ->
                            ok
                    end
            end
    end,
    ets:insert(ES, {ExpireAt, Key}),
    S.

del_data(Key, _S=#cache_state{data_store=DS,
                              expire_store=ES}) ->
    try
        OldExp = ets:lookup_element(DS, Key, 3),
        ets:delete(ES, OldExp),
        ets:delete(DS, Key)
    catch
        exit:badarg ->
            ok
    end.

data_reset(_S=#cache_state{data_store=DS,
                           expire_store=ES}) ->
    ets:delete_all_objects(DS),
    ets:delete_all_objects(ES).

data_size(_S=#cache_state{data_store=DS}) ->
    ets:info(DS, size).

data_table(Name) ->
    list_to_atom(atom_to_list(Name) ++ "_data").

expire_table(Name) ->
    list_to_atom(atom_to_list(Name) ++ "_clock").

expire_at(undefined) ->
    expire_at(?DEFAULT_TTL);
expire_at(TTL) when is_integer(TTL) ->
    timestamp_add(timestamp(), TTL).

timestamp() ->
    erlang:monotonic_time(nanosecond).

timestamp_add(Time, AddSec) ->
    Time + AddSec * 1000000000.

%%--------------------------
%%Tests
%%--------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(SPEED_TEST_OP_COUNT, 100000).

basic_put_get_remove_stats_test() ->
    ?debugMsg("running put/get/remove/stats tests"),

    {ok, _Pid} = start_link(basic_cache),
    ?assertEqual(undefined, get(basic_cache, my_key)),
    ?assertEqual(0, cache_size(basic_cache)),

    ?assertEqual(ok, put(basic_cache, my_key, "my_val")),
    ?assertEqual(1, cache_size(basic_cache)),

    ?assertEqual({ok, "my_val"}, get(basic_cache, my_key)),

    ?assertEqual(ok, put(basic_cache, my_key, <<"other_val">>)),
    ?assertEqual(1, cache_size(basic_cache)),

    ?assertEqual({ok, <<"other_val">>}, get(basic_cache, my_key)),
    ?assertEqual(1, cache_size(basic_cache)),

    ?assertEqual(ok, remove(basic_cache, my_key)),
    ?assertEqual(undefined, get(basic_cache, my_key)),
    ?assertEqual(0, cache_size(basic_cache)),
    ok = stop(basic_cache).

ttl_put_get_test() ->
    ?debugMsg("running put/get ttl tests"),

    {ok, _Pid} = start_link(ttl_cache),
    ?assertEqual(undefined, get(ttl_cache, my_key)),
    ?assertEqual(ok, put_ttl(ttl_cache, my_key, "my_val", 1)),
    ?assertEqual({ok, "my_val"}, get(ttl_cache, my_key)),
    timer:sleep(1100),
    ?assertEqual(undefined, get(ttl_cache, my_key)),

    ?assertEqual(ok, put_ttl(ttl_cache, my_key, "my_val2", 1)),
    ?assertEqual({ok, "my_val2"}, get(ttl_cache, my_key)),
    ?assertEqual(ok, put(ttl_cache, my_key, "my_val3")),
    timer:sleep(1100),
    ?assertEqual({ok, "my_val3"}, get(ttl_cache, my_key)),
    ?assertEqual(1, cache_size(ttl_cache)),
    ok = stop(ttl_cache).

default_put_get_test() ->
    ?debugMsg("running put/get w/default tests"),

    {ok, _Pid} = start_link(default_cache),
    ?assertEqual(undefined, get(default_cache, my_key)),
    ?assertEqual('DEF', get(default_cache, my_key, 'DEF')),
    ?assertEqual(0, cache_size(default_cache)),
    ?assertEqual(ok, put(default_cache, my_key, "my_val")),
    ?assertEqual("my_val", get(default_cache, my_key, 'DEF')),

    ?assertEqual(ok, put_ttl(default_cache, my_key, "my_val2", 1)),
    ?assertEqual("my_val2", get(default_cache, my_key, 'DEF')),
    timer:sleep(1100),
    ?assertEqual('DEF', get(default_cache, my_key, 'DEF')),
    ok = stop(default_cache).

lru_cache_test() ->
    ?debugMsg("running lru cache tests"),

    {ok, _Pid} = start_link(lru_cache, 2),
    ?assertEqual(ok, put(lru_cache, my_key1, "my_val1")),
    ?assertEqual(1, cache_size(lru_cache)),
    ?assertEqual(ok, put(lru_cache, my_key2, "my_val2")),
    ?assertEqual(2, cache_size(lru_cache)),
    ?assertEqual({ok, "my_val1"}, get(lru_cache, my_key1)),
    ?assertEqual(ok, put(lru_cache, my_key3, "my_val3")),
    timer:sleep(1000),
    ?assertEqual(2, cache_size(lru_cache)),
    ?assertEqual(undefined, get(lru_cache, my_key1)),
    ?assertEqual({ok, "my_val2"}, get(lru_cache, my_key2)),
    ?assertEqual({ok, "my_val3"}, get(lru_cache, my_key3)),
    ok = stop(lru_cache).

reset_test() ->
    ?debugMsg("running reset() tests"),

    {ok, _Pid} = start_link(reset_cache),
    ?assertEqual(ok, put(reset_cache, my_key, "my_val")),
    ?assertEqual({ok, "my_val"}, get(reset_cache, my_key)),
    ?assertEqual(1, cache_size(reset_cache)),

    ?assertEqual(ok, reset(reset_cache)),
    ?assertEqual(undefined, get(reset_cache, my_key)),
    ?assertEqual(0, cache_size(reset_cache)),
    ok = stop(reset_cache).

basic_speed_test() ->
    ?debugMsg("running basic speed test"),
    start_link(basic_cache),
    SimplePutGet =
        fun() ->
                lists:foreach(fun(I)-> put(basic_cache, I, I), get(basic_cache, I) end,
                              lists:seq(1, ?SPEED_TEST_OP_COUNT))
        end,
    ?debugTime(integer_to_list(?SPEED_TEST_OP_COUNT) ++ " simple put-get", SimplePutGet()),
    ok = stop(basic_cache).

ttl_put_get_speed_test() ->
    ?debugMsg("running TTL put-get speed test"),
    etsachet:start_link(ttl_cache),
    TtlPutGet =
        fun() ->
                lists:foreach(fun(I)-> etsachet:put_ttl(ttl_cache, I, I, I rem 10), etsachet:get(ttl_cache, I) end,
                              lists:seq(1, ?SPEED_TEST_OP_COUNT))
        end,
    ?debugTime(integer_to_list(?SPEED_TEST_OP_COUNT) ++ " TTL put-get", TtlPutGet()),
    ok = stop(ttl_cache).

lru_put_get_speed_test() ->
    ?debugMsg("running LRU put-get speed test"),
    etsachet:start_link(lru_cache, 1000),
    LruPutGet =
        fun() ->
                lists:foreach(fun(I)-> etsachet:put(lru_cache, I, I), etsachet:get(lru_cache, I) end,
                              lists:seq(1, ?SPEED_TEST_OP_COUNT))
        end,
    ?debugTime(integer_to_list(?SPEED_TEST_OP_COUNT) ++ " LRU put-get", LruPutGet()),
    ok = stop(lru_cache).

lru_ttl_put_get_speed_test() ->
    ?debugMsg("running LRU TTL put-get speed test"),
    etsachet:start_link(lru_ttl_cache, 1000),
    Cnt = lists:seq(1, ?SPEED_TEST_OP_COUNT),
    LruTtlPutGet =
        fun() ->
                lists:foreach(fun(I)-> etsachet:put_ttl(lru_ttl_cache, I, I, I rem 10), etsachet:get(lru_ttl_cache, I) end,
                              Cnt)
        end,
    ?debugTime(integer_to_list(?SPEED_TEST_OP_COUNT) ++ " LRU TTL put-get", LruTtlPutGet()),
    ok = stop(lru_ttl_cache),
    ok.

-endif.
