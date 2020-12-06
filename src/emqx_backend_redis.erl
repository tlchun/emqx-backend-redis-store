%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. 12月 2020 16:51
%%%-------------------------------------------------------------------
-module(emqx_backend_redis).
-include("../include/emqx_backend_redis.hrl").

-include_lib("../include/emqx.hrl").


%% API
-export([pool_name/1,compile_cmd/1]).

-export([
  register_metrics/0,
  load/0,
  unload/0]
).

-export([on_client_connected/3,
  on_subscribe_lookup/3,
  on_client_disconnected/4,
  on_message_fetch_for_queue/4,
  on_message_fetch_for_pubsub/4,
  on_message_fetch_for_keep_latest/4,
  on_retain_lookup/4,
  on_message_publish/2,
  on_message_store_keep_latest/2,
  on_message_retain/2,
  on_retain_delete/2,
  on_message_acked_for_queue/3,
  on_message_acked_for_pubsub/3,
  on_message_acked_for_keep_latest/3,
  run_redis_commands/2,
  run_redis_commands/3,
  run_redis_commands/4]).

pool_name(Pool)->
  list_to_atom(lists:concat([emqx_backend_redis,'_',Pool])).

register_metrics() ->
  [emqx_metrics:new(MetricName) || MetricName
    <- ['backend.redis.client_connected',
      'backend.redis.subscribe_lookup',
      'backend.redis.client_disconnected',
      'backend.redis.message_fetch_for_queue',
      'backend.redis.message_fetch_for_pubsub',
      'backend.redis.message_fetch_for_keep_latest',
      'backend.redis.retain_lookup',
      'backend.redis.message_publish',
      'backend.redis.message_store_keep_latest',
      'backend.redis.message_retain',
      'backend.redis.retain_delete',
      'backend.redis.message_acked_for_queue',
      'backend.redis.message_acked_for_pubsub',
      'backend.redis.message_acked_for_keep_latest']].

load()->
  HookList = parse_hook(application:get_env(emqx_backend_redis, hooks, [])),
  lists:foreach(
  fun ({Hook,Action,Pool,Filter,ExpiredTime}) ->
    case proplists:get_value(<<"function">>, Action) of
      undefined ->
        Commands = proplists:get_value(<<"commands">>, Action, []),
        Cmds = compile_cmds(Commands),
        load_(Hook, run_redis_commands, ExpiredTime, {Filter, Pool, Cmds});
      Fun ->
        load_(Hook, b2a(Fun), ExpiredTime, {Filter, Pool, undefined})
    end
  end, HookList),
  io:format("~s is loaded.~n", [emqx_backend_redis]),
  ok.

load_(Hook, Fun, ExpiredTime, {Filter, Pool, undefined}) ->
  load_(Hook, Fun, ExpiredTime, {Filter, Pool});
load_(Hook, Fun, ExpiredTime, Params) ->
  case Hook of
    'client.connected' ->
      emqx:hook(Hook, fun emqx_backend_redis:Fun/3, [Params]);
    'client.disconnected' ->
      emqx:hook(Hook, fun emqx_backend_redis:Fun/4, [Params]);
    'session.subscribed' ->
      emqx:hook(Hook, fun emqx_backend_redis:Fun/4, [Params]);
    'session.unsubscribed' ->
      emqx:hook(Hook, fun emqx_backend_redis:Fun/4, [Params]);
    'message.publish' ->
      Expired = case ExpiredTime =< 0 of
                  true -> 7200;
                  false -> ExpiredTime
                end,
      emqx:hook(Hook, fun emqx_backend_redis:Fun/2, [erlang:append_element(Params, Expired)]);
    'message.acked' ->
      emqx:hook(Hook, fun emqx_backend_redis:Fun/3, [Params]);
    'message.delivered' ->
      emqx:hook(Hook, fun emqx_backend_redis:Fun/3, [Params])
  end.

unload() ->
  HookList = parse_hook(application:get_env(emqx_backend_redis, hooks, [])),
  lists:foreach(
    fun ({Hook, Action, _Pool, _Filter, _ExpiredTime}) ->
      case proplists:get_value(<<"function">>, Action) of
        undefined -> unload_(Hook, run_redis_commands);
        Fun -> unload_(Hook, b2a(Fun))
      end
    end,
    HookList),
  io:format("~s is unloaded.~n", [emqx_backend_redis]),
  ok.

unload_(Hook, Fun) ->
  case Hook of
    'client.connected' ->
      emqx:unhook(Hook, fun emqx_backend_redis:Fun/3);
    'client.disconnected' ->
      emqx:unhook(Hook, fun emqx_backend_redis:Fun/4);
    'session.subscribed' ->
      emqx:unhook(Hook, fun emqx_backend_redis:Fun/4);
    'session.unsubscribed' ->
      emqx:unhook(Hook, fun emqx_backend_redis:Fun/4);
    'message.publish' ->
      emqx:unhook(Hook, fun emqx_backend_redis:Fun/2);
    'message.acked' ->
      emqx:unhook(Hook, fun emqx_backend_redis:Fun/3);
    'message.delivered' ->
      emqx:unhook(Hook, fun emqx_backend_redis:Fun/3)
  end.

on_client_connected(#{clientid := ClientId}, _ConnInfo, {Filter, Pool}) ->
  with_filter(
    fun () ->
      emqx_metrics:inc('backend.redis.client_connected'),
      emqx_backend_redis_cli:client_connected(Pool, [{clientid, ClientId}]) end,
      undefined,
    Filter);
on_client_connected(_Client, _ConnInfo, _Rule) -> ok.

on_subscribe_lookup(#{clientid := ClientId}, _ConnInfo,{Filter, Pool}) ->
  with_filter(
    fun () ->
      emqx_metrics:inc('backend.redis.subscribe_lookup'),
      case emqx_backend_redis_cli:subscribe_lookup(Pool, [{clientid,ClientId}]) of
        [] -> ok;
        TopicTable ->
          self() ! {subscribe, TopicTable},
          ok
      end
    end, undefined, Filter);
on_subscribe_lookup(_ClientInfo, _ConnInfo, _Envs) ->
  ok.

on_client_disconnected(#{clientid := ClientId}, _Reason,
    _ConnInfo, {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.redis.client_disconnected'),
    emqx_backend_redis_cli:client_disconnected(Pool,
      [{clientid,
        ClientId}])
              end,
    undefined,
    Filter).

on_message_fetch_for_queue(#{clientid := ClientId}, Topic, Opts, {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.redis.message_fetch_for_queue'),
    case maps:get(qos, Opts, 0) > 0 andalso
      maps:get(is_new, Opts, true)
    of
      true ->
        MsgList =
          emqx_backend_redis_cli:message_fetch_for_queue(Pool,
            [{clientid,
              ClientId},
              {topic,
                Topic}]),
        [self() ! {deliver, Topic, Msg}
          || Msg <- MsgList];
      false -> ok
    end
              end,
    Topic,
    Filter).

on_message_fetch_for_pubsub(#{clientid := ClientId},Topic, Opts, {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.redis.message_fetch_for_pubsub'),
    case maps:get(qos, Opts, 0) > 0 andalso
      maps:get(is_new, Opts, true)
    of
      true ->
        MsgList =
          emqx_backend_redis_cli:message_fetch_for_pubsub(Pool,
            [{clientid,
              ClientId},
              {topic,
                Topic}]),
        [self() ! {deliver, Topic, Msg}
          || Msg <- MsgList];
      false -> ok
    end
              end,
    Topic,
    Filter).

on_message_fetch_for_keep_latest(#{clientid := ClientId},Topic, Opts, {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.redis.message_fetch_for_keep_latest'),
    case maps:get(qos, Opts, 0) > 0 andalso
      maps:get(is_new, Opts, true)
    of
      true ->
        MsgList =
          emqx_backend_redis_cli:message_fetch_for_keep_latest(Pool,
            [{clientid,
              ClientId},
              {topic,
                Topic}]),
        [self() ! {deliver, Topic, Msg}
          || Msg <- MsgList];
      false -> ok
    end
              end,
    Topic,
    Filter).

on_retain_lookup(_Client, Topic, _Opts,
    {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.redis.retain_lookup'),
    MsgList = emqx_backend_redis_cli:lookup_retain(Pool,
      [{topic,
        Topic}]),
    [self() !
      {deliver,
        Topic,
        emqx_message:set_header(retained, true, Msg)}
      || Msg <- MsgList]
              end,
    Topic,
    Filter).

on_message_publish(Msg = #message{flags =
#{retain := true}, payload = <<>>}, _Rule) -> {ok, Msg};
on_message_publish(Msg0 = #message{topic = Topic}, {Filter, Pool, ExpiredTime}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.redis.message_publish'),
    Msg = emqx_backend_redis_cli:message_publish(Pool, Msg0, ExpiredTime),
    {ok, Msg} end,
    Msg0,
    Topic,
    Filter).

on_message_store_keep_latest(Msg = #message{flags =
#{retain := true},payload = <<>>},_Rule) ->{ok, Msg};

on_message_store_keep_latest(Msg0 = #message{topic = Topic}, {Filter, Pool, ExpiredTime}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.redis.message_store_keep_latest'),
    Msg =
      emqx_backend_redis_cli:message_store_keep_latest(Pool, Msg0, ExpiredTime),
    {ok, Msg}
              end,
    Msg0,
    Topic,
    Filter).

on_message_retain(Msg = #message{flags = #{retain := false}}, _Rule) -> {ok, Msg};
on_message_retain(Msg = #message{flags =
#{retain := true}, payload = <<>>}, _Rule) -> {ok, Msg};
on_message_retain(Msg0 = #message{flags = #{retain := true}, topic = Topic, headers = Headers0}, {Filter, Pool, ExpiredTime}) ->
  Headers = case erlang:is_map(Headers0) of
              true -> Headers0;
              false -> #{}
            end,
  case maps:find(retained, Headers) of
    {ok, true} -> {ok, Msg0};
    _ ->
      with_filter(fun () ->
        emqx_metrics:inc('backend.redis.message_retain'),
        Msg = emqx_backend_redis_cli:message_retain(Pool, Msg0, ExpiredTime), {ok, Msg} end,
        Msg0,
        Topic,
        Filter)
  end;
on_message_retain(Msg, _Rule) -> {ok, Msg}.

on_retain_delete(Msg0 = #message{flags =
#{retain := true}, topic = Topic, payload = <<>>}, {Filter, Pool, _ExpiredTime}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.redis.retain_delete'),
    Msg = emqx_backend_redis_cli:delete_retain(Pool, Msg0),
    {stop, Msg}
              end,
    Msg0,
    Topic,
    Filter);
on_retain_delete(Msg, _Rule) -> {ok, Msg}.

on_message_acked_for_queue(#{clientid := ClientId}, #message{topic = Topic, id = MsgId}, {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.redis.message_acked_for_queue'),
    emqx_backend_redis_cli:message_acked_for_queue(Pool,
      [{clientid,
        ClientId},
        {topic,
          Topic},
        {msgid,
          MsgId}])
              end,
    Topic,
    Filter).

on_message_acked_for_pubsub(#{clientid := ClientId},
    #message{topic = Topic, id = MsgId},
    {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.redis.message_acked_for_pubsub'),
    emqx_backend_redis_cli:message_acked_for_pubsub(Pool,
      [{clientid,
        ClientId},
        {topic,
          Topic},
        {msgid,
          MsgId}])
              end,
    Topic,
    Filter).

on_message_acked_for_keep_latest(#{clientid := ClientId},#message{topic = Topic}, {Filter, Pool}) ->
  with_filter(fun () ->
    emqx_metrics:inc('backend.redis.message_acked_for_keep_latest'),
    emqx_backend_redis_cli:message_acked_for_keep_latest(Pool,
      [{clientid,
        ClientId},
        {topic,
          Topic}])
              end,
    Topic,
    Filter).

run_redis_commands(Msg0 = #message{topic = Topic},
    {Filter, Pool, Cmdlines, _ExpiredTime}) ->
  with_filter(fun () ->
    Msg = emqx_backend_redis_cli:run_redis_commands(Pool,
      Msg0,
      Cmdlines),
    {ok, Msg}
              end,
    Msg0,
    Topic,
    Filter).

run_redis_commands(#{clientid := ClientId}, #message{topic = Topic, id = MsgId}, {Filter, Pool, Cmdlines}) ->
  with_filter(fun () -> emqx_backend_redis_cli:run_redis_commands(Pool, [{clientid, ClientId}, {topic, Topic}, {msgid, MsgId}], Cmdlines) end, Topic, Filter);
run_redis_commands(#{clientid := ClientId}, _ConnInfo, {Filter, Pool, Cmdlines}) ->
  with_filter(fun () -> emqx_backend_redis_cli:run_redis_commands(Pool, [{clientid, ClientId}], Cmdlines) end, undefined, Filter);
run_redis_commands(_, _, _) -> ok.

run_redis_commands(#{clientid := ClientId}, Topic, Opts, {Filter, Pool, Cmdlines})
  when is_binary(Topic) ->
  with_filter(fun () ->
    Qos = maps:get(qos, Opts, 0),
    emqx_backend_redis_cli:run_redis_commands(Pool,
      [{clientid,
        ClientId},
        {topic,
          Topic},
        {qos, Qos}],
      Cmdlines)
              end,
    Topic,
    Filter);
run_redis_commands(#{clientid := ClientId}, _Reason,
    _ConnInfo, {Filter, Pool, Cmdlines}) ->
  with_filter(fun () ->
    emqx_backend_redis_cli:run_redis_commands(Pool,
      [{clientid,
        ClientId}],
      Cmdlines)
              end,
    undefined,
    Filter);
run_redis_commands(_, _, _, _) -> ok.

parse_hook(Hooks) -> parse_hook(Hooks, []).

parse_hook([], Acc) -> Acc;
parse_hook([{Hook, Item} | Hooks], Acc) ->
  Params = emqx_json:decode(Item),
  Action = proplists:get_value(<<"action">>, Params),
  Pool = proplists:get_value(<<"pool">>, Params),
  Filter = proplists:get_value(<<"topic">>, Params),
  ExpiredTime = proplists:get_value(<<"expired_time">>, Params, 7200),
  Pools = application:get_env(emqx_backend_redis, pools, []),
  Pool1 = case lists:keyfind(b2a(Pool), 1, Pools) of
            false -> pool_name(b2a(Pool));
            {_, PoolArgs} ->
              case proplists:get_value(type, PoolArgs) of
                cluster -> {cluster, pool_name(b2a(Pool))};
                Type -> {Type, pool_name(b2a(Pool))}
              end
          end,
  parse_hook(Hooks, [{l2a(Hook), Action, Pool1, Filter, ExpiredTime} | Acc]).

compile_cmds([<<"pipeline">> | Cmds]) ->
  {pipeline, [compile_cmd(Cmd) || Cmd <- Cmds]};
compile_cmds(Cmds) -> [compile_cmd(Cmd) || Cmd <- Cmds].

compile_cmd(Cmd) ->
  compile_cmd(binary:split(Cmd, <<" ">>, [global]), []).

compile_cmd([], Acc) -> lists:reverse(Acc);
compile_cmd([Cmd | Tail], Acc) when Cmd =:= <<>> ->
  compile_cmd(Tail, Acc);
compile_cmd([Cmd | Tail], Acc) ->
  compile_cmd(Tail, [Cmd | Acc]).

with_filter(Fun, _, undefined) ->
  Fun(),
  ok;
with_filter(Fun, Topic, Filter) ->
  case emqx_topic:match(Topic, Filter) of
    true ->
      Fun(),
      ok;
    false -> ok
  end.

with_filter(Fun, _, _, undefined) -> Fun();
with_filter(Fun, Msg, Topic, Filter) ->
  case emqx_topic:match(Topic, Filter) of
    true -> Fun();
    false -> {ok, Msg}
  end.

l2a(L) -> erlang:list_to_atom(L).

b2a(B) -> erlang:binary_to_atom(B, utf8).








