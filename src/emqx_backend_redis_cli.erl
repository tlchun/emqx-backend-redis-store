%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. 12月 2020 17:14
%%%-------------------------------------------------------------------
-module(emqx_backend_redis_cli).

-include_lib("../include/emqx.hrl").


-export([client_connected/2,
  subscribe_lookup/2,
  client_disconnected/2,
  message_fetch_for_queue/2,
  message_fetch_for_pubsub/2,
  message_fetch_for_keep_latest/2,
  lookup_retain/2,
  message_publish/3,
  message_store_keep_latest/3,
  message_retain/3,
  delete_retain/2,
  message_acked_for_queue/2,
  message_acked_for_pubsub/2,
  message_acked_for_keep_latest/2,
  run_redis_commands/3]).

-export([connect/1]).

-export([q/2, qp/2]).

-vsn("4.2.1").

%% 客户端连接
client_connected(Pool, Msg) ->
%%  获取Msg的客户端id
  ClientId = proplists:get_value(clientid, Msg),
%%  组装消息，｛状态-1，在线时间，离线时间｝
  Value = [<<"state">>, 1, <<"online_at">>, erlang:system_time(millisecond), <<"offline_at">>, undefined],
%%  封装命令
  Cmd1 = [<<"HMSET">>, table_name("mqtt:client", ClientId) | Value],
%%  封装命令
  Cmd2 = [<<"HSET">>, table_name("mqtt:node", a2b(node())), ClientId, erlang:system_time(millisecond)],
%%  执行命令
  with_qp(Pool, [Cmd1, Cmd2]).

%% 订阅查询
subscribe_lookup(Pool, Msg) ->
  ClientId = proplists:get_value(clientid, Msg),
  CmdLine = [<<"HGETALL">>, table_name("mqtt:sub", ClientId)],
  case q(Pool, CmdLine) of
    {ok, []} -> [];
    {ok, Hash} -> parse_sub(Hash);
    {error, Reason} ->
      logger:error("Redis Error: " ++ "~p~nCmd:~p", [Reason, CmdLine]),
      []
  end.
%% 客户端连接断开
client_disconnected(Pool, Msg) ->
%%  从消息中获取客户端id
  ClientId = proplists:get_value(clientid, Msg),
%%  修改状态为0，记录下线时间
  Value = [<<"state">>, 0, <<"offline_at">>, erlang:system_time(millisecond)],
%% 封装命令 将多个 field-value (字段-值)对设置到哈希表中
  Cmd1 = [<<"HMSET">>, table_name("mqtt:client", ClientId) | Value],
%%  删除指定结点的客户端
  Cmd2 = [<<"HDEL">>, table_name("mqtt:node", a2b(node())), ClientId],
  with_qp(Pool, [Cmd1, Cmd2]).

message_fetch_for_pubsub(Pool, Msg) ->
  ClientId = proplists:get_value(clientid, Msg),
  Topic = proplists:get_value(topic, Msg),
  AckTab = table_name(b2l(table_name("mqtt:acked", ClientId)), Topic),
  MsgTab = table_name("mqtt:msg", Topic),
  case q(Pool, [<<"GET">>, AckTab]) of
    {ok, undefined} ->
      MsgId1 = case q(Pool, ["ZRANGE", MsgTab, 0, -1]) of
                 {ok, []} -> 0;
                 {ok, MsgIds} ->
                   [Head | _MsgIds] = lists:reverse(MsgIds),
                   Head;
                 {error, Reason1} ->
                   logger:error("Redis Error: " ++ "error: ~p",
                     [Reason1]),
                   0
               end,
      q(Pool,
        [<<"SET">>,
          table_name(b2l(table_name("mqtt:acked", ClientId)),
            Topic),
          MsgId1]),
      [];
    {ok, MsgId} ->
      case q(Pool, [<<"ZRANK">>, MsgTab, MsgId]) of
        {ok, undefined} -> [];
        {ok, Index} ->
          offline_msg(Pool, Topic, MsgTab, i(Index) + 1, -1);
        {error, Reason2} ->
          logger:error("Redis Error: " ++ "error: ~p", [Reason2]),
          []
      end;
    {error, Reason3} ->
      logger:error("Redis Error: " ++ "error: ~p", [Reason3]),
      []
  end.

%% 从队列获取消息
message_fetch_for_queue(Pool, Msg) ->
%%  提取消息的主题
  Topic = proplists:get_value(topic, Msg),
%%
  MsgTab = table_name("mqtt:msg", Topic),
%%
  offline_msg(Pool, Topic, MsgTab).

message_fetch_for_keep_latest(Pool, Msg) ->
  Topic = proplists:get_value(topic, Msg),
  case q(Pool,
    [<<"HGETALL">>, table_name("mqtt:latest_msg", Topic)])
  of
    {ok, []} -> [];
    {ok, Hash} -> [hash2msg(Hash)];
    {error, Reason} ->
      logger:error("Redis Error: " ++ "error: ~p", [Reason]),
      []
  end.
%% 获取遗留消息
lookup_retain(Pool, Msg0) ->
%%  消息主题
  Topic = proplists:get_value(topic, Msg0),
%%  命令封装
  CmdLine = [<<"GET">>, table_name("mqtt:retain", Topic)],
%%  执行查询
  case q(Pool, CmdLine) of
%%    未定义
    {ok, undefined} -> [];
%%    返回消息id
    {ok, MsgId} -> lookup_msg(Topic, Pool, [MsgId], []);
    {error, Reason1} ->
      logger:error("Redis Error: " ++ "~p~nCmd:~p", [Reason1, CmdLine]),
      []
  end.

%% 消息发布
message_publish(Pool, Msg = #message{id = MsgId, topic = Topic, qos = Qos, flags = #{retain := Retain}}, Expired) ->
  Cmd1 = [<<"HMSET">>, table_name("mqtt:msg", to_b62(MsgId)), message],
  Cmd2 = [<<"ZADD">>, table_name("mqtt:msg", Topic), 1, msgid],
  Cmd3 = [<<"EXPIRE">>, table_name("mqtt:msg", to_b62(MsgId)), Expired],
  Cmds = case Qos > 0 of
           true -> [Cmd1, Cmd2, Cmd3];
           false ->
             case Retain of
               true -> [Cmd1, Cmd3];
               false -> []
             end
         end,
  PipeLine = lists:map(fun ([Cmd, Key | Args]) -> [Cmd, Key | feed_args(Args, Msg)] end, Cmds),
  with_qp(Pool, PipeLine),
  Msg.
%% 保存最新的消息存储
message_store_keep_latest(Pool, Msg = #message{topic = Topic}, Expired) ->
  Cmd1 = [<<"HMSET">>, table_name("mqtt:latest_msg", Topic), message],
  Cmd2 = [<<"EXPIRE">>, table_name("mqtt:latest_msg", Topic),  Expired],
  PipeLine = lists:map(fun ([Cmd, Key | Args]) ->
    [Cmd, Key | feed_args(Args, Msg)] end,
    [Cmd1, Cmd2]),
  with_qp(Pool, PipeLine),
  Msg.

%% 遗留消息
message_retain(Pool, Msg = #message{topic = Topic, id = MsgId}, Expired) ->
  Cmd1 = [<<"SET">>, table_name("mqtt:retain", Topic), to_b62(MsgId)],
  Cmd2 = [<<"EXPIRE">>, table_name("mqtt:retain", Topic), Expired],
  PipeLine = [Cmd1, Cmd2],
  case with_qp(Pool, PipeLine) of
    ok -> Msg;
    _Errs -> Msg
  end.
%% 删除遗留消息
delete_retain(Pool, Msg = #message{topic = Topic}) ->
%%  获取对应主题的消息命令
  Cmd1 = [<<"GET">>, table_name("mqtt:retain", Topic)],
%%  删除对应主题消息命令
  Cmd2 = [<<"DEL">>, table_name("mqtt:retain", Topic)],
%%  执行命令Cmd1
  CmdLine = case q(Pool, Cmd1) of
%%             如果没有,返回命令Cmd2
              {ok, undefined} -> [Cmd2];
%%              如果有消息id 返回
              {ok, MsgId} ->
%%                封装命令3，删除对应的消息id的消息
                Cmd3 = [<<"DEL">>, table_name("mqtt:msg", MsgId)],
%%                返回命令Cmd2和Cmd3
                [Cmd2, Cmd3];
%%            返回错误
              {error, Error} ->
                logger:error("Redis Error: " ++ "~p~nCmdLine:~p", [Error, Cmd1]),
%%                返回命令2
                [Cmd2]
            end,
%%  执行上面封装的命令行
  case with_qp(Pool, CmdLine) of
%%    成功返回
    ok -> Msg;
%%    错误返回
    {error, Reason} ->
      logger:error("Redis Error: " ++ "~p~nCmdLine:~p", [Reason, CmdLine]),
      Msg
  end.

%% 消息回复从队列
message_acked_for_queue(Pool, Msg) ->
%%  获取Msg的主题
  Topic = proplists:get_value(topic, Msg),
%%  获取Msg的消息id
  MsgId = proplists:get_value(msgid, Msg),
%%  消息id转base62
  MsgId1 = to_b62(MsgId),
%%  命令行组合｛通过消息MsgId1删除消息，移除有序集中的MsgId1｝
  PipeLine = [[<<"DEL">>, table_name("mqtt:msg", MsgId1)], [<<"ZREM">>, table_name("mqtt:msg", Topic), MsgId1]],
  with_qp(Pool, PipeLine).

%% 发布订阅
message_acked_for_pubsub(Pool, Msg) ->
  ClientId = proplists:get_value(clientid, Msg),
  Topic = proplists:get_value(topic, Msg),
  MsgId = proplists:get_value(msgid, Msg),
  Cmd = [<<"SET">>, table_name(b2l(table_name("mqtt:acked", ClientId)), Topic), to_b62(MsgId)],
  case q(Pool, Cmd) of
    {ok, _} -> ok;
    {error, Reason} ->
      logger:error("Redis Error: " ++ "~p~nCmd:~p", [Reason, Cmd])
  end.

message_acked_for_keep_latest(Pool, Msg) ->
  Topic = proplists:get_value(topic, Msg),
  Cmd = [<<"DEL">>, table_name("mqtt:latest_msg", Topic)],
  case q(Pool, Cmd) of
    {ok, _} -> ok;
    {error, Reason} ->
      logger:error("Redis Error: " ++ "~p~nCmd:~p", [Reason, Cmd])
  end.

%% 运行redis命令
run_redis_commands(Pool, Msg, {pipeline, CmdLines}) ->
  PipeLine = lists:map(fun ([Cmd, Key | Params]) ->
    [Cmd, compile_key(Msg, Key) | compile_cmd(Params, Msg)] end, CmdLines),
  with_qp(Pool, PipeLine),
  Msg;
run_redis_commands(Pool, Msg, CmdLines) ->
  lists:foreach(fun ([Cmd, Key | Params]) ->
    CmdLine = [Cmd, compile_key(Msg, Key) | compile_cmd(Params, Msg)],
    case q(Pool, CmdLine) of
      {ok, _} -> ok;
      {error, Error} ->
        logger:error("Redis Error: " ++
        "~p~nCmdLine:~p",
          [Error, CmdLine])
    end
                end,
    CmdLines),
  Msg.

compile_key(Msg, Key) ->
  Args = case re:run(Key, <<"\\$\\{[^}]+\\}">>, [global, {capture, all, binary}]) of
           nomatch -> [];
           {match, Vars} -> lists:flatten(Vars)
         end,
  compile_key(Args, Msg, Key).

compile_key([], _Msg, Acc) -> Acc;
compile_key([<<"${topic}">> | Args], Msg, Acc) when is_list(Msg) ->
  Topic = proplists:get_value(topic, Msg),
  compile_key(Args, Msg, binary:replace(Acc, <<"${topic}">>, Topic));
compile_key([<<"${msgid}">> | Args], Msg, Acc) when is_list(Msg) ->
  MsgId = proplists:get_value(msgid, Msg),
  compile_key(Args, Msg, binary:replace(Acc, <<"${msgid}">>, to_b62(MsgId)));
compile_key([<<"${clientid}">> | Args], Msg, Acc) when is_list(Msg) ->
  ClientId = proplists:get_value(clientid, Msg),
  compile_key(Args, Msg, binary:replace(Acc, <<"${clientid}">>, ClientId));
compile_key([<<"${topic}">> | Args], Msg = #message{topic = Topic}, Acc) ->
  compile_key(Args, Msg, binary:replace(Acc, <<"${topic}">>, Topic));
compile_key([<<"${msgid}">> | Args], Msg = #message{id = MsgId}, Acc) ->
  compile_key(Args, Msg, binary:replace(Acc, <<"${msgid}">>, to_b62(MsgId)));
compile_key([<<"${clientid}">> | Args], Msg = #message{from = From}, Acc) ->
  ClientId = format_from(From),
  compile_key(Args, Msg, binary:replace(Acc, <<"${clientid">>, ClientId));
compile_key([_Key | Args], Msg, Acc) ->
  compile_key(Args, Msg, Acc).

compile_cmd(Cmd, Msg) -> compile_cmd(Cmd, Msg, []).

compile_cmd([], _Msg, Acc) -> lists:reverse(Acc);
compile_cmd([<<"${clientid}">> | Params], Msg, Acc) when is_list(Msg) ->
  ClientId = proplists:get_value(clientid, Msg),
  compile_cmd(Params, Msg, [ClientId | Acc]);
compile_cmd([<<"${clientid}">> | Params], Msg = #message{from = From}, Acc) ->
  ClientId = format_from(From),
  compile_cmd(Params, Msg, [ClientId | Acc]);
compile_cmd([<<"${topic}">> | Params], Msg, Acc) when is_list(Msg) ->
  Topic = proplists:get_value(topic, Msg),
  compile_cmd(Params, Msg, [Topic | Acc]);
compile_cmd([<<"${topic}">> | Params], Msg = #message{topic = Topic}, Acc) ->
  compile_cmd(Params, Msg, [Topic | Acc]);
compile_cmd([<<"${qos}">> | Params], Msg, Acc) when is_list(Msg) ->
  Qos = proplists:get_value(qos, Msg),
  compile_cmd(Params, Msg, [Qos | Acc]);
compile_cmd([<<"${qos}">> | Params], Msg = #message{qos = Qos}, Acc) ->
  compile_cmd(Params, Msg, [Qos | Acc]);
compile_cmd([<<"${msgid}">> | Params], Msg, Acc)
  when is_list(Msg) ->
  MsgId = proplists:get_value(msgid, Msg),
  compile_cmd(Params, Msg, [to_b62(MsgId) | Acc]);
compile_cmd([<<"${msgid}">> | Params],
    Msg = #message{id = MsgId}, Acc) ->
  compile_cmd(Params, Msg, [to_b62(MsgId) | Acc]);
compile_cmd([<<"${payload}">> | Params], Msg, Acc)
  when is_list(Msg) ->
  Payload = proplists:get_value(payload, Msg),
  compile_cmd(Params, Msg, [Payload | Acc]);
compile_cmd([<<"${payload}">> | Params],
    Msg = #message{payload = Payload}, Acc) ->
  compile_cmd(Params, Msg, [Payload | Acc]);
compile_cmd([<<"${message}">> | Params],
    Msg = #message{id = MsgId, from = From, qos = Qos,
      topic = Topic, payload = Payload, timestamp = Ts,
      flags = #{retain := Retain}},
    Acc) ->
  ClientId = format_from(From),
  compile_cmd(Params,
    Msg,
    [Retain,
      <<"retain">>,
      Ts,
      <<"ts">>,
      Payload,
      <<"payload">>,
      Topic,
      <<"topic">>,
      Qos,
      <<"qos">>,
      ClientId,
      <<"from">>,
      to_b62(MsgId),
      <<"id">>
      | Acc]);
compile_cmd([Key | Params], Msg, Acc) ->
  compile_cmd(Params, Msg, [Key | Acc]).

feed_args(Args, Message) ->
  feed_args(Args, Message, []).

feed_args([], _Message, Acc) ->
  lists:flatten(lists:reverse(Acc));
feed_args([Param | Args], Message, Acc) when is_binary(Param) ->
  feed_args([binary_to_atom(Param, utf8) | Args], Message, Acc);
feed_args([message | Args], Message, Acc) ->
  feed_args(Args, Message, [msg2hash(Message) | Acc]);
feed_args([msgid | Args], Message = #message{id = MsgId}, Acc) ->
  feed_args(Args, Message, [to_b62(MsgId) | Acc]);
feed_args([msgid | Args], Message, Acc) when is_list(Message) ->
  feed_args(Args, Message, [to_b62(proplists:get_value(msgid, Message, <<"">>)) | Acc]);
feed_args([topic | Args],
    Message = #message{topic = Topic}, Acc) ->
  feed_args(Args, Message, [Topic | Acc]);
feed_args([topic | Args], Message, Acc)
  when is_list(Message) ->
  feed_args(Args,
    Message,
    [proplists:get_value(topic, Message, null) | Acc]);
feed_args([payload | Args],
    Message = #message{payload = Payload}, Acc) ->
  feed_args(Args, Message, [Payload | Acc]);
feed_args([qos | Args], Message = #message{qos = Qos},
    Acc) ->
  feed_args(Args, Message, [Qos | Acc]);
feed_args([qos | Args], Message, Acc)
  when is_list(Message) ->
  feed_args(Args,
    Message,
    [proplists:get_value(qos, Message, null) | Acc]);
feed_args([clientid | Args],
    Message = #message{from = From}, Acc) ->
  ClientId = format_from(From),
  feed_args(Args, Message, [ClientId | Acc]);
feed_args([clientid | Args], Message, Acc)
  when is_list(Message) ->
  feed_args(Args,
    Message,
    [proplists:get_value(clientid, Message, null) | Acc]);
feed_args([Arg | Args], Message, Acc) ->
  feed_args(Args, Message, [Arg | Acc]).

msg2hash(#message{id = MsgId, from = From, qos = Qos,
  topic = Topic, payload = Payload, timestamp = Ts,
  flags = #{retain := Retain}}) ->
  ClientId = format_from(From),
  [<<"id">>,
    to_b62(MsgId),
    <<"from">>,
    ClientId,
    <<"qos">>,
    Qos,
    <<"topic">>,
    Topic,
    <<"payload">>,
    Payload,
    <<"ts">>,
    Ts,
    <<"retain">>,
    Retain].

hash2msg(Hash) ->
  Msg = parse(Hash),
  #message{id =
  from_b62(proplists:get_value(<<"id">>, Msg)),
    from = proplists:get_value(<<"from">>, Msg),
    qos = i(proplists:get_value(<<"qos">>, Msg)),
    topic = proplists:get_value(<<"topic">>, Msg),
    payload = proplists:get_value(<<"payload">>, Msg),
    timestamp = i(proplists:get_value(<<"ts">>, Msg)),
    flags = #{retain => b2a(proplists:get_value(<<"retain">>, Msg))}, headers = #{}}.

parse(Hash) -> parse(Hash, []).

parse([], Acc) -> Acc;
parse([Key, Val | Tail], Acc) ->
  parse(Tail, [{Key, Val} | Acc]).

i(B) -> list_to_integer(binary_to_list(B)).

b2l(B) -> binary_to_list(B).

l2b(L) -> list_to_binary(L).

a2b(A) -> erlang:atom_to_binary(A, utf8).

b2a(B) -> erlang:binary_to_atom(B, utf8).

format_from(From) when is_atom(From) -> a2b(From);
format_from(From) -> From.

table_name(Name, Val) ->
  l2b(lists:concat([Name, ":", b2l(Val)])).

to_b62(MsgId) -> emqx_guid:to_base62(MsgId).

from_b62(MsgId) -> emqx_guid:from_base62(MsgId).

parse_sub(Hash) -> parse_sub(Hash, []).

parse_sub([], Acc) -> Acc;
parse_sub([Topic, Qos | Hash], Acc) ->
  parse_sub(Hash, [{Topic, #{qos => i(Qos)}} | Acc]).

offline_msg(Pool, Topic, MsgTab) ->
  offline_msg(Pool, Topic, MsgTab, 0, -1).

offline_msg(Pool, Topic, MsgTab, Start, End) ->
  case q(Pool, [<<"ZRANGE">>, MsgTab, Start, End]) of
    {ok, []} -> [];
    {ok, MsgIds} -> lookup_msg(Topic, Pool, MsgIds, []);
    {error, Reason} ->
      logger:error("Redis Error: " ++ "error: ~p", [Reason]),
      []
  end.

lookup_msg(_Topic, _Pool, [], Acc) -> Acc;
lookup_msg(Topic, Pool, [MsgId | MsgIds], Acc) ->
  Acc2 = case q(Pool,
    [<<"HGETALL">>, table_name("mqtt:msg", MsgId)])
         of
           {ok, []} ->
             q(Pool,
               [<<"ZREM">>, table_name("mqtt:msg", Topic), MsgId]),
             Acc;
           {ok, Hash} -> Acc ++ [hash2msg(Hash)];
           {error, Reason} ->
             logger:error("Redis Error: " ++ "error: ~p", [Reason]),
             Acc
         end,
  lookup_msg(Topic, Pool, MsgIds, Acc2).

with_qp(_Pool, []) -> ok;
with_qp(Pool, PipeLine) ->
  case qp(Pool, PipeLine) of
    {error, Error} ->
      logger:error("Redis Error: " ++ "~p, PipeLine:~p", [Error, PipeLine]);
    R ->
      case [Err || Err = {error, _} <- R] of
        [] -> ok;
        Errs ->
          logger:error("Redis Error: " ++ "~p, PipeLine:~p", [Errs, PipeLine])
      end
  end.

connect(Opts) ->
  Sentinel = proplists:get_value(sentinel, Opts),
  case Sentinel =:= "" of
    true -> start(Opts);
    false ->
      eredis_sentinel:start_link(proplists:get_value(servers, Opts)),
      start(Sentinel, Opts)
  end.

start(Opts) ->
  eredis:start_link(proplists:get_value(host, Opts),
    proplists:get_value(port, Opts),
    proplists:get_value(database, Opts),
    proplists:get_value(password, Opts),
    3000,
    5000,
    proplists:get_value(options, Opts, [])).

start(Sentinel, Opts) ->
  eredis:start_link("sentinel:" ++ Sentinel,
    proplists:get_value(port, Opts),
    proplists:get_value(database, Opts),
    proplists:get_value(password, Opts),
    3000,
    5000,
    proplists:get_value(options, Opts, [])).

q({cluster, Pool}, Cmd) ->
  logger:debug("redis cmd:~p", [Cmd]),
  eredis_cluster:q(Pool, Cmd);

q({_, Pool}, Cmd) ->
  logger:debug("redis Pool:~p, cmd:~p", [Pool, Cmd]),
  ecpool:with_client(Pool, fun (C) -> eredis:q(C, Cmd) end).

qp({cluster, Pool}, PipeLine) ->
  logger:debug("redis cmd:~p", [PipeLine]),
  [eredis_cluster:q(Pool, Cmd) || Cmd <- PipeLine];
qp({_, Pool}, PipeLine) ->
  logger:debug("redis Pool:~p, PipeLine:~p", [Pool, PipeLine]),
  ecpool:with_client(Pool, fun (C) -> eredis:qp(C, PipeLine) end).
