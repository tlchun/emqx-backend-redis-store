%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. 12月 2020 17:14
%%%-------------------------------------------------------------------
-module(emqx_backend_redis_sub).
-behaviour(gen_server).

-export([start_link/1]).

-export([init/1,handle_call/3,handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {sub}).

start_link(Opts) ->
  gen_server:start_link(emqx_backend_redis_sub, [Opts], []).

init([Opts]) ->
  Channel = proplists:get_value(channel, Opts),
  Sentinel = proplists:get_value(sentinel, Opts),
  Host = case Sentinel =:= "" of
           true -> proplists:get_value(host, Opts);
           false ->
             eredis_sentinel:start_link(proplists:get_value(servers, Opts)),
             "sentinel:" ++ Sentinel
         end,
  {ok, Sub} = eredis:start_link(Host, proplists:get_value(port, Opts), proplists:get_value(database, Opts), proplists:get_value(password, Opts)),

  eredis_sub:controlling_process(Sub, self()),
  eredis_sub:subscribe(Sub, [list_to_binary(Channel)]),
  {ok, #state{sub = Sub}}.

handle_call(_Req, _From, State) ->
  {reply, ignore, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({subscribed, Channel, Sub}, State) ->
  logger:debug("recv subscribed:~p", [Channel]),
  eredis_sub:ack_message(Sub),
  {noreply, State};

handle_info({message, Channel, Msg, Sub}, State) ->
  logger:debug("recv channel:~p, msg:~p", [Channel, Msg]),
  eredis_sub:ack_message(Sub),
  try Params = emqx_json:decode(Msg),
  Type = proplists:get_value(<<"type">>, Params),
  Topic1 = proplists:get_value(<<"topic">>, Params),
  ClientId = proplists:get_value(<<"clientid">>, Params),
%%    查询客户端的进程id
  ClientPid = emqx_cm:lookup_proc(ClientId),
%%  判读是订阅类型 或者 取消订阅
  case Type of
    <<"subscribe">> ->
      Qos = proplists:get_value(<<"qos">>, Params),
      emqx_client:subscribe(ClientPid, [{Topic1, to_int(Qos)}]);
    <<"unsubscribe">> ->
      emqx_client:unsubscribe(ClientPid, [Topic1])
  end
  catch
    _:Error:Stack ->
      logger:error("decode json error:~p,~p", [Error, Stack])
  end,
  {noreply, State}.

terminate(_Reason, #state{}) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

to_int(undefined) -> 0;
to_int(B) when is_binary(B) -> binary_to_integer(B);
to_int(S) when is_list(S) -> list_to_integer(S);
to_int(I) when is_integer(I) -> I.