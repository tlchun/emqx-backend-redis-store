%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. 12月 2020 17:09
%%%-------------------------------------------------------------------
-module(emqx_backend_redis_sup).

-include("../include/emqx_backend_redis.hrl").
-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

%% 启动应用监听者
start_link(Pools) ->
  supervisor:start_link({local, emqx_backend_redis_sup}, emqx_backend_redis_sup, [Pools]).

%% 回调方法
init([Pools]) ->
%%  获取进程描述
  PoolSpec = lists:flatten([pool_spec(Pool, Env) || {Pool, Env} <- Pools]),
  {ok, {{one_for_one, 10, 100}, PoolSpec}}.

%% 进程池描述
pool_spec(Pool, Env) ->
  case proplists:get_value(type, Env) of
%%    集群模型
    cluster ->
      {ok, _} =
        eredis_cluster:start_pool(emqx_backend_redis:pool_name(Pool), Env), [];
%%    单机模式
    _ ->
      [sub_spec(Env), ecpool:pool_spec({emqx_backend_redis, Pool}, emqx_backend_redis:pool_name(Pool), emqx_backend_redis_cli, Env)]
  end.

sub_spec(Env) ->
  {emqx_backend_redis_sub, {emqx_backend_redis_sub, start_link, [Env]}, permanent, 5000, worker, [emqx_backend_redis_sub]}.
