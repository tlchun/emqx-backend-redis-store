%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. 12月 2020 17:08
%%%-------------------------------------------------------------------
-module(emqx_backend_redis_app).

-include("../include/emqx_backend_redis.hrl").

-behaviour(application).

-emqx_plugin(backend).

-export([start/2, stop/1]).

start(_Type, _Args) ->
%%   获取配置信息
  Pools = application:get_env(emqx_backend_redis, pools, []),
%%  启动应用监听者
  {ok, Sup} = emqx_backend_redis_sup:start_link(Pools),

%%  注册redis统计
  emqx_backend_redis:register_metrics(),

%%  载入redis模块
  emqx_backend_redis:load(),

  {ok, Sup}.

%% 停止应用,卸载安装的模块
stop(_State) ->
  lists:foreach(
    fun ({Pool, Env}) ->
      case proplists:get_value(type, Env) of
        cluster ->
          eredis_cluster:stop_pool(emqx_backend_redis:pool_name(Pool));
        _ -> ok
      end
    end,
    application:get_env(emqx_backend_redis, pools, [])),
  emqx_backend_redis:unload(),
  ok.