%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. 12月 2020 16:42
%%%-------------------------------------------------------------------
{application, emqx_backend_redis, [
  {description, ""},
  {vsn, "4.2.1"},
  {modules,[emqx_backend_redis, emqx_backend_redis_app, emqx_backend_redis_cli, emqx_backend_redis_sub, emqx_backend_redis_sup]},
  {registered, [emqx_backend_redis_sup]},
  {applications, [kernel,stdlib,eredis,eredis_cluster,ecpool]},
  {mod, {emqx_backend_redis, []}},
  {env, []}
]}.

%% eredis,eredis_cluster,ecpool