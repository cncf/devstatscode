select id, login from gha_actors where lower(login) {{exclude_bots}} and id in ({{ids}}) order by id;
