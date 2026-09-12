create temp table tmp_{{rnd}} as select id, login from gha_actors where id < 103;
select id, login from tmp_{{rnd}} order by id;
drop table tmp_{{rnd}};
