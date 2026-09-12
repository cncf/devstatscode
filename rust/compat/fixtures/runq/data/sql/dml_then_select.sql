insert into gha_postprocess_scripts(ord, path) values (100, 'first.sql');
select ord, path from gha_postprocess_scripts order by ord, path;
insert into gha_postprocess_scripts(ord, path) values (101, 'second.sql');
select 'not shown' as x;
