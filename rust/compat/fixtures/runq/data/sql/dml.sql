insert into gha_postprocess_scripts(ord, path) values ({{ord}}, '{{path}}') on conflict do nothing;
update gha_actors set name = 'Updated by runq' where id = 106;
delete from gha_countries where code = 'fr';
