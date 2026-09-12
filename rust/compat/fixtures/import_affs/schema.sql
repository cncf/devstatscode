create table gha_actors(id bigint not null, login varchar(120) not null, name varchar(120), country_id varchar(2), sex varchar(1), sex_prob double precision, tz varchar(40), tz_offset int, country_name text, age int, primary key(id, login));
create table gha_actors_emails(actor_id bigint not null, email varchar(120) not null, origin smallint not null default 0, primary key(actor_id, email));
create table gha_actors_names(actor_id bigint not null, name varchar(120) not null, origin smallint not null default 0, primary key(actor_id, name));
create table gha_companies(name varchar(160) not null, primary key(name));
create table gha_actors_affiliations(actor_id bigint not null, company_name varchar(160) not null, original_company_name varchar(160) not null, dt_from timestamp not null, dt_to timestamp not null, source varchar(30) not null default '', primary key(actor_id, company_name, dt_from, dt_to));
create table gha_imported_shas(sha text not null, dt timestamp default now() not null, primary key(sha));
