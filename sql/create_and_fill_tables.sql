create schema if not exists raw; 

create table if not exists raw.matchs (
	match_id varchar,
	"type" varchar,
	"date" varchar,
	best_of_type varchar,
	team1_id varchar,
	team2_id varchar,
	team1_score varchar,
	team2_score varchar
);

create table if not exists raw.match_stats (
	match_id varchar,
	side varchar,
	map_name varchar,
	adr varchar,
	diff varchar,
	kast varchar,
	kd varchar,
	nick varchar,
	player_id varchar,
	rating varchar,
	team_id varchar
);

create table if not exists raw.teams (
	team_id varchar,
	name varchar,
	link varchar
);

create table if not exists raw.players (
	player_id varchar,
	team_id varchar,
	nick varchar
);

create table if not exists raw.maps_vetos (
	match_id varchar,
	type varchar,
	choice_number varchar,
	map_name varchar,
	team_id varchar
);

create table if not exists raw.maps_results (
	match_id varchar,
	choice_number varchar,
	map_name varchar,
	pick_team_id varchar,
	ct varchar,
	result varchar,
	t varchar,
	team_name varchar
);

create table if not exists raw.maps (
	id serial primary key,
	name varchar not null
);

insert into raw.maps (name)
values 
	('Vertigo'),
	('Dust2'),
	('Overpass'),
	('Ancient'),
	('Mirage'),
	('Inferno'),
	('Nuke');
	
