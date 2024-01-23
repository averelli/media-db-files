create schema shows;

create table shows.show
(
	show_id serial,
	title varchar,
	primary key (show_id)
);

create table shows.episode
(
	episode_id serial,
	show_id integer,
	season integer,
	episode_num integer,
	length integer,
	primary key (episode_id),
	foreign key (show_id) references shows.show
);

create table shows.shows_log
(
	date date not null,
	episode_id integer not null,
	language varchar,
	primary key (date, episode_id),
	foreign key (episode_id) references shows.episode
);