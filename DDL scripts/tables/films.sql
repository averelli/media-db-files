create schema films;

create table films.film
(
	film_id serial,
	title varchar,
	length integer,
	primary key (film_id)
);

create table films.films_log
(
	date date not null,
	film_id integer not null,
	language varchar,
	primary key (date, film_id),
	foreign key (film_id) references films.film
);