create schema reading;

create table reading.book
(
	book_id serial,
	title varchar,
	author varchar,
	series varchar,
	word_count integer,
	comment varchar,
	rating integer,
	primary key (book_id)
);

create table reading.books_log
(
	date date not null,
	book_id integer not null,
	status varchar,
	percentage integer,
	streak integer,
	primary key (date, book_id),
	foreign key (book_id) references reading.book
);