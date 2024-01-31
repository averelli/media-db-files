create schema reports;

create view reports.viewing_history(date, title, num_watched, length, type) as
SELECT l.date,
       s.title,
       count(l.episode_id) AS num_watched,
       avg(e.length)       AS length,
       'show'              AS type
FROM shows.shows_log l
         JOIN shows.episode e ON e.episode_id = l.episode_id
         JOIN shows.show s ON s.show_id = e.show_id
GROUP BY l.date, s.title
UNION ALL
SELECT l.date,
       f.title,
       1            AS num_watched,
       f.length,
       'film'       AS type
FROM films.films_log l
         JOIN films.film f ON f.film_id = l.film_id;


create view reports.reading_history(date, title, wpd, type, fandom) as
SELECT l.date,
       b.title,
       round(l.percentage::numeric / 100::numeric * b.word_count::numeric) AS wpd,
       b.series AS series
FROM reading.books_log l
         JOIN reading.book b ON b.book_id = l.book_id