select count(*), max(request_id) from  request where request_type = 'movie'
and response_status = 200
union
select count(*), max(request_id) from  request where request_type = 'credits'
and response_status != 200
union
select count(*), max(request_id) from  request where request_type = 'movie'

select * from request where request_type = 'credits'

sele
with cte AS
         (SELECT *, ROW_NUMBER() OVER (PARTITION BY request_id) AS seq
          FROM request
          WHERE request_type = 'movie') select * from cte where seq = 2


select max(request_id) from public.request

select extract(year from release_date) as year, sum(revenue) as revenue from Vmovie
where revenue > 0 and extract(year from release_date) <> -1
group by extract(year from release_date)
order by  revenue desc

select title, count(*) from vmovie
group by title having count(*) > 1
order by 2 desc

select title, count(*) from movie
group by title having count(*) > 1
order by 2 desc

select concat(s::varchar(8),movie_id) , m.* from generate_series(10000, 10000) s
cross join movie m

select * from  request where request_id = 134189
select * from  vMovie where movie_id = 696071

select * from request where request_type = 'person'

SHOW hba_file;

with series AS
         (SELECT GENERATE_SERIES(1, 1000000) AS s)
insert into public.request_stream
(
request_url
)
select concat('https://api.themoviedb.org/3/person/',s::varchar(10),'?api_key=dd764c65e8685d30f05dddbe0f2f9e04')
from series

drop table if exists public.request_stream
create table public.request_stream
(
request_url text PRIMARY KEY ,
stream_id varchar(10) null,
response_status int null,
response_text text null
)

create index stream_idx on public.request_stream(stream_id)

insert into movie_credit
(movie_id, person_id, character_name, cast_order_no, cast_role)
select distinct request_id as movie_id,
                (jsonb_array_elements(response_text -> 'cast') ->> 'id')::integer as person_id,
                jsonb_array_elements(response_text -> 'cast') ->> 'character' as character_name,
                (jsonb_array_elements(response_text -> 'cast') ->> 'order')::integer as cast_order_no,
                jsonb_array_elements(response_text -> 'cast') ->> 'known_for_department' as cast_role
from request
where request_type = 'credits'



select distinct jsonb_array_elements(response_text -> 'spoken_languages') ->> 'iso_639_1' as language_code,
                jsonb_array_elements(response_text -> 'spoken_languages') ->> 'name' as language_descr,
                jsonb_array_elements(response_text -> 'spoken_languages') ->> 'english_name' as english_descr,

from request
where request_type = 'credits'
SELECT request.request_id    AS movie_id
FROM request
WHERE request.request_type::text = 'credits'::text
  AND (request.response_text ->> 'title'::text) IS NOT NULL;

       request.response_text ->> 'title'::text                                     AS title,
       request.response_text ->> 'status'::text                                    AS status,
       request.response_text ->> 'imdb_id'::text                                   AS imdb_id,
       request.response_text ->> 'homepage'::text                                  AS homepage,
       request.response_text ->> 'poster_path'::text                               AS poster_path,
       request.response_text ->> 'backdrop_path'::text                             AS backdrop_path,
       request.response_text ->> 'original_title'::text                            AS original_title,
       request.response_text ->> 'original_language'::text                         AS original_language,
       request.response_text ->> 'tagline'::text                                   AS tagline,
       request.response_text ->> 'overview'::text                                  AS overview,
       request.response_text ->> 'belongs_to_collection'::text                     AS belongs_to_collection,
       request.response_text ->> 'runtime'::text                                   AS runtime,
       TO_DATE(request.response_text ->> 'release_date'::text, 'yyyy-mm-dd'::text) AS release_date,
       (request.response_text ->> 'budget'::text)::bigint                          AS budget,
       (request.response_text ->> 'revenue'::text)::bigint                         AS revenue,
       ((request.response_text ->> 'popularity'::text))::numeric(7, 2)             AS popularity,
       (request.response_text ->> 'vote_count'::text)::integer                     AS vote_count,
       ((request.response_text ->> 'vote_average'::text))::numeric(5, 2)           AS vote_average
FROM request
WHERE request.request_type::text = 'movie'::text
  AND (request.response_text ->> 'title'::text) IS NOT NULL;

select * from worked_together_vw where character_name = 'James Bond' and co_character_name = 'Q'

select person_id, co_person_id, count(*) from worked_together_vw
   where release_date > '19900101' and upper(character_name) not like '%VOICE%'
group by person_id, co_person_id
order by count(*) desc

select * from worked_together_vw where person_id = 4566 and co_person_id = 11184
select * from worked_together_vw where person_id = 34445 and co_person_id = 34329

order by release_date
drop view public.worked_together_vw


create view public.worked_together_vw AS

with credit as
(
select mc.movie_id,
       m.title as movie_title,
       m.release_date,
       mc.person_id,
       mc.character_name,
       mc.cast_order_no,
       mc.cast_role
from   public.movie_credit mc
join   public.movie m
on     mc.movie_id = m.movie_id
)
select c1.movie_id,
       c1.movie_title,
       c1.release_date,
       c1.person_id,
       c1.character_name,
       c1.cast_order_no,
       c1.cast_role,
       c2.person_id co_person_id,
       c2.character_name co_character_name,
       c2.cast_order_no co_cast_name,
       c2.cast_role co_cast_role
from   credit c1
join   credit c2
on     c1.movie_id = c2.movie_id and
       c1.person_id != c2.person_id

--select * from same_movie_diff_actor
,
acted_together AS
(
SELECT person_id, other_person_id, count(*) count
FROM same_movie_diff_actor
--where (person_id = 2955 and other_person_id = 1248) or
 --     (person_id = 1248 and other_person_id = 2955)
group by  person_id, other_person_id
having count(*) > 1
),
dup as
(select m1.person_id, m1.other_person_id
 from acted_together m1
 join acted_together m2 on m1.person_id = m2.other_person_id
 and m1.other_person_id = m2.person_id)
select m.title, a.person_id, a.other_person_id,mc.character_name, mc1.character_name from dup a
join movie_credit mc on a.person_id = mc.person_id
join movie_credit mc1 on a.other_person_id = mc1.person_id AND
     mc.movie_id = mc1.movie_id
left join movie m on mc.movie_id = m.movie_id

select distinct da.movie_id, m.title, a.person_id, da.character_name, a.other_person_id, da.other_character_name, a.count
from acted_together a
join same_movie_diff_actor da on a.person_id = da.person_id and
                                 a.other_person_id = da.other_person_id
left join movie m on da.movie_id = m.movie_id
where character_name like '%Bourne%'
order by a.count desc


select distinct a.person_id, a.other_person_id, ma.movie_id, a.count
from acted_together a
join same_movie_diff_actor ma on  a.person_id = ma.person_id
join same_movie_diff_actor ma1 on  a.other_person_id = ma.person_id
order by 1,2

select * from movie_credit where person_id in( 9878, 10342)
order by movie_id


select * from movie_credit where character_name = 'James Bond'

select extract(year from release_date) release_year, count(*) no_of_releases, sum(revenue) from public.movie_json_vw
group by release_year
order by release_year desc


create or replace view public.movie_json_vw
(
    movie_id, title, status, imdb_id, homepage, poster_path, backdrop_path, original_title, original_language,
    tagline, overview, belongs_to_collection, runtime, release_date, budget, revenue, popularity, vote_count,
    vote_average
)
as
select request.request_id                                                          as movie_id,
       request.response_text ->> 'title'::text                                     as title,
       request.response_text ->> 'status'::text                                    as status,
       request.response_text ->> 'imdb_id'::text                                   as imdb_id,
       request.response_text ->> 'homepage'::text                                  as homepage,
       request.response_text ->> 'poster_path'::text                               as poster_path,
       request.response_text ->> 'backdrop_path'::text                             as backdrop_path,
       request.response_text ->> 'original_title'::text                            as original_title,
       request.response_text ->> 'original_language'::text                         as original_language,
       request.response_text ->> 'tagline'::text                                   as tagline,
       request.response_text ->> 'overview'::text                                  as overview,
       request.response_text ->> 'belongs_to_collection'::text                     as belongs_to_collection,
       request.response_text ->> 'runtime'::text                                   as runtime,
       to_date(request.response_text ->> 'release_date'::text, 'yyyy-mm-dd'::text) as release_date,
       (request.response_text ->> 'budget'::text)::bigint                          as budget,
       (request.response_text ->> 'revenue'::text)::bigint                         as revenue,
       ((request.response_text ->> 'popularity'::text))::numeric(7, 2)             as popularity,
       (request.response_text ->> 'vote_count'::text)::integer                     as vote_count,
       ((request.response_text ->> 'vote_average'::text))::numeric(5, 2)           as vote_average
from request
where request.request_type::text = 'movie'::text
  and (request.response_text ->> 'title'::text) is not null;