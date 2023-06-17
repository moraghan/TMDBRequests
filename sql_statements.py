create_objects_sql = """
 

         create table if not exists public.request_q
         (
         request_type    varchar(10)                         not null,
         request_id      integer                             not null,
         response_status varchar(10),
         last_updated    timestamp default CURRENT_TIMESTAMP not null,
         primary key (request_id, request_type)
         );

         create table if not exists public.movie
         (
         movie_id          integer not null primary key,
         title             text,
         adult_movie       boolean,
         status            varchar(20),
         imdb_id           varchar(10),
         homepage          text,
         poster_path       image_path,
         backdrop_path     image_path,
         original_title    text,
         original_language varchar(10),
         tagline           text,
         overview          text,
         collection_id     integer,
         runtime           integer,
         release_date      date,
         budget            bigint,
         revenue           bigint,
         popularity        numeric(7, 2),
         vote_count        integer,
         vote_average      numeric(5, 2),
         last_updated      timestamp not null default current_timestamp,
         profit            bigint generated always as ((revenue - budget)) stored
         );

         create table if not exists public.collection
         (
         collection_id          integer not null primary key,
         collection_descr       varchar(200),
         collection_poster_path image_path,
         backdrop_poster_path   image_path
         );

         create table if not exists public.company
         (
         company_id        integer primary key,
         company_name      varchar(200),
         homepage          varchar(300),
         logo_path         varchar(100),
         headquarters      varchar(1000),
         origin_country    char(2),
         parent_company_id integer
         );

         create table if not exists public.country
         (
         country_code  char(2) not null primary key,
         country_descr varchar(100) 
         );
         
         create table if not exists public.movie_country
         (
         movie_id       int not null,
         country_code   char(2) not null,
         primary key (movie_id, country_code)
         );

         create table if not exists public.movie_company
         (
         movie_id       int not null,
         company_id     int not null,
         primary key (movie_id, company_id)
         );
         
         create table if not exists public.movie_credit
         (
         movie_id       int not null,
         person_id      int not null,
         cast_order_no  int not null,
         character_name varchar(1000),
         primary key (movie_id, person_id, cast_order_no)
         );

         create table if not exists public.movie_crew
         (
         movie_id       int not null,
         person_id      int not null,
         department     varchar(50),
         job            varchar(100),
         primary key (movie_id, person_id, department, job)
         );
         
         create table if not exists public.genre
         (
         genre_id       int not null,
         genre_descr    varchar(100),
         primary key (genre_id)
         );
         
         create table if not exists public.movie_genre
         (
         movie_id       int not null,
         genre_id       int not null,
         primary key (movie_id, genre_id)
         );
         
         create table if not exists public.person
         (
         person_id     int not null primary key,
         name          varchar(100),
         adult_movie   boolean,
         gender        char(1),
         imdb_id       varchar(20),
         birthday      date,
         deathday      date,
         homepage      varchar(1000),
         biography     text,
         popularity    numeric(7, 2),
         profile_path  image_path,
         birth_place   varchar(200),
         primary_role  varchar(20) 
         );

         create table if not exists public.request_q
         (
         request_type    varchar(10)                         not null,
         request_id      integer                             not null,
         response_status varchar(10),
         last_updated    timestamp default CURRENT_TIMESTAMP not null,
         primary key (request_id, request_type)
         );

alter table public.request_q
    owner to postgres;
     """

get_last_request_for_type_sql = """

      select max(request_id) 
      from   public.request 
      where  request_type = %s and 
             request_id >= %s;

     """

insert_new_request_record_sql = """

         insert into public.request (request_type,
                                     request_id,
                                     response_text)
         select %s, %s, %s
         where not exists (select 1 from public.request where request_type = %s and
                                                              request_id = %s)
         on conflict do nothing

     """
insert_new_movie_record_sql = """
insert into public.movie 
(
movie_id, 
title, 
adult_movie,
status, 
imdb_id, 
homepage, 
poster_path, 
backdrop_path, 
original_title,
original_language, 
tagline, 
overview, 
collection_id, 
runtime, 
release_date, 
budget, 
revenue,
popularity, 
vote_count, 
vote_average
)
select %s as movie_id, 
       %s as title, 
       %s as adult,
       %s as status, 
       %s as imdb_id, 
       %s as homepage, 
       %s as poster_path, 
       %s as backdrop_path, 
       %s as original_title,
       %s as original_language, 
       %s as tagline, 
       %s as overview, 
       %s as collection_id, 
       %s as runtime, 
       %s as release_date, 
       %s as budget, 
       %s as revenue,
       %s as popularity, 
       %s as vote_count, 
       %s as vote_average
where not exists (select 1 from public.movie where movie_id = %s)
on conflict (movie_id) do nothing;

"""

insert_new_collection_record_sql = """

insert into public.collection
(
collection_id,
collection_descr,
collection_poster_path,
backdrop_poster_path
)
select %s as collection_id,
       %s as collection_descr,
       %s as collection_poster_path,
       %s as backdrop_poster_path
where not exists (select 1 from public.collection where collection_id = %s)

"""

insert_new_genre_record_sql = """

insert into public.genre
(
genre_id,
genre_descr
)
select %s as genre_id,
       %s as genre_descr
where not exists (select 1 from public.genre where genre_id = %s)

"""

insert_new_movie_genre_record_sql = """

insert into public.movie_genre
(
movie_id,
genre_id
)
select %s as movie_id,
       %s as genre_id
where not exists (select 1 from public.movie_genre where movie_id = %s and genre_id = %s)

"""

insert_new_country_record_sql = """

insert into public.country
(
country_code,
country_descr
)
select %s as country_code,
       %s as country_descr
where not exists (select 1 from public.country where country_code = %s)

"""


insert_new_movie_country_record_sql = """

insert into public.movie_country
(
movie_id,
country_code
)
select %s as movie_id,
       %s as country_code
where not exists (select 1 from public.movie_country where movie_id = %s and country_code = %s)

"""

insert_new_movie_company_record_sql = """

insert into public.movie_company
(
movie_id,
company_id
)
select %s as movie_id,
       %s as company_id
where not exists (select 1 from public.movie_company where movie_id = %s and company_id = %s)

"""

insert_new_person_record_sql = """

insert into public.person
(
person_id,
name,
adult_movie,
gender,
imdb_id,
birthday,
deathday,
homepage,
biography,
popularity,
profile_path,
birth_place,
primary_role
)
select %s as person_id,
       %s as name,
       %s as adult_movie,
       %s as gender,
       %s as imdb_id,
       %s as birthday,
       %s as deathday,
       %s as homepage,
       %s as biography,
       %s as popularity,
       %s as profile_path,
       %s as birth_place,
       %s as primary_role
where not exists (select 1 from public.person where person_id = %s)

"""

insert_new_company_record_sql = """

insert into public.company
(
company_id,
company_name,
homepage,
logo_path,
headquarters,
origin_country,
parent_company_id
)
select %s as company_id,
       %s as company_name,
       %s as homepage,
       %s as logo_path,
       %s as headquarters,  
       %s as origin_country, 
       %s as parent_company_id
where not exists (select 1 from public.company where company_id = %s)

"""

insert_new_movie_credit_record_sql = """

insert into public.movie_credit
(
movie_id,
person_id,
cast_order_no,
character_name
)
select %s as movie_id,
       %s as person_id,
       %s as cast_order_no,
       %s as character_name
where not exists (select 1 from public.movie_credit where movie_id = %s and person_id = %s and cast_order_no = %s)

"""

insert_new_movie_crew_record_sql = """

insert into public.movie_crew
(
movie_id,
person_id,
department,
job
)
select %s as movie_id,
       %s as person_id,
       %s as department,
       %s as job
where not exists (select 1 from public.movie_crew where movie_id = %s and person_id = %s and department = %s and job = %s)

"""

get_missing_person_from_credit_sql = """

select distinct 
       mc.person_id
from   public.movie_credit mc
left 
join  public.person p 
on    mc.person_id = p.person_id
where p.person_id is null

union

select distinct 
       mc.person_id
from   public.movie_crew mc
left 
join  public.person p 
on    mc.person_id = p.person_id
where p.person_id is null

"""


get_next_request_id_for_type_sql = """

select request_id as request_id
from   public.request_q
where  request_type = %s and 
       response_status = 'Waiting'
order
by     random() limit 1

"""

update_request_for_type_sql = """

update public.request_q
set    response_status = %s,
       last_updated = now()
where  request_type = %s and 
       request_id = %s

"""

insert_new_requests_q_sql = """


insert into public.request_q
(
request_type,
request_id,
response_status
)
with cte as
         (select 'movie'                     as request_type,
                 generate_series(200100, (200100 + 10000)) as request_id,
                 'Waiting'                   as response_status)
select * from cte
where not exists (select 1 from public.movie m where m.movie_id = cte.request_id)
and not exists (select 1 from public.request_q q where q.request_type = cte.request_type and q.request_id = cte.request_id)
on conflict do nothing;

insert into public.request_q
(
request_type,
request_id,
response_status
)
select distinct
       'person'      as request_type,
       mc.person_id  as request_id,
       'Waiting'     as response_status
from public.movie_credit mc
where not exists (select 1 from public.person p where p.person_id = mc.person_id)
and not exists (select 1 from public.request_q q where q.request_type = 'person' and q.request_id = mc.person_id)
on conflict do nothing;

insert into public.request_q
(
request_type,
request_id,
response_status
)
select distinct
       'person'      as request_type,
       mc.person_id  as request_id,
       'Waiting'     as response_status
from public.movie_crew mc
where not exists (select 1 from public.person p where p.person_id = mc.person_id)
and not exists (select 1 from public.request_q q where q.request_type = 'person' and q.request_id = mc.person_id)
on conflict do nothing;

insert into public.request_q
(
request_type,
request_id,
response_status
)
select distinct
       'company'      as request_type,
       mc.company_id  as request_id,
       'Waiting'     as response_status
from public.movie_company mc
where not exists (select 1 from public.company c where c.company_id = mc.company_id)
and not exists (select 1 from public.request_q q where q.request_type = 'company' and q.request_id = mc.company_id)
on conflict do nothing;

"""
