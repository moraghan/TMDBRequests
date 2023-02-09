create or replace view public.movie_json_vw
(
movie_id, title, adult_movie_ind, status, imdb_id, homepage, poster_path, backdrop_path, original_title, original_language,
tagline, overview, belongs_to_collection, runtime, release_date, budget, revenue, popularity, vote_count,
vote_average
)
as
select request_id                                                          as movie_id,
       response_text ->> 'title'::text                                     as title,
       case when response_text ->> 'adult'::varchar(5) = 'false'
            then 0 else 1 end                                                      as adult_movie_ind,
       response_text ->> 'status'::text                                    as status,
       response_text ->> 'imdb_id'::text                                   as imdb_id,
       response_text ->> 'homepage'::text                                  as homepage,
       response_text ->> 'poster_path'::image_path                         as poster_path,
       response_text ->> 'backdrop_path'::image_path                       as backdrop_path,
       response_text ->> 'original_title'::text                            as original_title,
       response_text ->> 'original_language'::text                         as original_language,
       response_text ->> 'tagline'::text                                   as tagline,
       response_text ->> 'overview'::text                                  as overview,
       response_text ->> 'belongs_to_collection'::text                     as belongs_to_collection,
       response_text ->> 'runtime'::text                                   as runtime,
       to_date(response_text ->> 'release_date'::text, 'yyyy-mm-dd'::text) as release_date,
       (response_text ->> 'budget'::text)::bigint                          as budget,
       (response_text ->> 'revenue'::text)::bigint                         as revenue,
       (response_text ->> 'popularity'::text)::numeric(7, 2)               as popularity,
       (response_text ->> 'vote_count'::text)::integer                     as vote_count,
       (response_text ->> 'vote_average'::text)::numeric(5, 2)             as vote_average
from request
where request_type::text = 'movie'::text and
     (response_text ->> 'title'::text) is not null;

drop view public.person_json_vw
create or replace view public.person_json_vw as

select request_id                                                          as person_id,
       response_text ->> 'name'::text                                      as name,
       case when response_text ->> 'adult'::varchar(5) = 'false'
            then 0 else 1 end                                                      as adult_movie_ind,
       response_text ->> 'gender'::text                                    as gender,
       response_text ->> 'imdb_id'::text                                   as imdb_id,
       to_date_yyyymmdd(response_text ->> 'birthday'::text)           as birthday,
       to_date_yyyymmdd(response_text ->> 'deathday'::text)           as deathday,
       response_text ->> 'homepage'::text                                  as homepage,
       response_text ->> 'biography'::text                                 as biography,
       (response_text ->> 'popularity'::text)::numeric(7, 2)               as popularity,
       response_text ->> 'profile_path'::image_path                        as profile_path,
       response_text ->> 'place_of_birth'::text                            as place_of_birth,
       response_text ->> 'known_for_department'::text                      as known_for_department

from request
where request_type::text = 'person'::text and
     (response_text ->> 'name'::text) is not null;

create view public.movie_person_json_vw as

with mvp as
(
select request_id as movie_id,
       response_text,
       (jsonb_array_elements(response_text -> 'cast') ->> 'id')::integer as cast_person_id,
       (jsonb_array_elements(response_text -> 'crew') ->> 'id')::integer as cast_person_id,
       *
from request
where  request_type = 'credits'

)
select  request_id as movie_id,
        (jsonb_array_elements(response_text -> 'cast') ->> 'id')::integer as person_id,
        (jsonb_array_elements(response_text -> 'cast') ->> 'adult')::boolean as adult_id,
        jsonb_array_elements(response_text -> 'cast') ->> 'character' as character_name,
        (jsonb_array_elements(response_text -> 'cast') ->> 'order')::integer as cast_order_no,
        jsonb_array_elements(response_text -> 'cast') ->> 'known_for_department' as cast_role,
        (jsonb_array_elements(response_text -> 'cast') ->> 'popularity')::numeric(7,2) as popularity,
        jsonb_array_elements(response_text -> 'cast') ->> 'profile_path' as prifile_path,
        null as crew_job,
        null as crew_department
from    request
where   request_type = 'credits'

union

select  request_id as movie_id,
        (jsonb_array_elements(response_text -> 'crew') ->> 'id')::integer as person_id,
        (jsonb_array_elements(response_text -> 'cast') ->> 'adult')::boolean as adult_id,
        jsonb_array_elements(response_text -> 'crew') ->> 'character' as character_name,
        (jsonb_array_elements(response_text -> 'crew') ->> 'order')::integer as cast_order_no,
        jsonb_array_elements(response_text -> 'crew') ->> 'known_for_department' as cast_role,
        (jsonb_array_elements(response_text -> 'crew') ->> 'popularity')::numeric(7,2) as popularity,
        jsonb_array_elements(response_text -> 'crew') ->> 'profile_path' as profile_path,
        jsonb_array_elements(response_text -> 'crew') ->> 'job' as crew_job,
        jsonb_array_elements(response_text -> 'crew') ->> 'department' as crew_department
from    request
where   request_type = 'credits'

select * from request where request_type = 'credits' and request_id = 2

select * from movie_person_json_vw where person_id is null

create or replace view public.movie_person_json_vw as

with mvp as
(
select request_id as movie_id,
       response_text,
       (jsonb_array_elements(response_text -> 'cast') ->> 'id')::integer as cast_person_id,
       (jsonb_array_elements(response_text -> 'crew') ->> 'id')::integer as crew_person_id
from request
where  request_type = 'credits'

)
select  movie_id,
        (jsonb_array_elements(response_text -> 'cast') ->> 'id')::integer as person_id,
        (jsonb_array_elements(response_text -> 'cast') ->> 'adult')::boolean as adult_id,
        jsonb_array_elements(response_text -> 'cast') ->> 'character' as character_name,
        (jsonb_array_elements(response_text -> 'cast') ->> 'order')::integer as cast_order_no,
        jsonb_array_elements(response_text -> 'cast') ->> 'known_for_department' as cast_role,
        (jsonb_array_elements(response_text -> 'cast') ->> 'popularity')::numeric(7,2) as popularity,
        jsonb_array_elements(response_text -> 'cast') ->> 'profile_path' as prifile_path,
        null as crew_job,
        null as crew_department
from    mvp
where   cast_person_id is not null

union

select  movie_id,
        (jsonb_array_elements(response_text -> 'crew') ->> 'id')::integer as person_id,
        (jsonb_array_elements(response_text -> 'cast') ->> 'adult')::boolean as adult_id,
        jsonb_array_elements(response_text -> 'crew') ->> 'character' as character_name,
        (jsonb_array_elements(response_text -> 'crew') ->> 'order')::integer as cast_order_no,
        jsonb_array_elements(response_text -> 'crew') ->> 'known_for_department' as cast_role,
        (jsonb_array_elements(response_text -> 'crew') ->> 'popularity')::numeric(7,2) as popularity,
        jsonb_array_elements(response_text -> 'crew') ->> 'profile_path' as profile_path,
        jsonb_array_elements(response_text -> 'crew') ->> 'job' as crew_job,
        jsonb_array_elements(response_text -> 'crew') ->> 'department' as crew_department
from    mvp
where   crew_person_id is not null
