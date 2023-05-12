drop view public.movie_json_vw

select * from movie_json_small_vw where release_date > '20230101'
create or replace view public.movie_json_small_vw

as
select request_id                                                          as movie_id,
       response_text ->> 'title'::text                                     as title,
       response_text ->> 'status'::text                                    as status,
       response_text ->> 'imdb_id'::text                                   as imdb_id,
       response_text ->> 'homepage'::text                                  as homepage,
       response_text ->> 'original_title'::text                            as original_title,
       response_text ->> 'original_language'::text                         as original_language,
       response_text ->> 'tagline'::text                                   as tagline,
       response_text ->> 'overview'::text                                  as overview,
       (response_text -> 'belongs_to_collection' ->> 'id')::integer        as collection_id,
       (response_text ->> 'runtime')::integer                                   as runtime,
       to_date_yyyymmdd(response_text ->> 'release_date'::text)            as release_date,
       (response_text ->> 'budget'::text)::bigint                          as budget,
       (response_text ->> 'revenue'::text)::bigint                         as revenue,
       (response_text ->> 'popularity'::text)::numeric(7, 2)               as popularity,
       (response_text ->> 'vote_count'::text)::integer                     as vote_count,
       (response_text ->> 'vote_average'::text)::numeric(5, 2)             as vote_average
from request
where request_type::text = 'movie'::text and
     (response_text ->> 'title'::text) is not null and
     response_text ->> 'adult'::varchar(5) = 'false';


create or replace view public.movie_json_vw

as
select request_id                                                          as movie_id,
       response_text ->> 'title'::text                                     as title,
       case when response_text ->> 'adult'::varchar(5) = 'false'
            then 0 else 1 end                                              as adult_movie_ind,
       response_text ->> 'status'::text                                    as status,
       response_text ->> 'imdb_id'::text                                   as imdb_id,
       response_text ->> 'homepage'::text                                  as homepage,
       response_text ->> 'poster_path'::image_path                         as poster_path,
       response_text ->> 'backdrop_path'::image_path                       as backdrop_path,
       response_text ->> 'original_title'::text                            as original_title,
       response_text ->> 'original_language'::text                         as original_language,
       response_text ->> 'tagline'::text                                   as tagline,
       response_text ->> 'overview'::text                                  as overview,
       (response_text -> 'belongs_to_collection' ->> 'id')::integer        as collection_id,
       (response_text ->> 'runtime')::integer                                   as runtime,
       to_date_yyyymmdd(response_text ->> 'release_date'::text)            as release_date,
       (response_text ->> 'budget'::text)::bigint                          as budget,
       (response_text ->> 'revenue'::text)::bigint                         as revenue,
       (response_text ->> 'popularity'::text)::numeric(7, 2)               as popularity,
       (response_text ->> 'vote_count'::text)::integer                     as vote_count,
       (response_text ->> 'vote_average'::text)::numeric(5, 2)             as vote_average
from request
where request_type::text = 'movie'::text and
     (response_text ->> 'title'::text) is not null;


create or replace view public.person_json_vw as

select request_id                                                          as person_id,
       response_text ->> 'name'::text                                      as name,
       case when response_text ->> 'adult'::varchar(5) = 'false'
            then 0 else 1 end                                              as adult_movie_ind,
       response_text ->> 'gender'::text                                    as gender,
       response_text ->> 'imdb_id'::text                                   as imdb_id,
       to_date_yyyymmdd(response_text ->> 'birthday'::text)                as birthday,
       to_date_yyyymmdd(response_text ->> 'deathday'::text)                as deathday,
       response_text ->> 'homepage'::text                                  as homepage,
       response_text ->> 'biography'::text                                 as biography,
       (response_text ->> 'popularity'::text)::numeric(7, 2)               as popularity,
       response_text ->> 'profile_path'::image_path                        as profile_path,
       response_text ->> 'place_of_birth'::text                            as place_of_birth,
       response_text ->> 'known_for_department'::text                      as known_for_department

from request
where request_type::text = 'person'::text and
      response_text ->> 'name'::text is not null;


select jsonb_each(response_text) from request where request_type = 'company' and request_id = 2
select distinct jsonb_object_keys(response_text)
from request where request_type = 'company' and request_id = 2

create view public.movie_person_json_vw as

drop view movie_person_json_vw

create or replace view movie_person_json_vw as

with mvp as
(
select distinct
              request_id                                                                      as movie_id,
              (jsonb_array_elements(response_text -> 'cast') ->> 'id')::integer               as person_id,
              (jsonb_array_elements(response_text -> 'cast') ->> 'adult')::boolean            as adult_id,
              jsonb_array_elements(response_text -> 'cast') ->> 'character'                   as character_name,
              (jsonb_array_elements(response_text -> 'cast') ->> 'order')::integer            as cast_order_no,
              jsonb_array_elements(response_text -> 'cast') ->> 'known_for_department'        as cast_role,
              null                                                                            as crew_job,
              null                                                                            as crew_department
from request
where request_type = 'credits'

union all

select distinct
              request_id                                                                      as movie_id,
              (jsonb_array_elements(response_text -> 'crew') ->> 'id')::integer               as person_id,
              (jsonb_array_elements(response_text -> 'cast') ->> 'adult')::boolean            as adult_id,
              jsonb_array_elements(response_text -> 'crew') ->> 'character'                   as character_name,
              (jsonb_array_elements(response_text -> 'crew') ->> 'order')::integer            as cast_order_no,
              jsonb_array_elements(response_text -> 'crew') ->> 'known_for_department'        as cast_role,
              jsonb_array_elements(response_text -> 'crew') ->> 'job'                         as crew_job,
              jsonb_array_elements(response_text -> 'crew') ->> 'department'                  as crew_department
from request
where request_type = 'credits'
)
select *
from  mvp
where person_id is not null;

create or replace view company_json_vw as

select distinct request_id                                              as company_id,
                response_text ->> 'name'::text                          as company_name,
                response_text ->> 'homepage'::text                      as homepage,
                response_text ->> 'logo_path'::image_path               as logo_path,
                response_text ->> 'description'::text                   as description,
                response_text ->> 'headquarters'::text                  as headquarters,
                response_text ->> 'origin_country'::text                as origin_country,
                (r.response_text -> 'parent_company' ->> 'id')::integer as parent_company_id
from   request r
where  r.request_type = 'company'

create or replace view public.json_collection_vw as

with dc as
(
select distinct (response_text -> 'belongs_to_collection' ->> 'id')::integer               as collection_id,
                (response_text -> 'belongs_to_collection' ->> 'name')::text                as collection_name,
                (response_text -> 'belongs_to_collection' ->> 'poster_path')::image_path   as poster_path,
                (response_text -> 'belongs_to_collection' ->> 'backdrop_path')::image_path as backdrop_path
from   request
where  request_type = 'movie'
and    response_text -> 'belongs_to_collection' ->> 'id' is not null
)
select collection_id,
       collection_name,
       max(poster_path) poster_path,
       max(backdrop_path) backdrop_path
from   dc
group by collection_id,
         collection_name


create view genre_json_vw as

select distinct (jsonb_array_elements(response_text -> 'genres') ->> 'id')::integer  as genre_id,
                (jsonb_array_elements(response_text -> 'genres') ->> 'name')::text  as genre_name
from   request
where  request_type = 'movie'


---------
select count(*) from company_json_vw
select m.movie_id,
       m.title,
       m.release_date,
       p.name,
       p.birthday,
       p.deathday,
       mp.cast_order_no,
       mp.character_name,
       mp.cast_role,
       mp.crew_job,
       mp.crew_department
from movie_person_json_vw mp
join movie_json_vw m on mp.movie_id = m.movie_id
join person_json_vw p on mp.person_id = p.person_id
where m.title = 'Reservoir Dogs'
order by mp.movie_id, mp.cast_order_no

select c.collection_name, m.release_date, m.title, m.revenue - m.budget as profit,
       sum(m.revenue - m.budget ) over (partition by c.collection_name) as collection_profit

from json_collection_vw c
join movie_json_vw  m on c.collection_id = m.collection_id
where m.adult_movie_ind = 0 and m.original_language = 'en' and m.status = 'Released'
order by collection_profit desc, c.collection_name, m.release_date

select * from p where movie_id = 3


drop index
CREATE INDEX response_text_gin ON request USING gin (response_text);

select * from request where request_type = 'company' and request_id = 3

select * from movie_person_json_vw where person_id is null


create or replace view public.json_collection_vw as

with dc as
(
select distinct (response_text -> 'belongs_to_collection' ->> 'id')::integer               as collection_id,
                (response_text -> 'belongs_to_collection' ->> 'name')::text                as collection_name,
                (response_text -> 'belongs_to_collection' ->> 'poster_path')::image_path   as poster_path,
                (response_text -> 'belongs_to_collection' ->> 'backdrop_path')::image_path as backdrop_path
from   request
where  request_type = 'movie'
and    response_text -> 'belongs_to_collection' ->> 'id' is not null
)
select collection_id,
       collection_name,
       max(poster_path) poster_path,
       max(backdrop_path) backdrop_path
from   dc
group by collection_id,
         collection_name

select collection_name, count(*) from json_collection_vw group by collection_name
having count(*) > 1

select * from json_collection_vw  where collection_name = 'Django Collection'

select distinct
                (jsonb_array_elements(response_text -> 'production_companies') ->> 'id')::integer  as company_id,
                (jsonb_array_elements(response_text -> 'production_companies') ->> 'name')::text  as company_name,
                (jsonb_array_elements(response_text -> 'production_companies') ->> 'logo_path')::text  as logo_path,
                (jsonb_array_elements(response_text -> 'production_companies') ->> 'origin_country')::text  as origin_country
from   request
where  request_type = 'movie'

select distinct request_id as company_id,
                response_text ->> 'name'::text                as company_name,
                response_text ->> 'homepage'::text            as homepage,
                response_text ->> 'logo_path'::image_path     as logo_path,
                response_text ->> 'description'::text            as description,
                response_text ->> 'headquarters'::text            as headquarters,
                response_text ->> 'origin_country'::text            as origin_country,
                (response_text ->> 'parent_company')::text            as parent_company
                --(jsonb_array_elements(response_text -> 'parent_company') ->> 'id')  as origin_country
from   request
where  request_type = 'company'

select distinct request_id as company_id,
                (jsonb_array_elements(response_text -> 'crew') ->> 'id')::integer               as person_id,
                (jsonb_array_elements(response_text -> 'parent_company') ->> 'id')  as origin_country
from   request
where  request_type = 'company' and request_id = 2

select    *
from       request t

SELECT row_to_json(request_id::text)
from request where request_id = 2

	select * from json_each(select response_text from request where request_id = 2)


select distinct request_id                                              as company_id,
                response_text ->> 'name'::text                          as company_name,
                response_text ->> 'homepage'::text                      as homepage,
                response_text ->> 'logo_path'::image_path               as logo_path,
                response_text ->> 'description'::text                   as description,
                response_text ->> 'headquarters'::text                  as headquarters,
                response_text ->> 'origin_country'::text                as origin_country,
                (r.response_text -> 'parent_company' ->> 'id')::integer as parent_company_id
from   request r
where  r.request_type = 'company'


select request_ID,
       value::jsonb ->> 'id' as parent_company_id
 from request,
      jsonb_each_text(request.response_text)
 where request_type = 'company'
   and key = 'parent_company'