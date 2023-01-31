

create or replace view public.movie_json_vw
(
movie_id, title, adult_movie_ind, status, imdb_id, homepage, poster_path, backdrop_path, original_title, original_language,
tagline, overview, belongs_to_collection, runtime, release_date, budget, revenue, popularity, vote_count,
vote_average
)
as
select request.request_id                                                          as movie_id,
       request.response_text ->> 'title'::text                                     as title,
       case when request.response_text ->> 'adult'::varchar(5) = 'false'
            then 0 else 1 end                                                      as adult_movie_ind,
       request.response_text ->> 'status'::text                                    as status,
       request.response_text ->> 'imdb_id'::text                                   as imdb_id,
       request.response_text ->> 'homepage'::text                                  as homepage,
       request.response_text ->> 'poster_path'::image_path                         as poster_path,
       request.response_text ->> 'backdrop_path'::image_path                       as backdrop_path,
       request.response_text ->> 'original_title'::text                            as original_title,
       request.response_text ->> 'original_language'::text                         as original_language,
       request.response_text ->> 'tagline'::text                                   as tagline,
       request.response_text ->> 'overview'::text                                  as overview,
       request.response_text ->> 'belongs_to_collection'::text                     as belongs_to_collection,
       request.response_text ->> 'runtime'::text                                   as runtime,
       to_date(request.response_text ->> 'release_date'::text, 'yyyy-mm-dd'::text) as release_date,
       (request.response_text ->> 'budget'::text)::bigint                          as budget,
       (request.response_text ->> 'revenue'::text)::bigint                         as revenue,
       (request.response_text ->> 'popularity'::text)::numeric(7, 2)               as popularity,
       (request.response_text ->> 'vote_count'::text)::integer                     as vote_count,
       (request.response_text ->> 'vote_average'::text)::numeric(5, 2)             as vote_average
from request
where request.request_type::text = 'movie'::text and
     (request.response_text ->> 'title'::text) is not null;



