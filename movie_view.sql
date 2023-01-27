create or replace view public.vmovie
            (movie_id, title, status, imdb_id, homepage, poster_path, backdrop_path, original_title, original_language,
             tagline, overview, belongs_to_collection, runtime, release_date, budget, revenue, popularity, vote_count,
             vote_average)
as
SELECT request.request_id                                                          AS movie_id,
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

alter table public.vmovie
    owner to postgres;

