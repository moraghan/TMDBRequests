SELECT request_id                                              AS movie_id,
       response_text ->> 'title'                               AS title,
       response_text ->> 'status'                              AS status,
       response_text ->> 'imdb_id'                             AS imdb_id,
       response_text ->> 'homepage'                            AS homepage,
       response_text ->> 'poster_path'                         AS poster_path,
       response_text ->> 'backdrop_path'                       AS backdrop_path,
       response_text ->> 'original_title'                      AS original_title,
       response_text ->> 'original_language'                   AS original_language,
       response_text ->> 'tagline'                             AS tagline,
       response_text ->> 'overview'                            AS overview,
       response_text ->> 'belongs_to_collection'               AS belongs_to_collection,
       response_text ->> 'runtime'                             AS runtime,
       TO_DATE(response_text ->> 'release_date', 'yyyy-mm-dd') AS release_date,
       (response_text ->> 'budget')::bigint                    AS budget,
       (response_text ->> 'revenue')::bigint                   AS revenue,
       (response_text ->> 'popularity')::numeric(7, 2)         AS popularity,
       (response_text ->> 'vote_count')::int                   AS vote_count,
       (response_text ->> 'vote_average')::numeric(5, 2)       AS vote_average

FROM request
WHERE request_type = 'movie'
  AND response_text ->> 'title' IS NOT NULL
