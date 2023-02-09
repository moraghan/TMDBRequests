create_objects_sql = """

         create or replace function public.is_date (text) returns integer as $$
         begin
              if ($1 is null) then
                  return 1;
              end if;
              perform $1::date;
              return 1;
         exception when others then
              return 0;
         end;
         $$ language plpgsql;

         do $$
         begin
             if not exists (select 1
                            from   pg_type 
                            where  typname = 'image_path' and
                                   typnamespace = 'public'::regnamespace) then
                         create domain public.image_path varchar(100);
             end if;
         end$$;

         create table if not exists public.request
         (
         request_type  varchar(10)  not null,
         request_id    integer      not null,
         response_text jsonb,
         primary key   (request_id, request_type)
         );

         create table if not exists public.movie
         (
         movie_id          integer not null primary key,
         title             varchar(100),
         status            varchar(20),
         imdb_id           varchar(10),
         homepage          varchar(150),
         poster_path       image_path,
         backdrop_path     image_path,
         original_title    varchar(100),
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
         profit            bigint generated always as ((revenue - budget)) stored
         );

         create table if not exists public.collection
         (
         collection_id          integer not null primary key,
         collection_descr       varchar(100) unique,
         collection_poster_path image_path,
         backdrop_poster_path   image_path
         );

         create table if not exists public.company
         (
         id                integer primary key,
         title             text,
         homepage          text,
         logo_path         text,
         headquarters      text,
         origin_country    char(2),
         parent_company_id integer
         );

         create table if not exists public.country
         (
         country_code  char(2) not null primary key,
         country_descr varchar(100) unique
         );

         create table if not exists public.movie_credit
         (
         movie_id       int not null,
         person_id      int not null,
         cast_order_no  int not null,
         cast_role      varchar(10),
         character_name varchar(100),
         primary key (movie_id, person_id, cast_order_no)
         );

         create table if not exists public.person
         (
         person_id     int not null primary key,
         name          varchar(100),
         adult_movie   boolean,
         gender        int,
         imdb_id       varchar(20),
         birthday      date,
         deathday      date,
         homepage      varchar(100),
         biography     text,
         popularity    numeric(7, 2),
         profile_path  image_path,
         birth_place   varchar(200),
         primary_role  varchar(10) 
         );

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
