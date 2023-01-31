import argparse

import pg8000
import requests

parser = argparse.ArgumentParser(prog='main.py',
                                 description='Extracts TMDB data to PostgreSQL database')

parser.add_argument('-t', '--TypeOfRequest', choices=['movie', 'person', 'company'], required=True)
parser.add_argument('-s', '--StartingRequestID', type=int, default=0)
parser.add_argument('-b', '--BatchRequestSize', type=int, default=100000)

args = parser.parse_args()

def main():
    api_key = "dd764c65e8685d30f05dddbe0f2f9e04"
    request_type = args.TypeOfRequest
    starting_request_key = args.StartingRequestID
    no_of_requests_to_make = args.BatchRequestSize
    db_conn = db_create_connection()
    db_create_tables(db_conn)
    db_starting_request_key = db_get_next_request_for_type(db_conn, request_type, starting_request_key)
    print(f'starting at request key {db_starting_request_key}')
    process_requests_for_type(api_key, db_conn, request_type, db_starting_request_key, no_of_requests_to_make)

    db_conn.close()


def request_to_db(api_key, db_conn, request_type, next_request_key):

        if request_type == 'credits':
            url_for_request = f"https://api.themoviedb.org/3/movie/{next_request_key}/credits?api_key={api_key}"
        else:
            url_for_request = f"https://api.themoviedb.org/3/{request_type}/{next_request_key}?api_key={api_key}"

        print(url_for_request)
        _response_data = requests.get(url_for_request)

        if _response_data.status_code == 200:
            response_data = _response_data.json()
            print('Inserting into DB')
            db_insert_request_for_type(db_conn,
                                       request_type,
                                       next_request_key,
                                       response_data
                                       )


def process_requests_for_type(api_key, db_conn, request_type, next_request_key, no_of_requests_to_make):
    while next_request_key <= (no_of_requests_to_make + next_request_key):

        request_to_db(api_key, db_conn, request_type, next_request_key)
        if request_type == 'movie': # if request type is movie then also retrieve credits
            request_to_db(api_key, db_conn, 'credits', next_request_key)

        next_request_key += 1


def db_create_connection():
    return pg8000.connect(user="postgres", password="postgres", database="movie")


def db_create_tables(db_conn):
    sql = """
    
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

    cursor = db_conn.cursor()
    cursor.execute(sql)
    db_conn.commit()


def db_get_next_request_for_type(db_conn, request_type, starting_request_key):
    sql = """
    
          select max(request_id) 
          from   public.request 
          where  request_type = %s and 
                 request_id >= %s;
                 
         """

    cursor = db_conn.cursor()
    cursor.execute(sql, (request_type, starting_request_key))
    current_max_key = cursor.fetchone()[0]
    if current_max_key is None:
        current_max_key = 1
    return current_max_key


def db_insert_request_for_type(db_conn,
                               request_type,
                               request_key,
                               response_text):
    sql = """
     
            insert into public.request (request_type,
                                        request_id,
                                        response_text)
            select %s, %s, %s
            where not exists (select 1 from public.request where request_type = %s and
                                                                 request_id = %s)
            on conflict do nothing
    
        """

    cursor = db_conn.cursor()
    cursor.execute(sql, (request_type,
                         request_key,
                         response_text,
                         request_type,
                         request_key,))
    db_conn.commit()


if __name__ == '__main__':
    main()
