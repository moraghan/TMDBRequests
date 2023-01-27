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


def process_requests_for_type(api_key, db_conn, request_type, next_request_key, no_of_requests_to_make):
    while next_request_key <= (no_of_requests_to_make + next_request_key):
        url_for_request = f"https://api.themoviedb.org/3/{request_type}/{next_request_key}?api_key={api_key}"
        print(url_for_request)
        _response_data = requests.get(url_for_request)

        if _response_data.status_code == 200:
            response_data = _response_data.json()
            print('Inserting into DB')
            db_insert_request_for_type(db_conn,
                                       request_type,
                                       next_request_key,
                                       url_for_request,
                                       response_data
                                       )

        next_request_key += 1


def db_create_connection():
    return pg8000.connect(user="postgres", password="postgres", database="movie")


def db_create_tables(db_conn):
    sql = """
    
            CREATE TABLE if NOT EXISTS PUBLIC.request
            (
            id            serial,
            request_type  VARCHAR(20)  NOT NULL,
            request_id    INTEGER      NOT NULL,
            request_url   VARCHAR(150) NOT NULL UNIQUE,
            response_text jsonb,
            PRIMARY KEY   (request_id, request_type)
            );
        
            CREATE TABLE if NOT EXISTS PUBLIC.movie
            (
            movie_id          INTEGER NOT NULL PRIMARY KEY,
            title             VARCHAR(100),
            status            VARCHAR(20),
            imdb_id           VARCHAR(10),
            homepage          VARCHAR(150),
            poster_path       image_path,
            backdrop_path     image_path,
            original_title    VARCHAR(100),
            original_language VARCHAR(10),
            tagline           text,
            overview          text,
            collection_id     INTEGER,
            runtime           INTEGER,
            release_date      DATE,
            budget            bigint,
            revenue           bigint,
            popularity        NUMERIC(7, 2),
            vote_count        INTEGER,
            vote_average      NUMERIC(5, 2),
            profit            bigint GENERATED ALWAYS AS ((revenue - budget)) stored
            );
        
            CREATE TABLE if NOT EXISTS PUBLIC.collection
            (
            collection_id          INTEGER NOT NULL PRIMARY KEY,
            collection_descr       VARCHAR(100) UNIQUE,
            collection_poster_path image_path,
            backdrop_poster_path   image_path
            );
        
            CREATE TABLE if NOT EXISTS PUBLIC.company
            (
            id                INTEGER PRIMARY KEY,
            title             text,
            homepage          text,
            logo_path         text,
            headquarters      text,
            origin_country    CHAR(2),
            parent_company_id INTEGER
            );
        
            CREATE TABLE if NOT EXISTS PUBLIC.country
            (
            country_code  CHAR(2) NOT NULL PRIMARY KEY,
            country_descr VARCHAR(100) UNIQUE
            );

        """

    cursor = db_conn.cursor()
    cursor.execute(sql)
    db_conn.commit()


def db_get_next_request_for_type(db_conn, request_type, starting_request_key):
    sql = """
    
          SELECT MAX(request_id) 
          FROM   public.request 
          WHERE  request_type = %s AND 
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
                               request_url,
                               response_text):
    sql = """
     
            INSERT INTO public.request (request_type,
                                        request_id,
                                        request_url,
                                        response_text)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM PUBLIC.request WHERE request_type = %s AND
                                                                 request_id = %s)
            ON CONFLICT DO NOTHING
    
        """

    cursor = db_conn.cursor()
    cursor.execute(sql, (request_type,
                         request_key,
                         request_url,
                         response_text,
                         request_type,
                         request_key,))
    db_conn.commit()


if __name__ == '__main__':
    main()
