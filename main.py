import argparse
import json
import pg8000
import requests
import sql_statements
from datetime import datetime

parser = argparse.ArgumentParser(prog='main.py',
                                 description='Extracts TMDB data into PostgreSQL database')

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
    db_create_objects(db_conn)
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

        if request_type == 'company':
            print(response_data)
            db_insert_company_record(db_conn,
                                     next_request_key,
                                     response_data['name'],
                                     response_data['homepage'],
                                     response_data['logo_path'],
                                     response_data['headquarters'],
                                     response_data['origin_country'],
                                     response_data['parent_company'],
                                     )

        if request_type == 'person':
            db_insert_person_record(db_conn,
                                    next_request_key,
                                    response_data['name'],
                                    response_data['adult'],
                                    response_data['gender'],
                                    response_data['imdb_id'],
                                    response_data['birthday'],
                                    response_data['deathday'],
                                    response_data['homepage'],
                                    response_data['biography'],
                                    response_data['popularity'],
                                    response_data['profile_path'],
                                    response_data['place_of_birth'],
                                    response_data['known_for_department'],
                                    )

        if request_type == 'movie':
            db_insert_movie_record(db_conn,
                                   next_request_key,
                                   response_data['title'],
                                   response_data['adult'],
                                   response_data['status'],
                                   response_data['imdb_id'],
                                   response_data['homepage'],
                                   response_data['poster_path'],
                                   response_data['backdrop_path'],
                                   response_data['original_title'],
                                   response_data['original_language'],
                                   response_data['tagline'],
                                   response_data['overview'],
                                   response_data['belongs_to_collection'],
                                   response_data['runtime'],
                                   response_data['release_date'],
                                   response_data['budget'],
                                   response_data['revenue'],
                                   response_data['popularity'],
                                   response_data['vote_count'],
                                   response_data['vote_average'],
                                   response_data['genres'],
                                   response_data['production_countries'],
                                   response_data['production_companies'],
                                   )

        if request_type == 'credits':

            for key, value in response_data.items():
                print(key, value)

                if key == 'cast':
                    for attribs in value:
                        person_id = attribs['id']
                        cast_order_no = attribs['order']
                        character_name = attribs['character']

                        db_insert_movie_credit_record(db_conn,
                                                      next_request_key,
                                                      person_id,
                                                      cast_order_no,
                                                      character_name)

                if key == 'crew':
                    for attribs in value:
                        person_id = attribs['id']
                        department = attribs['department']
                        job = attribs['job']

                        db_insert_movie_crew_record(db_conn,
                                                    next_request_key,
                                                    person_id,
                                                    department,
                                                    job)

        db_insert_request_for_type(db_conn,
                                   request_type,
                                   next_request_key,
                                   response_data
                                   )


def process_requests_for_type(api_key, db_conn, request_type, next_request_key, no_of_requests_to_make):
    while next_request_key <= (no_of_requests_to_make + next_request_key):

        request_to_db(api_key, db_conn, request_type, next_request_key)
        if request_type == 'movie':  # if request type is movie then also retrieve credits
            request_to_db(api_key, db_conn, 'credits', next_request_key)

        next_request_key += 1


def db_create_connection():
    return pg8000.connect(user="postgres", password="postgres", database="tmdb")


def db_create_objects(db_conn):
    cursor = db_conn.cursor()
    cursor.execute(sql_statements.create_objects_sql)
    db_conn.commit()


def db_get_next_request_for_type(db_conn, request_type, starting_request_key):
    cursor = db_conn.cursor()
    cursor.execute(sql_statements.get_last_request_for_type_sql, (request_type, starting_request_key))
    current_max_key = cursor.fetchone()[0]
    if current_max_key is None:
        current_max_key = 1
    return current_max_key


def db_insert_request_for_type(db_conn,
                               request_type,
                               request_key,
                               response_text):
    cursor = db_conn.cursor()
    cursor.execute(sql_statements.insert_new_request_record_sql, (request_type,
                                                                  request_key,
                                                                  response_text,
                                                                  request_type,
                                                                  request_key,))
    db_conn.commit()


def db_insert_movie_credit_record(db_conn,
                                  movie_id,
                                  person_id,
                                  cast_order_no,
                                  character_name):
    cursor = db_conn.cursor()

    cursor.execute(sql_statements.insert_new_movie_credit_record_sql, (movie_id,
                                                                       person_id,
                                                                       cast_order_no,
                                                                       character_name,
                                                                       movie_id,
                                                                       person_id,
                                                                       cast_order_no))

    db_conn.commit()


def db_insert_movie_crew_record(db_conn,
                                movie_id,
                                person_id,
                                department,
                                job):
    cursor = db_conn.cursor()

    cursor.execute(sql_statements.insert_new_movie_crew_record_sql, (movie_id,
                                                                     person_id,
                                                                     department,
                                                                     job,
                                                                     movie_id,
                                                                     person_id,
                                                                     department,
                                                                     job))

    db_conn.commit()


def db_insert_company_record(db_conn,
                             company_id,
                             company_name,
                             homepage,
                             logo_path,
                             headquarters,
                             origin_country,
                             parent_company
                             ):
    cursor = db_conn.cursor()

    # company_id = 100
    # company_name = 'comp name'
    # homepage = 'home'
    # logo_path = 'p'
    # headquarters = 'hq'
    # origin_country = 'oc'

    parent_company_id = None

    if parent_company:
        parent_company_id = parent_company['id']

    cursor.execute(sql_statements.insert_new_company_record_sql, (company_id,
                                                                  company_name,
                                                                  homepage,
                                                                  logo_path,
                                                                  headquarters,
                                                                  origin_country,
                                                                  parent_company_id,
                                                                  company_id))

    db_conn.commit()


def db_insert_person_record(db_conn,
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
                            ):
    cursor = db_conn.cursor()

    if birthday:
        datetime.strptime(birthday, '%Y-%m-%d')

    if deathday:
        if deathday == '03.05.2017':
            deathday = '2017-03-05'
        datetime.strptime(deathday, '%Y-%m-%d')

    gender_char = 'U'

    if gender == 1:
        gender_char = 'F'
    elif gender == 2:
        gender_char = 'M'

    cursor.execute(sql_statements.insert_new_person_record_sql, (person_id,
                                                                 name,
                                                                 adult_movie,
                                                                 gender_char,
                                                                 imdb_id,
                                                                 birthday,
                                                                 deathday,
                                                                 homepage,
                                                                 biography,
                                                                 popularity,
                                                                 profile_path,
                                                                 birth_place,
                                                                 primary_role,
                                                                 person_id))

    db_conn.commit()


def db_insert_movie_record(db_conn,
                           movie_id,
                           title,
                           adult,
                           status,
                           imdb_id,
                           homepage,
                           poster_path,
                           backdrop_path,
                           original_title,
                           original_language,
                           tagline,
                           overview,
                           belongs_to_collection,
                           runtime,
                           release_date,
                           budget,
                           revenue,
                           popularity,
                           vote_count,
                           vote_average,
                           genres,
                           production_countries,
                           production_companies
                           ):
    cursor = db_conn.cursor()

    collection_id = None

    if release_date == '':
        release_date = None

    if release_date:
        datetime.strptime(release_date, '%Y-%m-%d')

    if belongs_to_collection:
        collection_id = belongs_to_collection['id']
        collection_descr = belongs_to_collection['name']
        collection_poster_path = belongs_to_collection['poster_path']
        collection_backdrop_path = belongs_to_collection['backdrop_path']

        cursor.execute(sql_statements.insert_new_collection_record_sql, (collection_id,
                                                                         collection_descr,
                                                                         collection_poster_path,
                                                                         collection_backdrop_path,
                                                                         collection_id))

    cursor.execute(sql_statements.insert_new_movie_record_sql, (movie_id,
                                                                title,
                                                                adult,
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
                                                                vote_average,
                                                                movie_id))

    if genres:
        for genre in genres:
            genre_id = genre['id']
            genre_descr = genre['name']

            cursor.execute(sql_statements.insert_new_genre_record_sql, (genre_id,
                                                                        genre_descr,
                                                                        genre_id))

            cursor.execute(sql_statements.insert_new_movie_genre_record_sql, (movie_id,
                                                                              genre_id,
                                                                              movie_id,
                                                                              genre_id))

    if production_countries:
        for country in production_countries:
            country_code = country['iso_3166_1']
            country_descr = country['name']

            cursor.execute(sql_statements.insert_new_country_record_sql, (country_code,
                                                                          country_descr,
                                                                          country_code))

            cursor.execute(sql_statements.insert_new_movie_country_record_sql, (movie_id,
                                                                                country_code,
                                                                                movie_id,
                                                                                country_code))

    if production_companies:
        for company in production_companies:
            company_id = company['id']
            cursor.execute(sql_statements.insert_new_movie_company_record_sql, (movie_id,
                                                                                company_id,
                                                                                movie_id,
                                                                                company_id))

    db_conn.commit()


if __name__ == '__main__':
    main()
