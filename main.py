import argparse

import pg8000
import requests
import sql_statements

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


if __name__ == '__main__':
    main()
