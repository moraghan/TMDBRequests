import pg8000
import requests
import os

from snaql.factory import Snaql

root_location = os.path.abspath(os.path.dirname(__file__))
snaql_factory = Snaql(root_location, 'sql')
create_tables = snaql_factory.load_queries('create_tables.snaql')
request_dml = snaql_factory.load_queries('request_dml.snaql')


def main():
    api_key = "dd764c65e8685d30f05dddbe0f2f9e04"
    request_type = 'movie'

    db_conn = db_create_connection()
    db_create_tables(db_conn)
    next_request_key = db_get_next_request_for_type(db_conn, request_type)
    print(f'starting at request key {next_request_key}')

    while next_request_key <= 200000:
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

        next_request_key = next_request_key + 1

    db_conn.close()

def db_create_connection():
    return pg8000.connect(user="postgres", password="postgres", database="movie")


def db_create_tables(db_conn):
    create_tables_sql = create_tables.create_tables()
    cursor = db_conn.cursor()
    cursor.execute(create_tables_sql)
    db_conn.commit()


def db_get_next_request_for_type(db_conn, request_type):
    get_max_query_sql = request_dml.get_max_id_for_request_type()
    cursor = db_conn.cursor()
    cursor.execute(get_max_query_sql, (request_type,))
    current_max_key = cursor.fetchone()[0]
    if current_max_key is None:
        current_max_key = 1
    return current_max_key


def db_insert_request_for_type(db_conn,
                               request_type,
                               request_key,
                               request_url,
                               response_text):
    insert_request_sql = request_dml.insert_new_request_record()
    cursor = db_conn.cursor()
    cursor.execute(insert_request_sql, (request_type,
                                        request_key,
                                        request_url,
                                        response_text,
                                        request_type,
                                        request_key,))
    db_conn.commit()


if __name__ == '__main__':
    main()