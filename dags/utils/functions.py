import requests
import psycopg2
from psycopg2.extras import execute_values


# Retrieve data from API
def fetch_university_data():
    url = 'http://universities.hipolabs.com/search'
    response = requests.get(url)
    data = response.json()
    return data


# Data processing to differ by type
def process_university_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='fetch_university_data')

    result = []
    for entry in data:
        if 'College' in entry['name']:
            entry['type'] = 'College'
        elif 'University' in entry['name']:
            entry['type'] = 'University'
        elif 'Institute' in entry['name']:
            entry['type'] = 'Institute'
        else:
            entry['type'] = None

        result.append((
            entry['name'],
            entry['country'],
            entry['alpha_two_code'],
            entry.get('state-province'),
            entry.get('type'),
        ))

    # Transfer data via XCom
    kwargs['ti'].xcom_push(key='data', value=result)


# Connect to PostgreSQL and data upload
def load_to_postgres(**kwargs):
    # Accept data via XCom
    data = kwargs['ti'].xcom_pull(task_ids='process_university_data', key='data')

    try:
        # Connect to the PostgreSQL database
        with psycopg2.connect(
            dbname=kwargs['postgres_db'],
            user=kwargs['postgres_user'],
            password=kwargs['postgres_password'],
            host=kwargs['postgres_host'],
            port=kwargs['postgres_port'],
        ) as conn:
            # Create cursor object
            with conn.cursor() as cur:
                # Define insert query
                insert_query = '''
                        INSERT INTO university_data (name, country, alpha_two_code, state_province, type)
                        VALUES %s
                        ON CONFLICT (name) DO NOTHING;
                    '''
                # Execute the query with all records
                execute_values(cur, insert_query, data)
    except psycopg2.DatabaseError as error:
        print(error)
