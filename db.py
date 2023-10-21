from sqlalchemy import create_engine
import os


def df_to_postgres(df, table_name):
    """ This method takes a dataframe and inserts it into a Postgres database and gives it the specified name. """
    username = os.environ.get('POSTGRES_USER')
    password = os.environ.get('POSTGRES_PASSWORD')
    conn_string = f'postgresql://{username}:{password}@localhost/nba'
    db = create_engine(conn_string)
    conn = db.connect()

    df.to_sql(table_name, con=conn, if_exists='replace', index=False)

    return

if __name__ == '__main__':
    username = os.environ.get('POSTGRES_USER')
    password = os.environ.get('POSTGRES_PASSWORD')
    print(username)


