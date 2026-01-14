import psycopg2
from utils import get_db_url


def test_postgress_connection(connection_string):
    try:
        conn = psycopg2.connect(connection_string)

        cur = conn.cursor()

        #cur.execute("SELECT version();")
        sql_statement = """
            SELECT * FROM ORDERDETAIL;
        """
        cur.execute(sql_statement)

        db_version = cur.fetchall()
        print("Connection successful!")
        print(f"PostgresSQL version: {db_version}")

        cur.close()
        conn.close()
        print("Connection closed")
        return True
    except Exception as e:
        print("Connection failed")
        print(e)
        return False

DATABASE_URL = get_db_url()

test_postgress_connection(DATABASE_URL)