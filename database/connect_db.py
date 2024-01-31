import psycopg2
from database.db_config import DB_NAME, USER, PASSWORD, HOST, PORT

def send_query(query, values = None): 
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )

    response = None

    with conn.cursor() as curr:
        try:
            if values:
                curr.execute(query, values)
                try:
                    response = curr.fetchall()
                except Exception as e:
                    pass
            else:
                curr.execute(query)
                response = curr.fetchall()
            conn.commit()
        except Exception as e:
            conn.rollback()
    conn.close()
    return response