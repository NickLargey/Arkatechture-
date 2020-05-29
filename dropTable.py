import psycopg2
from config import config


def drop_table():
    # create tables in the PostgreSQL database
    command = (
        """
		DROP TABLE salesdata
			"""
    )
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            print("Table dropped")
            conn.close()


drop_table()
