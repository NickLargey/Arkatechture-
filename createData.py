import pandas as pd
import psycopg2

from psycopg2 import sql
from StringIO import StringIO

from config import config


df = pd.read_csv("./salesdata.csv")

df["date"] = pd.to_datetime(df.date)
df.sort_values("date", inplace=True, ascending=False)

df.amount.replace(r"\$", '', regex=True, inplace=True)
df.dropna(inplace=True)

buf = StringIO()
df.to_csv(buf, header=False, index=False)
buf.pos = 0


print(df)


def create_table():
    # create tables in the PostgreSQL database
    command = (
        """
		CREATE TABLE IF NOT EXISTS salesdata (
			sales_person VARCHAR(50),
			sales_department VARCHAR(50),
			amount decimal,
      quantity smallint,
      street VARCHAR(100),
      city VARCHAR(50),
      state VARCHAR(25),
      item_id VARCHAR(50),
      date DATE)
			"""
    )
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(command)
        cur.copy_from(buf, 'salesdata', sep=',')
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            print("Table set")
            conn.close()


def create_data():

    # create_table()

    highest_sales_by_person = sql.SQL("""
      SELECT sales_person, SUM(amount) as sum_amount FROM salesdata GROUP BY sales_person ORDER BY sum_amount DESC
    """)

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        cur.execute(highest_sales_by_person)
        test = cur.fetchall()

        for row in test:
            print(row)
        conn.commit()

        cur.close
        conn.close()
    except Exception as e:
        print(e)


create_data()
