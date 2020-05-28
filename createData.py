import pandas as pd
import psycopg2
from config import config


df = pd.read_csv("./salesdata.csv")

df["date"] = pd.to_datetime(df.date)
df.sort_values("date", inplace=True, ascending=False)
df.dropna(inplace=True)

print(df)

def create_table():
	""" create tables in the PostgreSQL database"""
	command = (
		"""
		CREATE TABLE IF NOT EXISTS salesdata (
			ID serial PRIMARY KEY,
			sales_person VARCHAR(50),
			sales_department VARCHAR(50),
			amount decimal
      quantity smallint
      street VARCHAR(100)
      city VARCHAR(50)
      state VARCHAR(25)
      item_id VARCHAR(50)
      date DATE)
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
			print("Table set")
			conn.close()
 
def create_data():
	create_table()
	
	try:
		# connect to the PostgreSQL server
		conn = psycopg2.connect(**params)
		cur = conn.cursor()

		# create the dates column as the primary key

		setData= "INSERT INTO   (date_of_show) VALUES (%s) ON CONFLICT (date_of_show) DO NOTHING;"

		# delete dates that have gone by
		cur.execute(sql.SQL("DELETE FROM {} WHERE date_of_show<%s").format(sql.Identifier('portland_shows')), [base_])

		# for obj in zip(date_list):
		# 	cur.execute(cur.mogrify(sql_time, obj))

		# # loop through returned dicts from Scraper and match key:value pairs in postgres
		# for k,v in df.items():
		# 	cur.execute('''UPDATE salesdata
		# 					   SET apohadion = (%s) 
		# 					 WHERE date_of_show = (%s);''',(v,k))

		# for k,v in sun_tiki.items():
		# 	cur.execute('''UPDATE portland_shows 
		# 					   SET sun_tiki = (%s) 
		# 					 WHERE date_of_show = (%s);''',(v,k))

		# for k,v in genos.items():
		# 	cur.execute('''UPDATE portland_shows 
		# 					   SET genos = (%s) 
		# 					 WHERE date_of_show = (%s);''',(v,k))

		conn.commit()

		cur.close
		conn.close()
	except Exception as e:
		print(e)