def create_table():
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
