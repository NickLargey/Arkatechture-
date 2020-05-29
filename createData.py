import pandas as pd
import psycopg2

from psycopg2 import sql
from config import config


def create_data():
    # Sales person with highest sales
    highest_sales_by_person = sql.SQL("""
      SELECT sales_person, SUM(amount) as sum_amount FROM salesdata GROUP BY sales_person ORDER BY sum_amount DESC LIMIT 1
    """)

    # Sales department with highest sales in 2017
    highest_sales_by_dept = sql.SQL("""
      SELECT sales_department, SUM(amount) as sum_amount FROM salesdata WHERE EXTRACT(YEAR from date)=2017 GROUP BY sales_department ORDER BY sum_amount DESC LIMIT 1
    """)

    # Average sales per month for all employees
    avg_sales_by_month = sql.SQL(""" 
      SELECT EXTRACT(MONTH from date) as mnth, SUM(amount) as sum_amount FROM salesdata GROUP BY mnth ORDER BY mnth DESC
    """)

    # Total sales amount for each sales person in descending order
    sales_by_person = sql.SQL("""
      SELECT sales_person, SUM(amount) as sum_amount FROM salesdata GROUP BY sales_person ORDER BY sum_amount DESC 
    """)
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        cur.execute(highest_sales_by_person)
        person = cur.fetchall()
        cur.execute(highest_sales_by_dept)
        dept = cur.fetchall()
        cur.execute(avg_sales_by_month)
        month = cur.fetchall()
        cur.execute(sales_by_person)
        avg = cur.fetchall()

        print("Question 1: ")
        for p in person:
            print(p)
        print("=========================================")

        print("Question 2: ")
        for d in dept:
            print(d)
        print("=========================================")

        print("Question 3: ")
        for m in month:
            print(m)
        print("=========================================")

        print("Question 4: ")
        for a in avg:
            print(a)
        print("=========================================")

        conn.commit()

        cur.close
        conn.close()
    except Exception as e:
        print(e)


create_data()
