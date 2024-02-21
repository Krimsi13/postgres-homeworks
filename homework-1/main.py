"""Скрипт для заполнения данными таблиц в БД Postgres."""
import os
import psycopg2
import csv
from config import ROOT_DIR

DATA_DIR_EMPLOYEES = os.path.join(ROOT_DIR, "homework-1", "north_data", "employees_data.csv")
DATA_DIR_CUSTOMERS = os.path.join(ROOT_DIR, "homework-1", "north_data", "customers_data.csv")
DATA_DIR_ORDERS = os.path.join(ROOT_DIR, "homework-1", "north_data", "orders_data.csv")

employees_list = []
customers_list = []
orders_list = []

with open(DATA_DIR_EMPLOYEES, newline='', encoding="windows-1251") as file:
    reader = csv.DictReader(file)
    for row in reader:
        element = (row['employee_id'],
                   row['first_name'],
                   row['last_name'],
                   row['title'],
                   row['birth_date'],
                   row['notes'])
        employees_list.append(element)

with open(DATA_DIR_CUSTOMERS, newline='', encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        element = (row['customer_id'],
                   row['company_name'],
                   row['contact_name'],)
        customers_list.append(element)

with open(DATA_DIR_ORDERS, newline='', encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        element = (row['order_id'],
                   row['customer_id'],
                   row['employee_id'],
                   row['order_date'],
                   row['ship_city'])
        orders_list.append(element)

# connect to db
conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='1a3w6q7z4')
try:
    with conn:
        # create cursor
        with conn.cursor() as cur:
            # execute query
            cur.executemany("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", employees_list)
            cur.executemany("INSERT INTO customers VALUES (%s, %s, %s)", customers_list)
            cur.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", orders_list)

finally:
    # close connection
    conn.close()
