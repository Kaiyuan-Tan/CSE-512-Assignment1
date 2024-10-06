# This is a sample Python script.

from random import random

import psycopg2
import random
from datetime import date, timedelta

DATABASE_NAME = 'assignment1'
SALES_REGION_TABLE = 'sales_region'
LONDON_TABLE = 'london'
SYDNEY_TABLE = 'sydney'
BOSTON_TABLE = 'boston'
SALES_TABLE = 'sales'
SALES_2020_TABLE = 'sales_2020'
SALES_2021_TABLE = 'sales_2021'
SALES_2022_TABLE = 'sales_2022'
REGIONS = ["Boston", "Sydney", "London"]
PRODUCT_NAMES = ["Product_A", "Product_B", "Product_C", "Product_D", "Product_E"]

PASSWORD = "12345678"

def create_database(dbname):
    """Connect to the PostgreSQL by calling connect_postgres() function
       Create a database named {DATABASE_NAME}
       Close the connection"""
    conn = connect_potsgres("postgres")
    cursor = conn.cursor()
    cursor.execute(f"create database {dbname};")
    print("...Database has been created...")
    cursor.close()
    conn.close()

def connect_potsgres(dbname):
    """Connect to the PostgreSQL using psycopg2 with default database
       Return the connection"""
    conn = psycopg2.connect(dbname=dbname, user="postgres", host="localhost", password=PASSWORD)
    conn.autocommit = True
    print(f"...Connected to {dbname}...")
    return conn

def list_partitioning(conn):
    """Function to create partitions of {SALES_REGION_TABLE} based on list of REGIONS.
       Create {SALES_REGION_TABLE} table and its list partition tables {LONDON_TABLE}, {SYDNEY_TABLE}, {BOSTON_TABLE}
       Commit the changes to the database"""
    create_table = f""" create table {SALES_REGION_TABLE}(
                            id int,
                            amount int,
                            region text
                        )partition by list (region);"""
    cursor = conn.cursor()
    cursor.execute("begin;")
    cursor.execute(create_table)
    for region in REGIONS:
        create_partition_table = f"""create table {region.lower()} partition of {SALES_REGION_TABLE} for values in('{region}');"""
        cursor.execute(create_partition_table)
    conn.commit()
    cursor.close()
    print("...List partition completed...")

def insert_list_data(conn):
    """ Generate 50 rows data for {SALES_REGION_TABLE}
        Execute INSERT statement to add data to the {SALES_REGION_TABLE} table.
        Commit the changes to the database"""
    cursor = conn.cursor()
    cursor.execute("begin;")
    for i in range(50):
        sql = f"""insert into {SALES_REGION_TABLE}(id, amount, region) values ({i+1}, {random.randint(100, 1000)}, '{random.choice(REGIONS)}')"""
        cursor.execute(sql)
    conn.commit()
    cursor.close()
    print("...Insertion completed...")

def select_list_data(conn):
    """Select data from {SALES_REGION_TABLE}, {BOSTON_TABLE}, {LONDON_TABLE}, {SYDNEY_TABLE} seperately.
       Print each tables' data.
       Commit the changes to the database
    """
    cursor = conn.cursor()
    cursor.execute("begin;")
    cursor.execute(f"""select * from {SALES_REGION_TABLE} order by id""")
    conn.commit()
    results = cursor.fetchall()
    print("Sales Region Data:")
    for result in results:
        print(result)
    # Tables = [ LONDON_TABLE, SYDNEY_TABLE, BOSTON_TABLE ]
    for region in REGIONS:
        cursor.execute("begin;")
        cursor.execute(f"""select * from {region.lower()} order by id""")
        conn.commit()
        print(f"{region} Data:")
        results = cursor.fetchall()
        for result in results:
            print(result)

    cursor.close()
    print("...Selection completed...")

def range_partitioning(conn):
    """Function to create partitions of {SALES_TABLE} based on range of sale_date.
       Create {SALES_REGION_TABLE} table and its range partition tables {SALES_2020_TABLE}, {SALES_2021_TABLE}, {SALES_2022_TABLE}
       Commit the changes to the database
    """
    create_table = f""" create table {SALES_TABLE}(
                            id int,
                            product_name text,
                            amount int,
                            sale_date date
                        )partition by range (sale_date);"""
    cursor = conn.cursor()
    cursor.execute("begin;")
    cursor.execute(create_table)
    cursor.execute(f"""create table {SALES_2020_TABLE} partition of sales for values from ('2020-01-01') to ('2021-01-01');""")
    cursor.execute(f"""create table {SALES_2021_TABLE} partition of sales for values from ('2021-01-01') to ('2022-01-01');""")
    cursor.execute(f"""create table {SALES_2022_TABLE} partition of sales for values from ('2022-01-01') to ('2023-01-01');""")
    conn.commit()
    cursor.close()
    print("...Range partition completed...")

def insert_range_data(conn):
    """ Generate 50 rows data for {SALES_REGION_TABLE}
        Execute INSERT statement to add data to the {SALES_REGION_TABLE} table.
        Commit the changes to the database"""
    cursor = conn.cursor()
    cursor.execute("begin;")
    for i in range(50):
        start_date = date(2020, 1, 1)
        end_date = date(2022, 12, 31)
        date_diff = random.randint(0, (end_date - start_date).days)
        sale_date = start_date + timedelta(days=date_diff)
        sql = f"""insert into {SALES_TABLE}(id, product_name, amount, sale_date) values ({i+1}, '{random.choice(PRODUCT_NAMES)}', {random.randint(1, 100)}, '{sale_date}')"""
        cursor.execute(sql)
    conn.commit()
    cursor.close()
    print("...Insertion completed...")

def select_range_data(conn):
    """Select data from {SALES_TABLE}, {SALES_2020_TABLE}, {SALES_2021_TABLE}, {SALES_2022_TABLE} seperately.
           Print each tables' data.
           Commit the changes to the database
        """
    cursor = conn.cursor()
    cursor.execute(f"""select * from {SALES_TABLE} order by id""")
    results = cursor.fetchall()
    print("Sales Data:")
    for result in results:
        print(result)

    cursor.execute(f"""select * from {SALES_2020_TABLE} order by id""")
    print("Sales 2020 Data:")
    results = cursor.fetchall()
    for result in results:
        print(result)

    cursor.execute(f"""select * from {SALES_2021_TABLE} order by id""")
    print("Sales 2021 Data:")
    results = cursor.fetchall()
    for result in results:
        print(result)

    cursor.execute(f"""select * from {SALES_2022_TABLE} order by id""")
    print("Sales 2022 Data:")
    results = cursor.fetchall()
    for result in results:
        print(result)

    cursor.close()
    print("...Selection completed...")    



# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    create_database(DATABASE_NAME)

    with connect_potsgres(dbname=DATABASE_NAME) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        list_partitioning(conn)
        insert_list_data(conn)
        select_list_data(conn)

        range_partitioning(conn)
        insert_range_data(conn)
        select_range_data(conn)

        print('Done')




