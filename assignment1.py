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
    conn = psycopg2.connect(dbname=dbname, user="postgres", password="12345678")
    conn.autocommit = True
    print(f"...Connected to {dbname}...")
    return conn


def list_partitioning(conn):
    """Function to create partitions of {SALES_REGION_TABLE} based on list of REGIONS.
       Create {SALES_REGION_TABLE} table and its list partition tables {LONDON_TABLE}, {SYDNEY_TABLE}, {BOSTON_TABLE}
       Commit the changes to the database"""

def insert_list_data(conn):
    """ Generate 50 rows data for {SALES_REGION_TABLE}
        Execute INSERT statement to add data to the {SALES_REGION_TABLE} table.
        Commit the changes to the database"""


def select_list_data(conn):
    """Select data from {SALES_REGION_TABLE}, {BOSTON_TABLE}, {LONDON_TABLE}, {SYDNEY_TABLE} seperately.
       Print each tables' data.
       Commit the changes to the database
    """

def range_partitioning(conn):
    """Function to create partitions of {SALES_TABLE} based on range of sale_date.
       Create {SALES_REGION_TABLE} table and its range partition tables {SALES_2020_TABLE}, {SALES_2021_TABLE}, {SALES_2022_TABLE}
       Commit the changes to the database
    """

def insert_range_data(conn):
    """ Generate 50 rows data for {SALES_REGION_TABLE}
        Execute INSERT statement to add data to the {SALES_REGION_TABLE} table.
        Commit the changes to the database"""


def select_range_data(conn):
    """Select data from {SALES_TABLE}, {SALES_2020_TABLE}, {SALES_2021_TABLE}, {SALES_2022_TABLE} seperately.
           Print each tables' data.
           Commit the changes to the database
        """

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # create_database(DATABASE_NAME)

    with connect_potsgres(dbname=DATABASE_NAME) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        list_partitioning(conn)
        insert_list_data(conn)
        select_list_data(conn)

        range_partitioning(conn)
        insert_range_data(conn)
        select_range_data(conn)

        print('Done')




