import os
import glob
import psycopg2
import pandas as pd
import io
import configparser

#link to the data source
cases_url = "https://raw.githubusercontent.com/AlrasheedA/saudi_covid19/master/data/saudi_covid19_places.csv"
tests_url = "https://raw.githubusercontent.com/AlrasheedA/saudi_covid19/master/data/saudi_covid19_tests.csv"

#Insert query to cases table
cases_query = """
INSERT INTO cases 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
;
"""

#Insert query to tests table
tests_query = """
INSERT INTO tests 
VALUES (%s, %s, %s)
;
"""

#Importing database information from config file
config = configparser.ConfigParser()
config.read('ETL/config.cfg')

# Connecting to the database
try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname={} user={} password={}".format(*config['DB'].values()))
    cur = conn.cursor() 
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)

def current_rows(table):
    '''
    This funcation return the number of records in the destination table before the ETL

    Function Parameters:
    table(string): the name of the table that we want to check

    Return: number of rows in the destination table
    '''
    current_rows_df = pd.read_sql("Select count(*) from {}".format(table), conn)
    current_rows = current_rows_df.iloc[-1]['count']
    return current_rows

# Truncating table funcation takes table name as parameter 
def trancate_tables(table):
    '''
    This function is to truncate the table in the destination before loading the data from source

    Function Parameters:
    table(string): the name of the table that we want to truncate
    '''
    try: 
        cur.execute('truncate table {}'.format(table))
        conn.commit()
    except psycopg2.Error as e: 
        print("Error: Issue truncating table {}".format(table))
        print (e)


def load_data(url,query):
    '''
    This function is to load the data from source into pandas dataframe. Then, convert the datetime to date type and load it into the database

    Function Parameters:
    url(string): The URL for the source data
    query(string): The insert query 

    Return: the number of inserted rows
    '''
    #Loading data into dataframe
    new_df = pd.read_csv(url)

    #Changing DateTime column type to date
    new_df['DateTime'] =  pd.to_datetime(new_df['DateTime'])

    #Inserting into cases table
    for i, row in new_df.iterrows():
        cur.execute(query, row)
        conn.commit()
    return(len(new_df))


def quality_check(table,source_rows):
    '''
    This function to check wheather the number of records in the destination are matching the number of records in the source

    Function Parameters:
    table(string): the name of the table that we want to check
    source_rows(int): the returned value from current_rows funcation
    '''
    #Getting number of rows in destination table and source table
    count_rows = pd.read_sql("Select count(*) from {}".format(table), conn)
    row_num = count_rows.iloc[-1]['count']

    #Calculating number of rows that have been inserted
    inserted_rows = int(row_num) - int(source_rows)

    #Check if this is the first ETL or not 
    if source_rows == 0:
        print("{} Records have been inserted into table {}".format(row_num,table))

    else:
        # Check if the number of records in destination is the same as the source
        if row_num == source_rows:
            print("Number of records are match, ETL succeed!")
            print("{} Records have been inserted into table {}".format(row_num,table))
            print("{} new records".format(inserted_rows))
            print("===============================================================")
        else:
            print("Number of records in table {} does not match, ETL failed!!!!!!!".format(table))
            print("Number of records in source: {}".format(source_rows))
            print("Number of records in table {}: {}".format(table,row_num))
            print("===============================================================")

    

def etl(table,url,query):
    '''
    This function is to run the ETL for specific data

    Function Parameters:
    table(string): the name of the table that we want to ETL the data to
    url(string): The URL for the source data
    query(string): The insert query
    '''
    current_number_rows = current_rows(table)
    trancate_tables(table)
    load_data(url,query)
    quality_check(table,current_number_rows)


# Running ETL for each table by providing the table name, source data url and insert query
etl("cases",cases_url,cases_query)
etl("tests",tests_url,tests_query)

#Closing the connection 
conn.close()

