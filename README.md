# Covid_SA

 This repo is ETL script to load data from this repo https://github.com/AlrasheedA/saudi_covid19 to a local PostgreSQL database. The data are Covid cases and tests in Saudi Arabia. I am only focusing on two tables, cases and tests.
 
 ## Requirements:
 * PostgreSQL database
 * Python 3.* 
 * psycopg2 library
 * pandas library
 
 ## How to run 
 * Fill config.cfg file with database information
 * Run create_tables.py to create tables in the database (for first run only)
 * Run ETL_PostgreSQL.py to load the data into PostgreSQL database
 
 ## Future work
 * Using cloud database (For example, AWS Redshift)
 * Do some analysis and dashboards 
 * Automate the ETL using Airflow or other ETL orchestration 
