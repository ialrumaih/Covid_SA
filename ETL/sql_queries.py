#Create tables queries
create_table_cases = '''
create table cases
(
    "DateTime"       date,
    "Place_EN"       varchar,
    "Place_AR"       varchar,
    "Confirmed"      integer,
    "Deaths"         integer,
    "Recovered"      integer,
    "Active"         integer,
    "Governorate_EN" varchar,
    "Governorate_AR" varchar,
    "Region_EN"      varchar,
    "Region_AR"      varchar
);
'''

create_table_tests = '''
create table tests
(
    "DateTime" date,
    "Tested"   integer,
    "Capacity" integer
);
'''

#Drop tables queires
drop_table_cases = "DROP TABLE IF EXISTS cases"
drop_table_tests = "DROP TABLE IF EXISTS tests"


# Insert queires
insert_cases_query = """
INSERT INTO cases 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
;
"""

insert_tests_query = """
INSERT INTO tests 
VALUES (%s, %s, %s)
;
"""

create_table_queries = [create_table_cases,create_table_tests ]
drop_table_queries = [drop_table_cases, drop_table_tests]