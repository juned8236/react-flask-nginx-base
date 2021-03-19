from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import  os
import psycopg2
import sys


host = os.getenv("HOST")
database = os.getenv("DATABASE")
user = os.getenv("ADMIN")
password = os.getenv("PASSWORD")


def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occured
    line_n = traceback.tb_lineno

    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)

    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)

    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    param = {
        "host": host,
        "database": database,
        "user": user,
        "password": password
    }
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**param)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection accessful")
    return conn

