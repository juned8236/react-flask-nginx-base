import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from configparser import ConfigParser
import argparse, sys, os
import utils
from databaseConnection import connect
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import  os
import psycopg2
import sys
def config():
    #establishing connection with kmwe database
    postgre_conn="postgresql://{0}:{1}@{2}:{3}/{4}".format(os.getenv("ADMIN"),os.getenv("PASSWORD"),os.getenv("HOST"),os.getenv("PORT"),os.getenv("DATABASE"))
    engine = create_engine(postgre_conn)
    return engine


def read_from_excel(path_to_excel):
    excel_file = "/home/juned8236/Desktop/Leesta/MtStock/dashboard/server/Code(database)/LF_ProdPlan_Input data & initial transformations_Sep2020.xlsx"
    #reading sheet1 of excel file
    df = pd.read_excel(excel_file, sheet_name='FSL_Lambda', skiprows = [0,2])
    df = df[df.columns[:30]]
    print(df)
    df = df.rename(columns=utils.fsl_columns)
    print(df.columns)
    #reading sheet2 of excel file
    df1 = pd.read_excel(excel_file, sheet_name='X-REF_ITEM_LAMBDA',skiprows = [0])
    df1 = df1[df1.columns[:7]]
    df1 = df1.rename(columns=utils.xref_columns)
    print(df1.columns)
    #establishing connection with newly created database
    #importing data into database table
    df.to_sql("fsl_lambda", config(), if_exists = 'replace', chunksize = 1000, index = False)
    df1.to_sql("x_ref_item_lambda",  config(), if_exists = 'replace', index = False)
    print("Data imported successfully.......")


def connection_with_db():
    #establishing the connection with default database
    conn = connect()
    #Preparing query to create a database
    #conn.execute("DROP DATABASE IF EXISTS kmwedb")
    conn.execution_options(isolation_level="AUTOCOMMIT").execute("DROP DATABASE IF EXISTS kmwedb")
    conn.execution_options(isolation_level="AUTOCOMMIT").execute("CREATE DATABASE kmwedb WITH ENCODING 'utf8'")
    #conn.execute("CREATE DATABASE kmwedb WITH ENCODING 'utf8'")
    print("Database created successfully........")
    #Closing the connection
    conn.close()

if __name__ == "__main__":
    # pass
    # parser = argparse.ArgumentParser()
    # print(parser)
    # parser.add_argument('excel_args', nargs='*', help='Enter excel path')
    # args = parser.parse_args()
    # print(args)
    # excel_args = args.excel_args
    # def invalid_usage(msg):
    #     sys.stderr.write('ERROR: %s%s' % (msg, os.linesep))
    #     parser.print_usage(sys.stderr)
    #     sys.exit(1)

    # # Ensure enough args are specified
    # if len(excel_args) != 1:
    #     invalid_usage('Insufficient args. Please supply exactly 1.')
    # path_to_excel = excel_args[0]
    read_from_excel('g')
    