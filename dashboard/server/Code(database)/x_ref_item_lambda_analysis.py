import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from configparser import ConfigParser
import warnings
warnings.filterwarnings('ignore')
from utils import *


def config(filename, section):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    #establishing connection with kmwe database
    postgre_conn="postgresql://{0}:{1}@{2}:{3}/{4}".format(db.get('user'),db.get('password'),db.get('host'),db.get('port'),db.get('database'))
    engine = create_engine(postgre_conn)
    return engine


#1
def column_1(df2):
    result1 = df2['WC1'].str[:2]
    return result1


#2
def column_2(df2):
    result2 = []
    for row, value in df2['WC_short'].items():    
        if value == '11':    
            result2.append('automatic')    
        elif value == '12':    
            result2.append('automatic')    
        else:    
            result2.append('manual')
    return result2



#3
def column_3(df2):
    result3 = []
    for row, value in df2['WC_type'].items():
        if value == 'automatic':
            result3.append(2)
        elif value == 'manual':
            result3.append(1)
    return result3


def xref_main():
    # Read data from PostgreSQL database table and load into a DataFrame instance
    df1 = pd.read_sql("select * from fsl_lambda", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))
    df1 = df1[df1['FA_confirmed_work_order_PA_Planned_work_order'] == 'FA']
    df1 = df1[['unique_part_identifier', 'end_customer_internal_work_order']]
    df1 = df1.groupby(['unique_part_identifier']).apply(lambda x: list(np.unique(x))).reset_index()
    df1 = df1.rename(columns = {0 : 'related_order_numbers'})
    

    df2 = pd.read_sql("select * from x_ref_item_lambda", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))
    df2['WC_short'] = column_1(df2)
    df2['WC_type'] = column_2(df2)
    df2['WC_disruption'] = column_3(df2)

    df_final = pd.merge(df2, df1, on='unique_part_identifier', how='outer')
    df_final = df_final.dropna(subset=['related_order_numbers'])
    df_final = df_final.drop('related_order_numbers', axis=1)
    df_final.to_sql("x_ref_item_lambda_analysis",config('D:\projects\KMWE_product_planning\database1.ini','postgresql'), if_exists = 'replace', index = False) 
    #conn.close()
    print(df_final)
    


if __name__ == "__main__":
    # xref_main_main
    pass
