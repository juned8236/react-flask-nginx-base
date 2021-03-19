import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from configparser import ConfigParser
import warnings
warnings.filterwarnings('ignore')

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



def qc_check(df):
    
    df = df[df.columns[:31]]
    df = df[df['FA_confirmed_work_order_PA_Planned_work_order'] == 'FA'] 
    df['end_customer_internal_work_order'] = df['end_customer_internal_work_order'].astype(str)

    df1 = df[df['document_source'] == 'A']
    df1 = df1[['end_customer_internal_work_order','unique_part_identifier']]
    df1 = df1.sort_values(by=['end_customer_internal_work_order'], ascending=True)
    df1['A_Combine'] = df1['end_customer_internal_work_order'] + df1['unique_part_identifier']
    df1 = df1.reset_index()
    df1 = df1.drop('index', axis = 1)

    df2 = df[df['document_source'] == 'K']
    df2 = df2[['end_customer_internal_work_order','unique_part_identifier']]
    df2 = df2.sort_values(by=['end_customer_internal_work_order'], ascending=True)
    df2['K_Combine'] = df2['end_customer_internal_work_order'] + df2['unique_part_identifier']
    df2 = df2.reset_index()
    df2 = df2.drop('index', axis = 1)

    df_final = pd.merge(df1, df2, on='end_customer_internal_work_order', how='outer')
    df_final = df_final.drop_duplicates()
    df_final['check'] = (df_final['A_Combine'] == df_final['K_Combine'])
    
    df_final = df_final.rename(columns = {'unique_part_identifier_total_of_approx_150_parts_x':'A_unique_part_identifier_total_of_approx_150_parts',
                                          'unique_part_identifier_total_of_approx_150_parts_y':'K_unique_part_identifier_total_of_approx_150_parts'})
    
    table = df_final.to_sql("qc_a_vs_k_docs", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'), if_exists = 'replace', chunksize = 1000, index = False)
    
    return table
    


def qc_main():
    # Read data from PostgreSQL database table and load into a DataFrame instance
    df = pd.read_sql("select * from fsl_lambda_analysis", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))
    
    qc_check(df)

if __name__ == "__main__":
    # qc_main()
    pass   
