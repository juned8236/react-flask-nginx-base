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





def column_1(df):
    result1 = df['volume_of_part']
    return result1
    

def column_2(df1):
    result2 = df1['Module']
    return result2    
    


def column_3(df):
    result3 = df['relative_avg_price']
    return result3    
    


def column_4(df):
    result4 = df['avg_order_qty_per_unq_work_orders']
    return result4    
    


def column_5(df):
    result5 = df['disruption_score_using_normalized_raw_values_0_1_scale'].round(3)
    return result5    
    


def column_6(df):
    result6 = df['normalized_disruption_score_using_raw_values_0_1_excl_outliers'].round(3)
    return result6


def module_1_2_regression_main():
    # Read data from PostgreSQL database table and load into a DataFrame instance
    df = pd.read_sql("cash_vs_disruption", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))
    df1 = pd.read_sql("select * from x_ref_item_lambda_analysis", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))
    df2 = pd.read_sql("select * from module1_actual_data", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))
    df2 = df2.iloc[:, np.r_[0,-3:0]]

    module2_df = df[df.columns[:3]]
    module2_df['volume_of_part'] = column_1(df)
    module2_df['module'] = column_2(df1)
    module2_df['price'] = column_3(df)
    module2_df['avg_order_qty_per_unq_work_orders'] = column_4(df)
    
    module1_df = pd.merge(module2_df,df2,on='unique_part_identifier')
    module1_df['module'] = module1_df['module'].replace(np.nan, 'Not available')
    module1_df = module1_df.round(3)

    module2_df['normalized_disruption_score_using_raw_values_0_1_incl_outliers'] = column_5(df)
    module2_df['normalized_disruption_score_using_raw_values_0_1_excl_outliers'] = column_6(df)
    module2_df['module'] = module2_df['module'].replace(np.nan, 'Not available')
    module2_df = module2_df.round(3)

    module1_df.to_sql("module1_regression_setup", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'), if_exists = 'replace', index = False)
    module2_df.to_sql("module2_regression_setup", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'), if_exists = 'replace', index = False) 
    print(module1_df)
    print(module2_df)


    
if __name__ == "__main__":
    # module_1_2_regression_main()
    pass    