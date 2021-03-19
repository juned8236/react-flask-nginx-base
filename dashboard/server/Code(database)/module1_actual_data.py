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





def column_1(module1_df, df):
    result1 = []
    for i in module1_df.index:
        part_num = module1_df['unique_part_identifier'][i]
        data = df[(df['unique_part_identifier'] == part_num) & (df['FA_confirmed_work_order_PA_Planned_work_order'] == 'FA') & (df['document_source'] == 'A')].shape[0]
        result1.append(data)
    return result1
    

def column_2(df1):
    result2 = df1['total_re_outs_in_6_month_period_across_work_orders']
    return result2    
    


def column_3(df2):
    result3 = df2['total_re_ins_in_6_month_period_across_work_orders']
    return result3    
    


def column_4(module1_df):
    result4 = module1_df['total_number_of_re_outs_across_work_orders'] + module1_df['total_number_of_re_outs_across_work_orders']
    return result4    
    


def column_5(module1_df):
    result5 = (module1_df['total_number_of_re_outs_across_work_orders'] / module1_df['number_of_forecast_received_from_end_customer']) * 100
    return result5    
    


def column_6(module1_df):
    result6 = (module1_df['total_number_of_re_ins_across_work_orders'] / module1_df['number_of_forecast_received_from_end_customer']) * 100
    return result6



def column_7(df_pivot):
    result7 = (module1_df['total_number_of_re_ins_or_re_outs_across_work_orders'] / module1_df['number_of_forecast_received_from_end_customer']) * 100
    return result7




def module1_main():
    # Read data from PostgreSQL database table and load into a DataFrame instance
    df = pd.read_sql("fsl_lambda_analysis", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))
    df1 = pd.read_sql("select * from q1abc_table", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))
    df2 = pd.read_sql("select * from q2abc_table", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))

    module1_df = df1[df1.columns[:5]]
    module1_df['number_of_forecast_received_from_end_customer'] = column_1(module1_df, df)
    module1_df['total_number_of_re_outs_across_work_orders'] = column_2(df1)
    module1_df['total_number_of_re_ins_across_work_orders'] = column_3(df2)
    module1_df['total_number_of_re_ins_or_re_outs_across_work_orders'] = column_4(module1_df)
    module1_df['prob_re_out_occur_bsd_on_total_re_outs_and_forecasts_received'] = column_5(module1_df)
    module1_df['prob_re_in_occur_bsd_on_total_re_ins_and_forecasts_received'] = column_6(module1_df)
    module1_df['prob_re_out_or_in_occur_bsd_on_total_re_outs_or_ins_nd_fcst_rec'] = column_6(module1_df)
    module1_df = module1_df.round(3)
    
    module1_df.to_sql("module1_actual_data", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'), if_exists = 'replace', index = False) 
    print(module1_df)


    
if __name__ == "__main__":
    # module1_main()
    pass    