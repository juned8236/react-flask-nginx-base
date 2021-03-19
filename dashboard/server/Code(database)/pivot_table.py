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
    result1 = []
    for row in df.index:
        order_num = df['end_customer_internal_work_order'][row]
        df_1 = df[df['end_customer_internal_work_order'] == order_num].shape[0]
        result1.append(df_1)
    return result1



def column_2(df):
    result2 = []
    df['A_original_order_lead_time_number_of_days'] = pd.to_numeric(df['A_original_order_lead_time_number_of_days'],errors='coerce')
    for row in df.index:
        order_num = df['end_customer_internal_work_order'][row]
        df_1 = df[df['end_customer_internal_work_order'] == order_num]
        df_1 = df_1['A_original_order_lead_time_number_of_days'].mean()
        result2.append(df_1)
    return result2    
    


def column_3(df):
    result3 = []
    df['A_current_req_date_vs_req_date_1_wk_ago_re_in'] = pd.to_numeric(df['A_current_req_date_vs_req_date_1_wk_ago_re_in'],errors='coerce')
    for row in df.index:
        order_num = df['end_customer_internal_work_order'][row]
        df_1 = df[df['end_customer_internal_work_order'] == order_num]
        df_1 = df_1['A_current_req_date_vs_req_date_1_wk_ago_re_in'].replace('Na', np.nan).sum()
        result3.append(df_1)
    return result3    
    


def column_4(df):
    result4 = []
    df['A_current_req_date_vs_req_date_1_wk_ago_re_out'] = pd.to_numeric(df['A_current_req_date_vs_req_date_1_wk_ago_re_out'],errors='coerce')
    for row in df.index:
        order_num = df['end_customer_internal_work_order'][row]
        df_1 = df[df['end_customer_internal_work_order'] == order_num]
        df_1 = df_1['A_current_req_date_vs_req_date_1_wk_ago_re_out'].replace('Na', np.nan).sum()
        result4.append(df_1)
    return result4



def column_5(df):
    result5 = []
    df['K_KMWE_order_lead_time_number_of_days'] = pd.to_numeric(df['K_KMWE_order_lead_time_number_of_days'],errors='coerce')
    for row in df.index:
        order_num = df['end_customer_internal_work_order'][row]
        df_1 = df[df['end_customer_internal_work_order'] == order_num]
        df_1 = df_1['K_KMWE_order_lead_time_number_of_days'].mean()
        df_1 = np.round_(df_1)
        result5.append(df_1)
    return result5    
    

def column_6(df):
    result6 = []
    df['K_KMWE_committed_delivery_date_current_vs_1_wk_ago_re_in'] = pd.to_numeric(df['K_KMWE_committed_delivery_date_current_vs_1_wk_ago_re_in'],errors='coerce')
    for row in df.index:
        order_num = df['end_customer_internal_work_order'][row]
        df_1 = df[df['end_customer_internal_work_order'] == order_num]
        df_1 = df_1['K_KMWE_committed_delivery_date_current_vs_1_wk_ago_re_in'].replace('Na', np.nan).sum()
        result6.append(df_1)
    return result6    
    


def column_7(df):
    result7 = []
    df['K_KMWE_committed_delivery_date_current_vs_1_wk_ago_re_out'] = pd.to_numeric(df['K_KMWE_committed_delivery_date_current_vs_1_wk_ago_re_out'],errors='coerce')
    for row in df.index:
        order_num = df['end_customer_internal_work_order'][row]
        df_1 = df[df['end_customer_internal_work_order'] == order_num]
        df_1 = df_1['K_KMWE_committed_delivery_date_current_vs_1_wk_ago_re_out'].replace('Na', np.nan).sum()
        result7.append(df_1)
    return result7    
    



def column_8(df):
    result8 = []
    for row in df.index:
        order_num = df['end_customer_internal_work_order'][row]
        df_1 = df[df['end_customer_internal_work_order'] == order_num]
        df_1 = df_1['order_quantity_a_new_po_is_created_if_additional_order_quantity'].mean()
        df_1 = np.round_(df_1)
        result8.append(df_1)
    return result8    
     


def column_9(df):
    result9 = []
    df['re_out_occured_but_actual_completion_date_stays_the_same'] = pd.to_numeric(df['re_out_occured_but_actual_completion_date_stays_the_same'],errors='coerce')
    for row in df.index:
        order_num = df['end_customer_internal_work_order'][row]
        df_1 = df[df['end_customer_internal_work_order'] == order_num]
        df_1 = df_1['re_out_occured_but_actual_completion_date_stays_the_same'].sum()
        df_1 = np.round_(df_1)
        result9.append(df_1)
    return result9     
    
 

def column_10(df):
    result10 = []
    df['re_out_occured_but_a_re_in_came_in_at_a_later_date'] = pd.to_numeric(df['re_out_occured_but_a_re_in_came_in_at_a_later_date'],errors='coerce')
    for row in df.index:
        order_num = df['end_customer_internal_work_order'][row]
        df_1 = df[df['end_customer_internal_work_order'] == order_num]
        df_1 = df_1['re_out_occured_but_a_re_in_came_in_at_a_later_date'].count()
        df_1 = np.round_(df_1)
        result10.append(df_1)
    return result10    
    


def column_11(df):
    result11 = []
    df['re_in_occured_but_actual_completion_date_stays_the_same'] = pd.to_numeric(df['re_in_occured_but_actual_completion_date_stays_the_same'],errors='coerce')
    for row in df.index:
        order_num = df['end_customer_internal_work_order'][row]
        df_1 = df[df['end_customer_internal_work_order'] == order_num]
        df_1 = df_1['re_in_occured_but_actual_completion_date_stays_the_same'].sum()
        df_1 = np.round_(df_1)
        result11.append(df_1)
    return result11    
    


def column_12(df):
    result12 = []
    df['re_in_occured_but_a_re_out_came_in_at_a_later_date'] = pd.to_numeric(df['re_in_occured_but_a_re_out_came_in_at_a_later_date'],errors='coerce')
    for row in df.index:
        order_num = df['end_customer_internal_work_order'][row]
        df_1 = df[df['end_customer_internal_work_order'] == order_num]
        df_1 = df_1['re_in_occured_but_a_re_out_came_in_at_a_later_date'].count()
        df_1 = np.round_(df_1)
        result12.append(df_1)
    return result12    
    


def column_13(df):
    result13 = []
    for row in df.index:
        part_num = df['unique_part_identifier'][row]
        df_1 = df[df['unique_part_identifier'] == part_num]
        a = df_1['relative_price_of_part_1_low_cost_5_high_cost'].max()
        result13.append(a)
    return result13    
    

def grouby_operations(df):
    df = df.groupby(['unique_part_identifier', 'end_customer_internal_work_order']).first().reset_index()
    df1 = df[df.columns[-13:-1]]
    df2 = df[['unique_part_identifier','FA_confirmed_work_order_PA_Planned_work_order','end_customer_internal_work_order','relative_price_of_part_1_low_cost_5_high_cost','Max_relative_price_of_part_1_low_cost_5_high_cost']]
    df_pivot = pd.concat([df2,df1], axis = 1)
    return df_pivot



def pivot_main():
    # Read data from PostgreSQL database table and load into a DataFrame instance
    df = pd.read_sql("select * from fsl_lambda_analysis", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))
    df = df[df['FA_confirmed_work_order_PA_Planned_work_order'] == 'FA']
    
    df['count_of_document'] = column_1(df)
    df['AVG_A_original_order_lead_time_number_of_days'] = column_2(df)
    df['SUM_A_current_req_date_vs_req_date_1_wk_ago_re_in'] = column_3(df) 
    df['SUM_A_current_req_date_vs_req_date_1_wk_ago_re_out'] = column_4(df)
    df['AVG_K_KMWE_order_lead_time_number_of_days'] = column_5(df) 
    df['SUM_K_KMWE_committed_delivery_date_current_vs_1_wk_ago_re_in'] = column_6(df)
    df['SUM_K_KMWE_committed_delivery_date_current_vs_1_wk_ago_re_out'] = column_7(df) 
    df['AVG_order_quantity_a_new_po_is_created_if_additional_order_qty'] = column_8(df)
    df['SUM_re_out_occured_but_actual_completion_date_stays_the_same_'] = column_9(df)
    df['COUNT_re_out_occured_but_a_re_in_came_in_at_a_later_date'] = column_10(df)
    df['SUM_re_in_occured_but_actual_completion_date_stays_the_same'] = column_11(df) 
    df['COUNT_re_in_occured_but_a_re_out_came_in_at_a_later_date'] = column_12(df)
    df['Max_relative_price_of_part_1_low_cost_5_high_cost'] = column_13(df)
    grouby_operations(df) 
 
    grouby_operations(df).to_sql("pivot_table", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'), if_exists = 'replace', chunksize = 1000, index = False) 
    print(grouby_operations(df))
    

if __name__ == "__main__":
    # pivot_main() 
    pass
