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



def column_1(df_pivot):
    result1 = []
    for row in df_pivot.index:
        part_num = df_pivot['unique_part_identifier'][row]
        df_1 = df_pivot[df_pivot['unique_part_identifier'] == part_num]
        a = df_1['relative_price_of_part_1_low_cost_5_high_cost'].mean() 
        a = round(a,1)
        result1.append(a)
    return result1

def column_2(df_pivot):
    result2 = []
    for row in df_pivot.index:
        part_num = df_pivot['unique_part_identifier'][row]
        df_1 = df_pivot[df_pivot['unique_part_identifier'] == part_num]
        a = df_1['AVG_order_quantity_a_new_po_is_created_if_additional_order_qty'].sum() 
        result2.append(a)
    return result2    

def column_3(df_pivot):
    result3 = []
    for row in df_pivot.index:
        part_num = df_pivot['unique_part_identifier'][row]
        df_1 = df_pivot[df_pivot['unique_part_identifier'] == part_num]
        a = df_1['end_customer_internal_work_order'].count() 
        result3.append(a)
    return result3

def column_4(df_pivot):
    result4 = []
    for row in df_pivot.index:
        part_num = df_pivot['unique_part_identifier'][row]
        df_1 = df_pivot[df_pivot['unique_part_identifier'] == part_num]
        a = df_1['SUM_A_current_req_date_vs_req_date_1_wk_ago_re_in'].sum() 
        result4.append(a)
    return result4

def column_5(df_pivot):
    result5 = []
    for row in df_pivot.index:
        part_num = df_pivot['unique_part_identifier'][row]
        df_1 = df_pivot[df_pivot['unique_part_identifier'] == part_num]
        a = df_1['SUM_A_current_req_date_vs_req_date_1_wk_ago_re_in'] > 0
        a = a.sum()
        result5.append(a) 
    return result5

def column_6(df_pivot):
    result6 = df_pivot['total_re_ins_in_6_month_period_across_work_orders']  / df_pivot['number_of_unique_work_orders_excl_PA']
    result6 = result6 * 100
    result6 = round(result6)
    return result6

def column_7(df_pivot):
    result7 = df_pivot['total_unq_FA_work_orders_with_re_ins']  / df_pivot['number_of_unique_work_orders_excl_PA']
    result7 = result7 * 100
    result7 = round(result7)
    return result7

def column_8(df_pivot):
    result8 = pd.cut(x = df_pivot['prob_re_in_occur_bsd_on_total_unq_FA_work_orders_with_re_ins'], bins=[0, 1, 25, 50, 75, 101], labels=['5. No re-ins', '4. Low(below 25%)', '3. Low-Medium(25-49%)', '2. Medium-High(50-74%)', '1. High(75%+)'], right=False)
    return result8

def column_9(df_pivot):
    result9 = []
    for row in df_pivot.index:
        part_num = df_pivot['unique_part_identifier'][row]
        df_1 = df_pivot[df_pivot['unique_part_identifier'] == part_num]
        a = df_1['SUM_re_in_occured_but_actual_completion_date_stays_the_same'].sum()
        result9.append(a) 
    return result9  

def column_10(df_pivot):
    result10 = []
    for row, value in  df_pivot['total_re_ins_in_6_month_period_across_work_orders'].items():
        if value != 0:
            df_1 = df_pivot['number_times_completion_date_stayed_same_when_re_in_occur_Q2a'][row]  / df_pivot['total_re_ins_in_6_month_period_across_work_orders'][row]
            df_1 = df_1 * 100
            a = round(df_1)
            result10.append(a)
        else:
            result10.append('No re-ins')
    return result10 

def column_11(df_pivot):
    result11 = []
    for row, value in df_pivot['prob_re_in_not_met_Q2a'].items():
        if value == 0:
            result11.append('5. Completion date adjusted')
        elif value in range(1,25):
            result11.append('4. Low(below 25%)')
        elif value in range(25,50):
            result11.append('3. Low-Medium(25-49%)')
        elif value in range(50,74):
            result11.append('2. Medium-High(50-74%)')
        elif value in range(75,1000):
            result11.append('1. High(75%+)')
        else:
            result11.append('6. No re-ins')
    return result11 

def column_12(df_pivot):
    result12 = []
    for row in df_pivot.index:
        part_num = df_pivot['unique_part_identifier'][row]
        df_1 = df_pivot[df_pivot['unique_part_identifier'] == part_num]
        a = df_1['COUNT_re_in_occured_but_a_re_out_came_in_at_a_later_date'] > 0
        a = a.sum()
        result12.append(a)
    return result12 


def column_13(df_pivot):
    result13 = []
    for row, value in  df_pivot['total_unq_FA_work_orders_with_re_ins'].items():
        if value != 0:
            df_1 = df_pivot['total_unq_FA_work_orders_with_re_ins_had_a_subsequent_re_out_Q2bc'][row]  / df_pivot['total_unq_FA_work_orders_with_re_ins'][row]
            df_1 = df_1 * 100
            a = round(df_1)
            result13.append(a)
        else:
            result13.append('No re-ins')
    return result13   

def column_14(df_pivot):
    result14 = []
    for row, value in df_pivot['prob_re_in_work_orders_with_subsequent_re_outs_Q2bc'].items():
        if value == 0:
            result14.append('5. No Subsequent re-out')
        elif value in range(1,25):
            result14.append('4. Low(below 25%)')
        elif value in range(25,50):
            result14.append('3. Low-Medium(25-49%)')
        elif value in range(50,74):
            result14.append('2. Medium-High(50-74%)')
        elif value in range(75,1000):
            result14.append('1. High(75%+)')
        else:
            result14.append('6. No re-ins')
    return result14

def gb_operations(df_x_ref, df_pivot):
    df_pivot = df_pivot.groupby(['unique_part_identifier']).first().reset_index()
    df_pivot_new = df_pivot.iloc[:, np.r_[0,17:31]]
    df_final = pd.merge(df_x_ref, df_pivot_new, on='unique_part_identifier', how='outer')
    df_final = df_final.dropna()
    return df_final


def q2_main():
    # Read data from PostgreSQL database table and load into a DataFrame instance
    df_x_ref = pd.read_sql("select * from x_ref_item_lambda", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))
    df_x_ref = df_x_ref[df_x_ref.columns[:3]]
    df_pivot = pd.read_sql("select * from pivot_table", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))

    df_pivot['relative_avg_price_of_part_1_low_cost_5_high_cost'] = column_1(df_pivot)
    df_pivot['total_order_qty_across_work_orders']  = column_2(df_pivot)
    df_pivot['number_of_unique_work_orders_excl_PA']  = column_3(df_pivot)
    df_pivot['total_re_ins_in_6_month_period_across_work_orders'] = column_4(df_pivot)
    df_pivot['total_unq_FA_work_orders_with_re_ins']  = column_5(df_pivot)
    df_pivot['prob_re_in_occur_bsd_on_total_re_ins_across_work_orders'] = column_6(df_pivot)
    df_pivot['prob_re_in_occur_bsd_on_total_unq_FA_work_orders_with_re_ins'] = column_7(df_pivot)
    df_pivot['prob_re_in_occur_bsd_on_total_unq_FA_work_orders_with_re_ins_bins'] = column_8(df_pivot)
    df_pivot['number_times_completion_date_stayed_same_when_re_in_occur_Q2a']  = column_9(df_pivot)
    df_pivot['prob_re_in_not_met_Q2a'] = column_10(df_pivot)
    df_pivot['prob_re_in_not_met_Q2a_bins'] = column_11(df_pivot)
    df_pivot['total_unq_FA_work_orders_with_re_ins_had_a_subsequent_re_out_Q2bc']  = column_12(df_pivot)
    df_pivot['prob_re_in_work_orders_with_subsequent_re_outs_Q2bc'] = column_13(df_pivot)
    df_pivot['prob_re_in_work_orders_with_subsequent_re_outs_Q2bc_bins'] = column_14(df_pivot)
    gb_operations(df_x_ref, df_pivot)

    gb_operations(df_x_ref, df_pivot).to_sql("q2abc_table", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'), if_exists = 'replace', chunksize = 1000, index = False) 
    print(gb_operations(df_x_ref, df_pivot))


    
if __name__ == "__main__":
    # q2_main()
    pass                                                                 