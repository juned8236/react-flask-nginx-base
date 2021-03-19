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
def column_1(df):
    result1 = df['document'].str[0]
    return result1



#2
def column_2(df):
    result2 = df['document'].str[1:]
    result2 = result2.apply(int)
    result2 = result2.apply(str)
    return result2



#3
def column_3(df):
    result3 = df['document_source'] + df['document_number']
    return result3



#4 Documents starting with A: Original order lead time (original delivery requirement date - original order date) Number of days
def column_4(df):
    result4 = df['original_delivery_requirement_date']- df['order_date']
    # Remove days from given column output
    for i in df.index:
        result4[i] = result4[i].days
    # Search within A:
    for row, value in df["document_source"].items(): 
        if value != 'A': 
            result4[row] = ''
            result4.replace(np.nan,'NA')
    return result4   
        


#5
def column_5(df):
    result5= []
    for row, value in df['document_source'].items():
        if value == 'A':
            if (df['requirement_date_current'][row] == df['original_delivery_requirement_date'][row]):
                result5.append(0)
            elif (df['requirement_date_current'][row] < df['original_delivery_requirement_date'][row]):
                result5.append(1)
            elif (df['requirement_date_current'][row] > df['original_delivery_requirement_date'][row]):
                result5.append(2)   
            else:
                result5.append('NA')
        else:
            result5.append('')
    return result5       
        


#6
def column_6(df):
    result6 = []
    for row, value in df['document_source'].items():
        if value == 'A':
            if (df['requirement_date_current'][row] == df['requirement_date_1_weeks_ago'][row]):
                result6.append(0)
            elif (df['requirement_date_current'][row] < df['requirement_date_1_weeks_ago'][row]):
                result6.append(1)
            elif (df['requirement_date_current'][row] > df['requirement_date_1_weeks_ago'][row]):
                result6.append(2)  
            else:
                result6.append('NA')
        else:
            result6.append('')
    return result6        
        



#7
def column_7(df):
    result7 = []
    for row, value in df['A_current_req_date_vs_req_date_1_wk_ago'].items():
        if value == 1:
            result7.append(1)
        elif value == 'NA':
            result7.append('NA')
        elif value == '': 
            result7.append('')
        else:
            result7.append(0)
    return result7



#8
def column_8(df):
    result8 = []
    for row, value in df['A_current_req_date_vs_req_date_1_wk_ago'].items():
        if value == 2:
            result8.append(1)
        elif value == 'NA':
            result8.append('NA')
        elif value == '': 
            result8.append('')
        else:
            result8.append(0)
    return result8  



#9 Documents starting with K: KMWE committed delivery date - Original delivery requirement date (Number of Days)
def column_9(df):
    df['kmwe_committed_delivery_date_current'] = pd.to_datetime(df['kmwe_committed_delivery_date_current'],errors='coerce')    
    result9 = df['kmwe_committed_delivery_date_current'] - df['original_delivery_requirement_date']
    # Remove days from given column output
    for i in df.index:
        result9[i] = result9[i].days
        # Search within K:
    for row, value in df["document_source"].items(): 
        if value != 'K': 
            result9[row]  = ''
            result9 = result9.replace(np.nan,'NA')             
    return result9        
            
#10
def column_10(df):
    result10 = []
    for i in df.index:
        doc_num = df.loc[df['end_customer_internal_work_order'] == df['end_customer_internal_work_order'][i]]['document_original'][i]
        order_num = df.loc[df['end_customer_internal_work_order'] == df['end_customer_internal_work_order'][i]]['end_customer_internal_work_order'][i]
        order_index = doc_num[1:]
        previous_week_half = int(order_index) - 1
        previous_week_full = str(doc_num[0]) + str(previous_week_half)
        all_order_num = df.loc[df['end_customer_internal_work_order'] == order_num, 'document_original'].reset_index()
    
        if previous_week_full in all_order_num.values:
            org_index = all_order_num[all_order_num['document_original'] == previous_week_full]['index'].values[0]
            val = df['kmwe_committed_delivery_date_current'][org_index] 
            result10.append(val)
        else:
            result10.append('')
    return result10       



#11
def column_11(df):
    df['KMWE_committed_delivery_date_1_weeks_ago_updated'] = pd.to_datetime(df['KMWE_committed_delivery_date_1_weeks_ago_updated'])
    result11 = []
    for row, value in df['document_source'].items():
        if value == 'K':
            if (df['kmwe_committed_delivery_date_current'][row] == df['KMWE_committed_delivery_date_1_weeks_ago_updated'][row]):
                result11.append(0)
            elif (df['kmwe_committed_delivery_date_current'][row] < df['KMWE_committed_delivery_date_1_weeks_ago_updated'][row]):
                result11.append(1)
            elif (df['kmwe_committed_delivery_date_current'][row] > df['KMWE_committed_delivery_date_1_weeks_ago_updated'][row]):
                result11.append(2)   
            else:
                result11.append('NA')
        else:
            result11.append('')
    return result11                                                                      



#12
def column_12(df):
    result12 = []
    for row, value in df['K_KMWE_committed_delivery_date_current_vs_1_wk_ago'].items():
        if value == 1:
            result12.append(1)
        elif value == 'NA':
            result12.append('NA')
        elif value == '': 
            result12.append('')
        else:
            result12.append(0)
    return result12



#13
def column_13(df):
    result13 = []
    for row, value in df['K_KMWE_committed_delivery_date_current_vs_1_wk_ago'].items():
        if value == 2:
            result13.append(1)
        elif value == 'NA':
            result13.append('NA')
        elif value == '': 
            result13.append('')
        else:
            result13.append(0)
    return result13



#14
def column_14(df):
    df['K_based_on_A_current_requirement_date_vs_requirement_date_1_wk_ago'] = ''
    for row in df.index:
        order_num = df['end_customer_internal_work_order'][row]
        df1 = df[df['end_customer_internal_work_order'] == order_num]
        a = df1['A_current_req_date_vs_req_date_1_wk_ago'][row]
        if a != '':
            doc_org = df1['document_original'][row].replace('A','K')
            if doc_org in df1['document_original'].tolist():
                df2 = df1[df1['document_original'] == doc_org]
                org_index = df2.index[0]
                df['K_based_on_A_current_requirement_date_vs_requirement_date_1_wk_ago'][org_index] = a
    return df['K_based_on_A_current_requirement_date_vs_requirement_date_1_wk_ago']
#15
def column_15(df):
    result15 = []
    for row, value in df['K_based_on_A_current_requirement_date_vs_requirement_date_1_wk_ago'].items():
        if value == 1:
            result15.append(1)
        elif value == '': 
            result15.append('')
        else:
            result15.append(0)
    return result15       



def column_16(df):
    result16 = []
    for row, value in df['K_based_on_A_current_requirement_date_vs_requirement_date_1_wk_ago'].items():
        if value == 2:
            result16.append(1)
        elif value == '': 
            result16.append('')
        else:
            result16.append(0)
    return result16        



#17 Re-out occured but actual completion date stays the same (i.e., inventory is held) 1: inventory held, 0: completion date moved
def column_17(df):
    result17 = []
    for row, value in df['K_based_on_A_req_date_current_vs_1_wk_ago_re_out'].items():
        if value == 1:
            a = df['K_KMWE_committed_delivery_date_current_vs_1_wk_ago'][row]
            if a == 0:
                result17.append(1)
            else:
                result17.append(0)
            
            
        elif value == 0:
            result17.append('NA')
        else: 
            result17.append('')
    return result17      




#18 re_out_occured_but_a_re_in_came_in_at_a_later_date(1: Re-in came in at a later date, 0: no further re-ins came in; NA: No re-out)
def column_18(df):
    result18 = []
    for row, value in df['A_current_req_date_vs_req_date_1_wk_ago_re_out'].items():
        if value == 1:
            index_num = row
            order_num = df['end_customer_internal_work_order'][row]
            all_orders = df.loc[df['end_customer_internal_work_order'] == order_num, 'K_based_on_A_req_date_current_vs_1_wk_ago_re_in'].reset_index()
            all_orders_index = all_orders[all_orders['index'] == index_num].index[0]
            condition = int(all_orders['K_based_on_A_req_date_current_vs_1_wk_ago_re_in'][all_orders_index + 1:].apply(pd.to_numeric, errors='coerce').sum())
            if condition > 0:
                result18.append(1)
            else:
                result18.append('#NA')
        else:
            result18.append('NA')
    return result18      



#19 Re-in occured but actual completion date stays the same (i.e., re-in was not met) 1: completion date stays same, 0: completion date moved
def column_19(df):
    result19 = []
    for row, value in df['K_based_on_A_req_date_current_vs_1_wk_ago_re_in'].items():
        if value == 1:
            a = df['K_KMWE_committed_delivery_date_current_vs_1_wk_ago'][row]
            if a == 0:
                result19.append(1)
            else:
                result19.append(0)
            
            
        elif value == 0:
            result19.append('NA')
        else: 
            result19.append('')
    return result19       



#20 Re-in occured but a re-out came in at a later date(1. Re-out came in at a later date, 0: no further re-outs came in; NA: No re-in)
# TODO change the name of function
def column_20(df):
    result20 = []
    for row, value in df['A_current_req_date_vs_req_date_1_wk_ago_re_in'] .items():
        if value == 1:
            index_num = row
            order_num = df['end_customer_internal_work_order'][row]
            all_orders = df.loc[df['end_customer_internal_work_order'] == order_num, 'K_based_on_A_req_date_current_vs_1_wk_ago_re_out'].reset_index()
            all_orders_index = all_orders[all_orders['index'] == index_num].index[0]
            condition = int(all_orders['K_based_on_A_req_date_current_vs_1_wk_ago_re_out'][all_orders_index + 1:].apply(pd.to_numeric, errors='coerce').sum())
            if condition > 0:
                result20.append(1)
            else:
                result20.append('#NA')
        else:
            result20.append('NA')
    return result20       
        


def fsl_lambda_main():
    # Read data from PostgreSQL database table and load into a DataFrame instance
    df = pd.read_sql("select * from fsl_lambda", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))

    df['document_source'] = column_1(df)
    df['document_number'] = column_2(df)
    df['document_original'] = column_3(df)
    df['A_original_order_lead_time_number_of_days'] =  column_4(df)
    df['A_current_req_date_vs_original_delivery_req_date'] = column_5(df)
    df['A_current_req_date_vs_req_date_1_wk_ago'] = column_6(df)
    df['A_current_req_date_vs_req_date_1_wk_ago_re_in'] = column_7(df)
    df['A_current_req_date_vs_req_date_1_wk_ago_re_out'] = column_8(df)
    df['K_KMWE_order_lead_time_number_of_days'] = column_9(df)
    df['KMWE_committed_delivery_date_1_weeks_ago_updated'] = column_10(df)
    df['K_KMWE_committed_delivery_date_current_vs_1_wk_ago'] = column_11(df)
    df['K_KMWE_committed_delivery_date_current_vs_1_wk_ago_re_in'] = column_12(df)
    df['K_KMWE_committed_delivery_date_current_vs_1_wk_ago_re_out'] = column_13(df)
    column_14(df)
    df['K_based_on_A_req_date_current_vs_1_wk_ago_re_in'] = column_15(df)
    df['K_based_on_A_req_date_current_vs_1_wk_ago_re_out'] = column_16(df)
    df['re_out_occured_but_actual_completion_date_stays_the_same'] = column_17(df)
    df['re_out_occured_but_a_re_in_came_in_at_a_later_date'] = column_18(df)
    df['re_in_occured_but_actual_completion_date_stays_the_same'] = column_19(df)
    df['re_in_occured_but_a_re_out_came_in_at_a_later_date'] = column_20(df)
    #conn = config('D:\projects\KMWE_product_planning\database1.ini','postgresql').connect()
    df.to_sql("fsl_lambda_analysis",config('D:\projects\KMWE_product_planning\database1.ini','postgresql'), if_exists = 'replace', chunksize = 1000, index = False) 
    #conn.close()
    print(df)
    

if __name__ == "__main__":
    # fsl_lambda_main()
    pass










