import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from sklearn import preprocessing
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




def column_1(df1):
    result1 = df1[['unique_part_identifier']] 
    return result1


def column_2(df):
    part_no = df["unique_part_identifier"].str.split(":", n = 1, expand = True)
    result2 = part_no[1]
    return result2


def column_3(df1):
    result3 = df1['Plane']
    return result3

## 1. Cash Flow Impact Index

### 1a. Raw values
def column_4(df1):
    result4 = df1['relative_avg_price_of_part_1_low_cost_5_high_cost']
    return result4


def column_5(df1):
    result5 = df1['total_order_qty_across_work_orders'] / df1['number_of_unique_work_orders_excl_PA']
    result5 = result5.round(1)
    return result5


def column_6(df):
    result6 = df['relative_avg_price'] * df['avg_order_qty_per_unq_work_orders']
    result6 = result6.round()
    return result6


def column_7(df):
    result7 = []
    data = df['cash_flow_impact']
    q1, q3 = np.percentile(data,[25,75])
    iqr = q3 - q1
    lower_bound = q1 -(1.5 * iqr) 
    upper_bound = q3 +(1.5 * iqr)
    for i in data.tolist():
        if i >= lower_bound and i <= upper_bound:
            result7.append(False)
        else:
            result7.append(True)
    return result7 


### 1b. Standardization 
def column_8(df):
    x = df['relative_avg_price'].values #returns a numpy array
    x = x.reshape(-1, 1) # if your data has a single feature
    standard_scaler = preprocessing.StandardScaler()
    x_scaled = standard_scaler.fit_transform(x)
    result8 = pd.DataFrame(x_scaled)
    return result8


def column_9(df):
    x = df['avg_order_qty_per_unq_work_orders'].values #returns a numpy array
    x = x.reshape(-1, 1) # if your data has a single feature
    standard_scaler = preprocessing.StandardScaler()
    x_scaled = standard_scaler.fit_transform(x)
    result9 = pd.DataFrame(x_scaled)
    return result9


def column_10(df):
    result10 = df['standardized_relative_avg_price_normal_distr'] * df['standardized_avg_order_qty_per_unq_work_order_normal_distr']
    return result10


### 1c. Including Outliers
def column_11(df):
    x = df['standardized_cash_flow_impact_incl_outliers'].values #returns a numpy array
    x = x.reshape(-1, 1) # if your data has a single feature
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    result11 = pd.DataFrame(x_scaled)
    return result11


def column_12(df):
    result12 = df['normalized_cash_flow_impact_0_to_1_scale_incl_outliers'].rank(method='min')
    return result12


### 1d. Excluding Outliers
def column_13(df):
    result13 = []
    for row, value in df['outlier'].items():
        if value != True:
            a = df['standardized_cash_flow_impact_incl_outliers'][row]
            result13.append(a)
        else:
            result13.append(np.nan)
    return result13


def column_14(df):
    x = df['standardized_cash_flow_impact_excl_outliers'].values #returns a numpy array
    x = x.reshape(-1, 1) # if your data has a single feature
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    result14 = pd.DataFrame(x_scaled)
    return result14


def column_15(df):
    result15 = df['normalized_cash_flow_impact_0_to_1_scale_excl_outliers'].rank(method='min')
    return result15    


## 2. Disruption Index

### 2a. Weights-Raw Values
def column_16(df1):
    result16 = df1['Volume']
    return result16


def column_17(df):
    result17 = np.nan
    return result17


def column_18(df3):
    result18 = df3['WC_disruption']
    return result18


def column_19(df1):
    result19 = df1['total_re_outs_in_6_month_period_across_work_orders']
    return result19


def column_20(df1):
    result20 = df1['total_unq_FA_work_orders_with_re_outs']
    return result20


def column_21(df2):
    result21 = df2['total_re_ins_in_6_month_period_across_work_orders'] * 2
    return result21


def column_22(df2):
    result22 = df2['total_unq_FA_work_orders_with_re_ins'] * 2
    return result22


def column_23(df):
    result23 = df.iloc[:, -7:].sum(axis=1)
    return result23


def column_24(df):
    result24 = []
    data = df['disruption_score_using_raw_values']
    q1, q3 = np.percentile(data,[25,75])
    iqr = q3 - q1
    lower_bound = q1 -(1.5 * iqr) 
    upper_bound = q3 +(1.5 * iqr)
    for i in data.tolist():
        if i >= lower_bound and i <= upper_bound:
            result24.append(False)
        else:
            result24.append(True)
    return result24                 


### 2b. Weights-Standardization
def column_25(df):
    x = df['volume_of_part'].values #returns a numpy array
    x = x.reshape(-1, 1) # if your data has a single feature
    standard_scaler = preprocessing.StandardScaler()
    x_scaled = standard_scaler.fit_transform(x)
    result25 = pd.DataFrame(x_scaled)
    return result25


def column_26(df):
    x = df['number_of_tools_used'].values #returns a numpy array
    x = x.reshape(-1, 1) # if your data has a single feature
    standard_scaler = preprocessing.StandardScaler()
    x_scaled = standard_scaler.fit_transform(x)
    result26 = pd.DataFrame(x_scaled)
    return result26


def column_27(df):
    x = df['workcenter'].values #returns a numpy array
    x = x.reshape(-1, 1) # if your data has a single feature
    standard_scaler = preprocessing.StandardScaler()
    x_scaled = standard_scaler.fit_transform(x)
    result27 = pd.DataFrame(x_scaled)
    return result27


def column_28(df):
    x = df['total_re_outs_in_6_month_period_across_work_orders'].values #returns a numpy array
    x = x.reshape(-1, 1) # if your data has a single feature
    standard_scaler = preprocessing.StandardScaler()
    x_scaled = standard_scaler.fit_transform(x)
    result28 = pd.DataFrame(x_scaled)
    return result28


def column_29(df):
    x = df['total_unq_FA_work_orders_with_re_outs'].values #returns a numpy array
    x = x.reshape(-1, 1) # if your data has a single feature
    standard_scaler = preprocessing.StandardScaler()
    x_scaled = standard_scaler.fit_transform(x)
    result29 = pd.DataFrame(x_scaled)
    return result29


def column_30(df):
    x = df['total_re_ins_in_6_month_period_across_work_orders'].values #returns a numpy array
    x = x.reshape(-1, 1) # if your data has a single feature
    standard_scaler = preprocessing.StandardScaler()
    x_scaled = standard_scaler.fit_transform(x)
    result30 = pd.DataFrame(x_scaled * 2)
    return result30


def column_31(df):
    x = df['total_unq_FA_work_orders_with_re_ins'].values #returns a numpy array
    x = x.reshape(-1, 1) # if your data has a single feature
    standard_scaler = preprocessing.StandardScaler()
    x_scaled = standard_scaler.fit_transform(x)
    result31 = pd.DataFrame(x_scaled * 2)
    return result31


def column_32(df):
    result32 = df.iloc[:, -7:].sum(axis=1)
    return result32


### 2c. Including Outliers
def column_33(df):
    x = df['disruption_score_using_standardized_raw_values'].values #returns a numpy array
    x = x.reshape(-1, 1) # if your data has a single feature
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    result33 = pd.DataFrame(x_scaled)
    return result33


def column_34(df):
    result34 = df['disruption_score_using_normalized_raw_values_0_1_scale'].rank(method='min')
    return result34


def column_35(df):
    result35 = []
    for row, value in df['disruption_score_using_raw_values_outlier'].items():
        if value != True:
            a = df['disruption_score_using_standardized_raw_values'][row]
            result35.append(a)
        else:
            result35.append(np.nan)
    return result35


def column_36(df):
    x = df['disruption_score_using_raw_values_excl_outliers'].values #returns a numpy array
    x = x.reshape(-1, 1) # if your data has a single feature
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    result36 = pd.DataFrame(x_scaled)
    return result36


def column_37(df):
    result37 = df['normalized_disruption_score_using_raw_values_0_1_excl_outliers'].rank(method='min')
    return result37


def cash_disr_main():
    # Read data from PostgreSQL database table and load into a DataFrame instance
    df1 = pd.read_sql("select * from q1abc_table", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))
    df2 = pd.read_sql("select * from q2abc_table", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))
    df3 = pd.read_sql("select * from x_ref_item_lambda_analysis", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'))
    
    df = column_1(df1)
    df['part_number'] =  column_2(df)
    df['plane'] =  column_3(df1)
    df['relative_avg_price'] = column_4(df1)
    df['avg_order_qty_per_unq_work_orders'] = column_5(df1)
    df['cash_flow_impact'] = column_6(df) 
    df['outlier'] =  column_7(df)
    df['standardized_relative_avg_price_normal_distr'] = column_8(df)
    df['standardized_avg_order_qty_per_unq_work_order_normal_distr'] = column_9(df)
    df['standardized_cash_flow_impact_incl_outliers'] = column_10(df)
    df['normalized_cash_flow_impact_0_to_1_scale_incl_outliers'] = column_11(df)
    df['cash_flow_impact_rank_order_incl_outliers'] = column_12(df)
    df['standardized_cash_flow_impact_excl_outliers'] = column_13(df)
    df['normalized_cash_flow_impact_0_to_1_scale_excl_outliers'] = column_14(df)
    df['cash_flow_impact_rank_order_excl_outliers'] = column_15(df)
    df['volume_of_part'] = column_16(df1)
    df['number_of_tools_used'] = column_17(df)
    df['workcenter'] = column_18(df3)
    df['total_re_outs_in_6_month_period_across_work_orders'] = column_19(df1)
    df['total_unq_FA_work_orders_with_re_outs'] = column_20(df1)
    df['total_re_ins_in_6_month_period_across_work_orders'] = column_21(df2)
    df['total_unq_FA_work_orders_with_re_ins'] = column_22(df2)
    df['disruption_score_using_raw_values'] = column_23(df)
    df['disruption_score_using_raw_values_outlier'] =  column_24(df)
    df['standardized_volume_of_part'] = column_25(df)
    df['standardized_number_of_tools_used'] = column_26(df)
    df['standardized_workcenter'] = column_27(df)
    df['standardized_total_re_outs_in_6_month_period_across_work_orders'] = column_28(df)
    df['standardized_total_unq_FA_work_orders_with_re_outs'] = column_29(df)
    df['standardized_total_re_ins_in_6_month_period_across_work_orders']  =  column_30(df) 
    df['standardized_total_unq_FA_work_orders_with_re_ins'] = column_31(df)
    df['disruption_score_using_standardized_raw_values'] = column_32(df)
    df['disruption_score_using_normalized_raw_values_0_1_scale'] = column_33(df)
    df['disruption_index_rank_order_incl_outliers'] = column_34(df)
    df['disruption_score_using_raw_values_excl_outliers'] = column_35(df)
    df['normalized_disruption_score_using_raw_values_0_1_excl_outliers'] = column_36(df)
    df['normalized_disruption_index_rank_order_excl_outliers'] = column_37(df)

    df.to_sql("cash_vs_disruption", config('D:\projects\KMWE_product_planning\database1.ini','postgresql'), if_exists = 'replace', chunksize = 1000, index = False) 
    print(df)


    
if __name__ == "__main__":
    cash_disr_main()  
    # pass                                                               










