

# %%
from pickle import FALSE
import numpy as np  
import pandas as pd
from scipy import stats
import re
import sklearn
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from MergeDataFrameToTable import MergeDFToTable
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from pandas.io.parsers import ParserBase
import argparse
import warnings
import sys 

warnings.filterwarnings('ignore')
print("Sklearn version here:", sklearn.__version__, '\t')
print("python version here:", sys.version, '\t') 
print("pandas version here:", pd.__version__, '\t') 


# %%



def run_etl(start_date, end_date, if_local, outb_para, std_para, std_para2):
    
    
    try: 
        outb_para = np.float(outb_para[0])
        std_para  = np.float(std_para[0])
        std_para2 = np.float(std_para2[0])        
    except:
        pass
    print('====\n',start_date, end_date, if_local, outb_para, std_para, std_para2, 'args11', '\n====')
    def ld_dt(if_local):
        global spark
        if if_local == True:
            link = r'C:\Users\dscshap3808\Documents\my_scripts_new\play1\ou_daily_kpi.csv'
            # link = r'C:\Users\dscshap4085\SFDSC/ou_daily_kpi.csv'
            df = pd.read_csv(link, sep = '\001').fillna(0)
            df.columns =  [re.sub('\w+\.', '', i) for i in df.columns]
        else:
            spark = SparkSession.builder.enableHiveSupport().getOrCreate()
            spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
            """
            load data
            """
            df = spark.sql("""select * from dsc_dws.dws_dsc_wh_ou_daily_kpi_sum where operation_day >= '20220101' """)
            # df.show(15,False)
            df = df.select("*").toPandas()
            df = df.fillna(0)
            df = df.replace([np.inf, -np.inf], 0) 
            pass
        df[[
            'inbound_receive_qty', 'outbound_shipped_qty', 'total_working_hour','operation_day'
            ]] = df[[
                'inbound_receive_qty', 'outbound_shipped_qty', 'total_working_hour','operation_day'
                ]].astype(np.int64)

        clean_df2 = df.groupby('ou_code')[[
            'inbound_receive_qty', 'outbound_shipped_qty'
            ]].sum().reset_index()
        clean_df2['sum'] = clean_df2.sum(axis = 1)
        clean_df2 = clean_df2[clean_df2['sum'] != 0]
        df = df[df['ou_code'].isin(clean_df2.ou_code)] 

        clean_df3 = (df.groupby('ou_code')[[
            'total_working_hour'
            ]].sum() == 0).reset_index()
        clean_df3 = clean_df3[clean_df3['total_working_hour'] == False]
        df = df[df['ou_code'].isin(clean_df3.ou_code)]
        df= df.reset_index()

        df = df[[
            'ou_code','operation_day', 'inbound_receive_qty', 'is_holiday',
            'outbound_shipped_qty','total_head_count','total_working_hour',
            'outsource_working_hour', 'perm_working_hour',
            'other_working_hour', 'direct_working_hour', 'indirect_working_hour',
            'outbound_inbound_qty_ratio', 'perm_working_hour_ratio',
            'working_hour_per_head', 'location_usage_rate', 'location_idle_rate']]

        df = df.fillna(0).sort_values('operation_day')
        df = df[df['total_working_hour'] != 0]
        return df


    #  =====================================
    df = ld_dt(if_local)
    print(df.info(), '====info====')
    ou_code = df['ou_code'].unique()
        
    # %% 
    # efficiency_selection
    df_mean_std = pd.DataFrame()
    ou_codes = []
    mean_ = []
    std_ = []
    pvalue_ = []
    total_count = []
    keep_count = []
    fail_list = []
    for sub_ou in ou_code:
        sub_df = df[(df['ou_code'] == sub_ou)\
                & (df['operation_day'] >=  int(start_date))
                & (df['operation_day'] <=  int(end_date))
                & ((df['inbound_receive_qty'] > 0)
                | (df['outbound_shipped_qty'] > 0))] 
        sub_df['per_rate'] = (sub_df['inbound_receive_qty'] +  sub_df['outbound_shipped_qty'] * outb_para) / sub_df['total_working_hour']
        sub_df = sub_df.sort_values(['operation_day'])
        sub_df = sub_df.reset_index(drop=True)
        u = sub_df['per_rate'].mean()  # 计算均值
        std = sub_df['per_rate'].std()  # 计算标准差
        try:
            result = stats.kstest(sub_df['per_rate'], 'norm', (u, std))
            if result.pvalue < 0.05:
                for i in range(5) :
                    tmp_df = sub_df.loc[(sub_df['per_rate'] <= u + std_para*std) & (sub_df['per_rate'] >= u - std_para*std)]
                    u = tmp_df['per_rate'].mean()  # 计算均值
                    std = tmp_df['per_rate'].std()  # 计算标准差
                    result = stats.kstest(tmp_df['per_rate'], 'norm', (u, std))
                    
                    if result.pvalue >= 0.05:
                        ou_codes.append(sub_ou)
                        total_count.append(sub_df.shape[0])
                        keep_count.append(tmp_df.shape[0])
                        mean_.append(u)
                        std_.append(std)
                        pvalue_.append(result.pvalue)
                        break
            else:
                total_count.append(sub_df.shape[0])
                keep_count.append(sub_df.shape[0])
                ou_codes.append(sub_ou)
                mean_.append(u)
                std_.append(std)
                pvalue_.append(result.pvalue)
        except:
            fail_list = fail_list + [sub_ou]
            


    # %%
    df_mean_std = pd.DataFrame()
    # ou_codes = []
    # mean_ = []
    # std_ = []
    # pvalue_ = []
    df_mean_std['ou_code'] = ou_codes
    df_mean_std['mean_'] = mean_
    df_mean_std['std_'] = std_
    df_mean_std['pvalue_'] = pvalue_
    df_mean_std['total_count'] = total_count
    df_mean_std['keep_count'] = keep_count


    # %%
    merge_df = pd.merge(left=df, right=df_mean_std, on='ou_code')


    # %%
    merge_df['predict_by_mean_std'] = (
        merge_df['inbound_receive_qty'] +  merge_df['outbound_shipped_qty']* outb_para
        ) / merge_df['mean_']
    
    ## ====
    # target = pd.read_csv('./target.csv', index = None)
    # merge_df['tar'] = .5
    target = spark.sql(""" select * from dsc_dim.dim_dsc_hr_efficiency_manual """).select("*").toPandas()
    tar_main = merge_df[['ou_code']].drop_duplicates()
    tar_main['tar'] = .5
    tar_main = tar_main[~tar_main['ou_code'].isin(target['ou_code'])]
    print(tar_main.tail())
    target = pd.concat([tar_main, target], axis = 0)
    merge_df = merge_df.merge(target, on = 'ou_code', how = 'left')
    
    ## ====
    
    merge_df['predict_by_mean_std_upper'] = (
        merge_df['inbound_receive_qty'] +  merge_df['outbound_shipped_qty']* outb_para
        ) / (merge_df['mean_'] - merge_df['tar'] * merge_df['std_'])
    merge_df['predict_by_mean_std_lower'] = (
        merge_df['inbound_receive_qty'] +  merge_df['outbound_shipped_qty']* outb_para
        ) / (merge_df['mean_'] + merge_df['tar'] * merge_df['std_'])

    merge_df['keep_cnt_rate'] = merge_df['keep_count']/ merge_df['total_count']
    merge_df['flag_exceed_upperbound'] = np.where(merge_df['predict_by_mean_std_upper']-merge_df['total_working_hour']>0,0,1)
    merge_df['flag_none_nmd'] = np.where(merge_df['pvalue_'] > 0.05, 0, 1)

    # %%
    merge_df = merge_df[['ou_code', 'operation_day', 'inbound_receive_qty', 'is_holiday',\
        'outbound_shipped_qty', 'total_head_count', 'total_working_hour',\
        'outsource_working_hour', 'perm_working_hour', 'other_working_hour',\
        'mean_', 'std_', 'pvalue_', 'total_count', 'keep_count',\
        'predict_by_mean_std', 'predict_by_mean_std_upper',\
        'predict_by_mean_std_lower','keep_cnt_rate', 'flag_exceed_upperbound', 'flag_none_nmd']]
    merge_df[['operation_day', 'total_head_count']] = merge_df[['operation_day', 'total_head_count']].astype(np.int64)
    
    merge_df['dis_to_kernel'] = np.abs(merge_df['predict_by_mean_std_upper'] - merge_df['total_working_hour'])
    print(merge_df.info())
    
    spark_df = spark.createDataFrame(merge_df)
    # spark table as view, aka in to spark env. able to be selected or run by spark sql in the following part.
    spark_df.createOrReplaceTempView("df")
    

    merge_table = "dsc_dws.dws_qty_working_hour_predicting_sum_df"
    print("===============================merge_table--%s================================="%merge_table)

    sql = """insert overwrite table """ + merge_table +  """ select * from df"""
    # print(sql)
    spark.sql(sql).show()

    # %%
    

def main():
    args = argparse.ArgumentParser()
    args.add_argument("--start_date", help="start date for refresh data, format: yyyyMMdd",\
        default=[(datetime.now() + timedelta(days=-1)).strftime('%Y%m%d')], nargs="*")
    args.add_argument("--end_date", help="end date", \
        default=[(datetime.now() + timedelta(days=-91)).strftime('%Y%m%d')], nargs="*")
    # args.add_argument("--if_local", help = '', default=False, nargs = "*")
    args.add_argument("--outb_para", help = '', default=1.2 , nargs = "*")
    args.add_argument("--std_para" , help = '', default=1.85, nargs = "*")
    args.add_argument("--std_para2", help = '', default=0.5 , nargs = "*")
    
    if_local = False
    args_parse = args.parse_args()
    start_date = args_parse.start_date[0]
    end_date = args_parse.end_date[0]
    outb_para = args_parse.outb_para
    std_para  = args_parse.std_para
    std_para2 = args_parse.std_para2
    
    run_etl(start_date, end_date, if_local, outb_para, std_para, std_para2)

    
if __name__ == '__main__':
    main()
    
# def run_etl(start_date, end_date, if_local = False, outb_para = 1.2, std_para = 1.85, std_para2 = 0.5):
