
import numpy as np  
import pandas as pd 
import matplotlib.pyplot as plt
from sklearn import cluster
import os
import re
import sklearn
from datetime import date
# from sklearn.metrics import davies_bouldin_score
# import seaborn as sns
os.getcwd()
import warnings
warnings.filterwarnings('ignore')
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# from MergeDataFrameToTable import MergeDFToTable
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
"""
load data
"""


df = spark.sql("""select * 
        from dsc_dws.dws_dsc_wh_ou_daily_kpi_sum""")

# df.show(15,False)

df = df.select("*").toPandas()



df2 =  spark.sql("""select substr(inc_day, 1,6) as month, ou_code
        ,count(w.emp_code) as total_head_count
        ,count(distinct case when lower(w.emp_type) = 'perm' then w.emp_code else '' end) - 1 as pm_head_cnt
        ,count(distinct case when lower(w.emp_type) = 'outsource' then w.emp_code else '' end) - 1 as os_head_cnt
        ,count(distinct case 
        when lower(w.emp_type) = 'contract' then w.emp_code
        when lower(w.emp_type) = 'sub contract' then w.emp_code
        else '' end) - 1 as ctrct_head_cnt
        ,sum(w.working_hours) as total_working_hours
        ,sum(case when lower(w.emp_type) = 'perm' then w.working_hours else 0 end) as perm_working_hours
        ,sum(case when lower(w.emp_type) = 'outsource' then w.working_hours else 0 end) as outsource_working_hours
        ,sum(case when lower(w.emp_type) not in ('perm','outsource') or w.emp_type is null then w.working_hours else 0 end) as other_working_hour
        ,sum(case when lower(s.direct) = 'direct' then w.working_hours else  0 end) as direct_working_hour
        ,sum(case when lower(coalesce(s.direct,'indirect')) = 'indirect' then w.working_hours else 0 end) as indirect_working_hour
        from dsc_dwd.dwd_hr_dsc_working_hour_dtl_di w
        left join dsc_dim.dim_dsc_staff_info s
        on w.emp_code = s.staff_id
        where ou_code <> ''
        and working_hours > 0 
        group by ou_code, substr(inc_day, 1,6) """)

df2 = df2.select("*").toPandas()


"""
test local
"""

# link = r'C:\Users\dscshap3808\Documents\my_scripts_new\play1\daily_ou_kpi.csv'
# df = pd.read_csv(link)
# df.head()
# re1 = re.compile(r'(?<=\.).+')
# df.columns = [re1.findall(i)[0] for i in list(df.columns.to_numpy())]
 

"""
test local end
"""

print("=================================0================================")
print("Sklearn version here:", sklearn.__version__)
print("=================================0================================")
# print(df.head())
df.tail()

"""
clean data,
operation day less than 24 days in two months will be removed.
inb oub qty sum always nill,  will be removed.
# tt_workinghour always nill, will be removed. 
only keep rows where total working hour is not nill 
"""
# clean_df0 :
df = df.dropna(axis =0,subset =['ou_code'])
df['operation_day'] = df['operation_day'].apply(int)
# df = df[df['operation_day'] >= 20210601] 


df['qty_sum'] = df['inbound_receive_qty']+ df['outbound_shipped_qty']
df['montly_qty_sum'] = df.groupby(
    ['month', 'ou_code'])['qty_sum'].transform('sum')


  

print("===================calculate_2, below is other calculate measures=========================")
"""
add out resource part end 
"""

df_final.head()
df_final['max_wh'] = df_final.groupby(
    ['ou_code', 'kernal_core1', 'kernal_core2']
    )['total_working_hour'].transform('max')
df_final['min_wh'] = df_final.groupby(
    ['ou_code', 'kernal_core1', 'kernal_core2']
    )['total_working_hour'].transform('min')
df_final['median_wh'] = df_final.groupby(
    ['ou_code', 'kernal_core1', 'kernal_core2']
    )['total_working_hour'].transform('median')
df_final['mean_wh'] = df_final.groupby(
    ['ou_code', 'kernal_core1', 'kernal_core2']
    )['total_working_hour'].transform('mean')
df_final['qt_66_wh'] = df_final.groupby(
    ['ou_code', 'kernal_core1', 'kernal_core2']
    )['total_working_hour'].transform('quantile', .6667)
df_final['qt_75_wh'] = df_final.groupby(
    ['ou_code', 'kernal_core1', 'kernal_core2']
    )['total_working_hour'].transform('quantile', .75)

df_final['d_to_core_outer'] =  np.abs(
    df_final['total_working_hour']-df_final['kernal_value3']).round(3)
df_final['d_to_core_outer_os'] = np.abs(
    df_final['outsource_working_hour'] - df_final['kernal_value4']).round(3)  

df_final['percent_error_66'] = (
        df_final['qt_66_wh'] - df_final['total_working_hour'])/(
                df_final['total_working_hour']
                )
df_final['percent_error_75'] = (
        df_final['qt_75_wh'] - df_final['total_working_hour']
        )/(df_final['total_working_hour']
        )
 
# def cals_cunc(df):
#     cals = df.groupby(
#         ['ou_code', 'kernal_core1', 'kernal_core2']
#         ).agg({
#             'total_working_hour': ['max', 'min', 'median', 'mean', \
#                 lambda a: a.quantile(.6667), 
#                 lambda b: b.quantile(.75)]
#         }).reset_index()

#     cals.columns = ['ou_code','kernal_core1','kernal_core2',\
#         'max_wh','min_wh','median_wh','mean_wh','qt_66_wh','qt_75_wh']

#     return cals

# cals = cals_cunc(df_final)
# df_final = df_final.merge(cals, on = ['ou_code', 'kernal_core1', 'kernal_core2'], how = 'left')

# def mutate_cals(df_final): 
#     df_final['d_to_core_outer'] = np.abs(df_final['total_working_hour'] - df_final['kernal_value3']).round(3)
#     df_final['d_to_core_outer_os'] = np.abs(df_final['outsource_working_hour'] - df_final['kernal_value4']).round(3)
#     df_final['percent_error_66'] = (df_final['qt_66_wh'] - df_final['total_working_hour'])\
#         /(df_final['total_working_hour'])
#     df_final['percent_error_75'] = (df_final['qt_75_wh'] - df_final['total_working_hour'])\
#         /(df_final['total_working_hour'])

    
#     return df_final

# df_final = mutate_cals(df_final)

 


df_final['qt_75_os'] = df_final.groupby(
    ['ou_code', 'kernal_core1', 'kernal_core2']
    )['outsource_working_hour'].transform('quantile', .75)

df_final['pe_66_os'] = (
        df_final['qt_75_os'] - df_final['outsource_working_hour'])/(
                df_final['outsource_working_hour']
                )
df_final['pe_75_os'] = (
        df_final['qt_75_os'] - df_final['outsource_working_hour']
        )/(df_final['outsource_working_hour']
        )
 

df_final['qt_75_dis_core_os_inner'] = df_final.groupby(
    ['ou_code', 'kernal_core4']
    )['dis_core_os'].transform('quantile', .75)

df_final['qt_75_dis_core_os_outer'] = df_final.groupby(
    ['ou_code', 'kernal_core1', 'kernal_core2']
    )['d_to_core_outer_os'].transform('quantile', .75)


"""
工时异常以及额外工时标记.
"""
 

print("=================================1================================")
  
 

# diff_tt_kn = pd.concat([diff_tt_kn.rename({'qt_75_os' : 'dis_tt_kernel'}, axis = 1), \
#     df_final[df_final['flag_75_wh'] == 1]], axis = 1)[['dis_tt_kernel', 'ou_code', 'operation_day']]

# df_final = df_final.merge(diff_tt_kn, on = ['ou_code', 'operation_day'], how = 'left').fillna(0)


"""
ssr corr
"""
std_table = df_final.groupby('ou_code').agg({
    'inbound_receive_qty': ['std'],
    'outbound_shipped_qty': ['std'],
    'outsource_working_hour': ['std']
    }).reset_index()

std_table.columns = ['ou_code', 'inb_qty_std', 'outb_qty_std', 'os_wh_std']

df_final = df_final.merge(std_table, on = 'ou_code', how = 'left')


def data_logs(df_final):
    df_final['log_inb_qty'] = np.log2(df_final['inbound_receive_qty'])
    df_final['log_outb_qty'] = np.log2(df_final['outbound_shipped_qty'])
    df_final_copy = df_final[
        df_final['log_inb_qty'] > -10000 & ~np.isnan(df_final['log_inb_qty'])]  
    df_final_copy2 = df_final[
        df_final['log_outb_qty'] > -10000 & ~np.isnan(df_final['log_outb_qty'])] 
    in_boundary = df_final_copy.groupby('ou_code')['log_inb_qty'].agg(['mean', 'std']).reset_index()
    ou_boundary = df_final_copy2.groupby('ou_code')['log_outb_qty'].agg(['mean', 'std']).reset_index()
    in_boundary.columns = ['ou_code', 'mean_inb_log', 'std_inb_log']
    ou_boundary.columns = ['ou_code', 'mean_oub_log', 'std_oub_log']
    return in_boundary , ou_boundary

in_boundary , ou_boundary = data_logs(df_final)
df_final = df_final.merge(
    in_boundary, on = 'ou_code', how = 'left').merge(
    ou_boundary, on = 'ou_code', how = 'left')

df_final  = df_final.replace(float('inf'), 0) 
df_final['log_inb_qty'] = df_final['log_inb_qty'].astype(float)
df_final['log_outb_qty'] = df_final['log_outb_qty'].astype(float)#log_inb_qty	log_outb_qty
print("=================================2================================")
"""
ssr corr done
"""


df_final['date_stamp'] = str(date.today())
df_final['date_stamp'] = df_final['date_stamp'].str.replace('-', '')
 
# df_final['inc_day']  = '99991231'

df_final['flag_75_wh'] = df_final['flag_75_wh'].astype(str)
df_final['kernal_value4'] = df_final['kernal_value4'].astype(float)
df_final['pe_66_os'] = df_final['pe_66_os'].astype(float)
df_final['pe_75_os'] = df_final['pe_75_os'].astype(float)
df_final['kernal_core4'] = df_final['kernal_core4'].astype(int)
pd.set_option("display.max_rows", None, "display.max_columns", None)
print(df_final.head(5))

"""
add ou_name & bg_name
"""

df_ou_bg = spark.sql("""select * 
        from dsc_dws.dws_dsc_wh_ou_daily_kpi_sum""")
        
df_ou_bg = df_ou_bg.select("*").toPandas()
df_ou_bg = df_ou_bg[['bg_code','bg_name_cn','ou_code','ou_name']]
df_final = df_final.merge(df_ou_bg, on = 'ou_code', how = 'left')
df_final = df_final.drop_duplicates()

df_final['inc_day']  = '99991231'
"""
end
"""
print("=================================3================================")
# df_final
df = spark.createDataFrame(df_final)

df.show(11, False)

df.createOrReplaceTempView("df_final")

df.show(15, False)
df = spark.sql("""select ou_code, cast(operation_day as string),inbound_receive_qty
,kernal_core1,kernal_value1,outbound_shipped_qty,kernal_core2,kernal_value2
,total_working_hour,kernal_core3,kernal_value3,dis_core,outbound_inbound_qty_ratio
,working_hour_per_head,total_head_count,is_holiday,max_wh,min_wh,median_wh
,mean_wh,qt_66_wh,qt_75_wh,d_to_core_outer,percent_error_66,percent_error_75
,date_stamp
,bg_code,bg_name_cn,ou_name,
kernal_core4,kernal_value4,
pe_66_os, pe_75_os, flag_75_wh,qt_75_os,
inb_qty_std, outb_qty_std, os_wh_std,
outsource_working_hour,dis_core_os,
d_to_core_outer_os,qt_75_dis_core_os_inner,qt_75_dis_core_os_outer,
dis_tt_kernel,
log_inb_qty,log_outb_qty, mean_inb_log, std_inb_log, mean_oub_log, std_oub_log 
,flag_75_os, dis_os_kernel
,inc_day 
from df_final
""")
df.schema

df.repartition("inc_day").write.mode("overwrite").partitionBy(
    "inc_day").parquet(
        "hdfs://dsc/hive/warehouse/dsc/DWS/dsc_dws/dws_qty_working_hour_labeling_sum_df")
spark.sql("""msck repair table dsc_dws.dws_qty_working_hour_labeling_sum_df""")
spark.sql("""alter table dsc_dws.dws_qty_working_hour_labeling_sum_df drop partition (inc_day='20210817')""")


    
# C:\Users\dscshap3808\Documents\my_scripts_new\play1\play1_script.py

# df.write.mode("overwrite").parquet(
# "hdfs://dsc/hive/warehouse/dsc/DWS/dsc_dws/dws_qty_working_hour_labeling_sum_df/inc_day=" + str(date.today()).replace('-', '')

