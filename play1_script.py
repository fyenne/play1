
import numpy as np  
import pandas as pd 
import matplotlib.pyplot as plt
from sklearn import cluster
import os
import re
# import seaborn as sns
os.getcwd()
import warnings
warnings.filterwarnings('ignore')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from MergeDataFrameToTable import MergeDFToTable
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
from datetime import date
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
"""
load data
"""


df = spark.sql("""select * 
        from dsc_dws.dws_dsc_wh_ou_daily_kpi_sum""")

 
# df = pd.read_csv('./play1/daily_kpi_all_810.csv')
# df.show(15,False)

df = df.select("*").toPandas()
# print(df.head())
df.tail()

"""
clean data,
operation day less than 24 days in two months will be removed.
inb oub qty sum always nill,  will be removed.
# tt_workinghour always nill, will be removed. 
only keep rows where total working hour is not nill 
"""

clean_df1 = (df.groupby('ou_code')['operation_day'].count() < 24).reset_index()
clean_df1.columns = ['ou_code', 'flag1']
df = clean_df1.merge(df, on = 'ou_code', how = 'inner')
df = df[df['flag1'] == False]

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
df = df.fillna(0)
df = df[df['total_working_hour'] != 0]
df.head()
"""
calculations functions def
""" 

def mnb_kmeans_in(ou_code):
        """
        mini batch kmeans, inbound, outbound, working hour data.
        simple algorithm, adding cols {max, min, mean, median, 75 quantile, distance to kernal}
        """
        alg1 = cluster.MiniBatchKMeans(n_clusters = 4, random_state = 529)
        """
        null data fill
        """
        df_fin = pd.DataFrame()
        df_sub = df[df['ou_code'] == ou_code][['ou_code', 'operation_day', 'inbound_receive_qty']]        
        df_fin = df_fin.append(df_sub[df_sub['inbound_receive_qty'] == 0])
        df_fin['kernal_core1' ] = -1
        df_fin['kernal_value1'] = 0
        """
        not null data training
        """
        df_rec = df_sub[df_sub['inbound_receive_qty'] != 0]      
        hist1 = alg1.fit(np.reshape(list(df_rec['inbound_receive_qty']), (-1,1)))
        df_rec['kernal_core1'] = hist1.labels_
        cl_1 = pd.concat(
                [pd.DataFrame(hist1.cluster_centers_), pd.Series(np.arange(0,4))], axis = 1
                )
        
        cl_1.columns = ['kernal_value1', 'kernal_core1']
        df_rec = df_rec.merge(
                cl_1, on = 'kernal_core1', how = 'inner'
                )
        
        """
        merging
        """
        df_fin = df_fin.append(df_rec).reset_index().drop(['index'], axis = 1)

        # df_fin['kind'] = 'inbound'

        return df_fin


def mnb_kmeans_out(ou_code):
        alg1 = cluster.MiniBatchKMeans(n_clusters = 4, random_state = 707)
        df_fin = pd.DataFrame()
        df_sub = df[df['ou_code'] == ou_code][['ou_code', 'operation_day', 'outbound_shipped_qty']]        
        df_fin = df_fin.append(df_sub[df_sub['outbound_shipped_qty'] == 0])
        df_fin['kernal_core2' ] = -1
        df_fin['kernal_value2'] = 0
        df_rec = df_sub[df_sub['outbound_shipped_qty'] != 0]

        hist1 = alg1.fit(np.reshape(list(df_rec['outbound_shipped_qty']), (-1,1)))

        df_rec['kernal_core2'] = hist1.labels_
        cl_1 = pd.concat(
                [pd.DataFrame(hist1.cluster_centers_), pd.Series(np.arange(0,4))], axis = 1
                )
        
        cl_1.columns = ['kernal_value2', 'kernal_core2']

        df_rec = df_rec.merge(
                cl_1, on = 'kernal_core2', how = 'inner'
                )
        df_fin = df_fin.append(df_rec).reset_index().drop(['index'], axis = 1)
        # df_fin['kind'] = 'outbound'
        return df_fin

def mnb_kmeans_hr(ou_code):
        alg1 = cluster.MiniBatchKMeans(n_clusters = 4, random_state = 5290707)
        df_fin = pd.DataFrame()
        df_sub = df[df['ou_code'] == ou_code][['ou_code', 'operation_day', 'total_working_hour']]        
        df_fin = df_fin.append(df_sub[df_sub['total_working_hour'] == 0])
        df_fin['kernal_core3' ] = -1
        df_fin['kernal_value3'] = 0
        df_rec = df_sub[df_sub['total_working_hour'] != 0]

        hist1 = alg1.fit(np.reshape(list(df_rec['total_working_hour']), (-1,1)))

        df_rec['kernal_core3'] = hist1.labels_
        cl_1 = pd.concat(
                [pd.DataFrame(hist1.cluster_centers_), pd.Series(np.arange(0,4))], axis = 1
                )
        
        cl_1.columns = ['kernal_value3', 'kernal_core3']

        df_rec = df_rec.merge(
                cl_1, on = 'kernal_core3', how = 'inner'
                )
        df_fin = df_fin.append(df_rec).reset_index().drop(['index'], axis = 1)

        # df_fin['kind'] = 'working_hour'
        # df_fin['max_wh']    = df_fin.groupby('kernal_core3')['total_working_hour'].transform('max')
        # df_fin['min_wh']    = df_fin.groupby('kernal_core3')['total_working_hour'].transform('min')
        # df_fin['median_wh'] = df_fin.groupby('kernal_core3')['total_working_hour'].transform('median')
        # df_fin['mean_wh']   = df_fin.groupby('kernal_core3')['total_working_hour'].transform('mean')
        # df_fin['qt_66_wh']  = df_fin.groupby('kernal_core3')['total_working_hour'].transform('quantile', .66)
        # df_fin['qt_75_wh']  = df_fin.groupby('kernal_core3')['total_working_hour'].transform('quantile', .75)
        """
        组内kernal distance 
        """
        df_fin['dis_core']  = df_fin.groupby(
                'kernal_core3', as_index = False, observed = True
                )['total_working_hour','kernal_value3'].agg('diff', axis = 1).drop('total_working_hour', axis = 1).round(3)
        return df_fin


"""
remove OUs which can not be calculate due to deficiency of data length.
"""
ou_codes = list(df['ou_code'].unique())
p = list()
for i in ou_codes:
    try: 
        mnb_kmeans_in(i)
        mnb_kmeans_out(i)
        # mnb_kmeans_hr(i)
    except:
        p.append(i)


for i in p:
    ou_codes.remove(i)

print(p)
ou_codes
"""
for loop , 对所有ou进行独立的kmeans on inb qty and outb qty
随后merge 原始表
and other calculation measures.
"""

from functools import reduce
df_final = pd.DataFrame()
for i in ou_codes:
        df_final = df_final.append(
            reduce(
                lambda left,right: pd.merge(
                    left,right,on= ['ou_code', 'operation_day']
                ), [mnb_kmeans_in(i), mnb_kmeans_out(i), mnb_kmeans_hr(i)]
                )
    )

df_final.tail()

# np.setdiff1d(ou_codes,df_final.ou_code.unique())
df_final = df_final.merge(
    df[['ou_code','operation_day','outbound_inbound_qty_ratio','working_hour_per_head','total_head_count','is_holiday']],
    on = ['ou_code', 'operation_day'],
    how = 'left'
    )

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
df_final['d_to_core_outer'] = df_final.groupby(
        ['ou_code', 'kernal_core1', 'kernal_core2'], as_index = False
                )['total_working_hour','kernal_value3'].agg(
                    'diff', axis = 1
                    ).drop('total_working_hour', axis = 1).round(3)



df_final['percent_error_66'] = (
        df_final['qt_66_wh'] - df_final['total_working_hour'])/(
                df_final['total_working_hour']
                )
df_final['percent_error_75'] = (
        df_final['qt_75_wh'] - df_final['total_working_hour']
        )/(df_final['total_working_hour']
        )


df_final['inc_day'] = str(date.today())
df_final['inc_day'] = df_final['inc_day'].str.replace('-', '')
 
# df_final['inc_day']  = '20210811'

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
,inc_day from df_final
""")
df.schema
df.repartition("inc_day").write.mode("overwrite").partitionBy(
    "inc_day").parquet(
        "hdfs://dsc/hive/warehouse/dsc/DWS/dsc_dws/dws_qty_working_hour_labeling_sum_df")
<<<<<<< HEAD
        
<<<<<<< HEAD

=======
>>>>>>> 6781027 (message)
=======

# C:\Users\dscshap3808\Documents\my_scripts_new\play1\play1_script.py
<<<<<<< HEAD
>>>>>>> 024e50f (commit)
=======

# df.write.mode("overwrite").parquet(
# "hdfs://dsc/hive/warehouse/dsc/DWS/dsc_dws/dws_qty_working_hour_labeling_sum_df/inc_day=" + str(date.today()).replace('-', '')

>>>>>>> d97d414 (commit static partition)
