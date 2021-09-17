
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
from MergeDataFrameToTable import MergeDFToTable
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
"""
load data
"""


df = spark.sql("""select * 
        from dsc_dws.dws_dsc_wh_ou_daily_kpi_sum""")

# df.show(15,False)

df = df.select("*").toPandas()

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
df['operation_day'] = df['operation_day'].apply(int)
df = df[df['operation_day'] >= 20210601] 


# clean_df1 = (df.groupby('ou_code')['operation_day'].count() < 2).reset_index()
# clean_df1.columns = ['ou_code', 'flag1']
# df = clean_df1.merge(df, on = 'ou_code', how = 'inner')
# df = df[df['flag1'] == False]

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
 
view = df[df['ou_code'] == 'CN-066'].sort_values('operation_day')
view['operation_day'].head(40)

"""
calculations functions def automate
""" 
# # from sklearn.metrics import davies_bouldin_scores
# daylen = int(df['operation_day'].nunique()/10)
# def mnb_kmeans_in(ou_code):
#         """
#         mini batch kmeans, inbound, outbound, working hour data.
#         simple algorithm, adding cols {max, min, mean, median, 75 quantile, distance to kernal}
#         Calculate Davies Bouldin score
#         from sklearn.metrics import davies_bouldin_score
#         null data fill
#         """

#         df_fin = pd.DataFrame()
#         df_sub = df[df['ou_code'] == ou_code][['ou_code', 'operation_day', 'inbound_receive_qty']]        
#         df_fin = df_fin.append(df_sub[df_sub['inbound_receive_qty'] == 0])
#         df_fin['kernal_core1' ] = -1
#         df_fin['kernal_value1'] = 0
#         df_rec = df_sub[df_sub['inbound_receive_qty'] != 0]      

#         """
#         auto choose kernel nums
#         """
#         scores = []
#         for center in list(range(2,daylen)):
#             kmeans = cluster.MiniBatchKMeans(n_clusters=center)
#             model  = kmeans.fit_predict(
#                         X = np.reshape(list(df_rec['inbound_receive_qty']), (-1,1))
#                         )
#             score  = davies_bouldin_score(
#                         np.reshape(list(df_rec['inbound_receive_qty']), (-1,1)), model
#                         )
#             scores.append(score)

#         center = np.argmin(scores)
#         print(center)
     
#         """
#         not null data training
#         """
    
#         alg1 = cluster.MiniBatchKMeans(n_clusters = center, random_state = 5290403)
#         hist1 = alg1.fit(np.reshape(list(df_rec['inbound_receive_qty']), (-1,1)))
#         df_rec['kernal_core1'] = hist1.labels_
#         cl_1 = pd.concat(
#                 [pd.DataFrame(hist1.cluster_centers_), pd.Series(np.arange(0,center))], axis = 1
#                 )
        
#         cl_1.columns = ['kernal_value1', 'kernal_core1']
#         df_rec = df_rec.merge(
#                 cl_1, on = 'kernal_core1', how = 'inner'
#                 )
        
#         """
#         merging
#         """
#         df_fin = df_fin.append(df_rec).reset_index().drop(['index'], axis = 1)

#         # df_fin['kind'] = 'inbound'

#         return df_fin



# def mnb_kmeans_out(ou_code):
        
#         df_fin = pd.DataFrame()
#         df_sub = df[df['ou_code'] == ou_code][['ou_code', 'operation_day', 'outbound_shipped_qty']]        
#         df_fin = df_fin.append(df_sub[df_sub['outbound_shipped_qty'] == 0])
#         df_fin['kernal_core2' ] = -1
#         df_fin['kernal_value2'] = 0
#         df_rec = df_sub[df_sub['outbound_shipped_qty'] != 0]

#         scores = []
#         for center in list(range(2,daylen)):
#             kmeans = cluster.MiniBatchKMeans(n_clusters=center)
#             model  = kmeans.fit_predict(
#                         X = np.reshape(list(df_rec['outbound_shipped_qty']), (-1,1))
#                         )
#             score  = davies_bouldin_score(
#                         np.reshape(list(df_rec['outbound_shipped_qty']), (-1,1)), model
#                         )
#             scores.append(score)

#         center = np.argmin(scores)
#         print(center)
#         alg1 = cluster.MiniBatchKMeans(n_clusters = center, random_state = 5290403)
#         hist1 = alg1.fit(np.reshape(list(df_rec['outbound_shipped_qty']), (-1,1)))

#         df_rec['kernal_core2'] = hist1.labels_
#         cl_1 = pd.concat(
#                 [pd.DataFrame(hist1.cluster_centers_), pd.Series(np.arange(0,center))], axis = 1
#                 )
        
#         cl_1.columns = ['kernal_value2', 'kernal_core2']

#         df_rec = df_rec.merge(
#                 cl_1, on = 'kernal_core2', how = 'inner'
#                 )
#         df_fin = df_fin.append(df_rec).reset_index().drop(['index'], axis = 1)
#         # df_fin['kind'] = 'outbound'
#         return df_fin

 
"""
calculations functions def 0
""" 
nc = 7
def mnb_kmeans_in(ou_code):
        """
        mini batch kmeans, inbound, outbound, working hour data.
        simple algorithm, adding cols {max, min, mean, median, 75 quantile, distance to kernal}
        """
        
        alg1 = cluster.MiniBatchKMeans(n_clusters = nc, random_state = 5290403)
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
                [pd.DataFrame(hist1.cluster_centers_), pd.Series(np.arange(0,nc))], axis = 1
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
        alg1 = cluster.MiniBatchKMeans(n_clusters = nc, random_state = 5290403)
        df_fin = pd.DataFrame()
        df_sub = df[df['ou_code'] == ou_code][['ou_code', 'operation_day', 'outbound_shipped_qty']]        
        df_fin = df_fin.append(df_sub[df_sub['outbound_shipped_qty'] == 0])
        df_fin['kernal_core2' ] = -1
        df_fin['kernal_value2'] = 0
        df_rec = df_sub[df_sub['outbound_shipped_qty'] != 0]

        hist1 = alg1.fit(np.reshape(list(df_rec['outbound_shipped_qty']), (-1,1)))

        df_rec['kernal_core2'] = hist1.labels_
        cl_1 = pd.concat(
                [pd.DataFrame(hist1.cluster_centers_), pd.Series(np.arange(0,nc))], axis = 1
                )
        
        cl_1.columns = ['kernal_value2', 'kernal_core2']

        df_rec = df_rec.merge(
                cl_1, on = 'kernal_core2', how = 'inner'
                )
        df_fin = df_fin.append(df_rec).reset_index().drop(['index'], axis = 1)
        # df_fin['kind'] = 'outbound'
        return df_fin

"""
calculations functions def 0
""" 

def mnb_kmeans_hr(ou_code):
        alg1 = cluster.MiniBatchKMeans(n_clusters = nc, random_state = 5290403)
        df_fin = pd.DataFrame()
        df_sub = df[df['ou_code'] == ou_code][['ou_code', 'operation_day', 'total_working_hour']]        
        df_fin = df_fin.append(df_sub[df_sub['total_working_hour'] == 0])
        df_fin['kernal_core3' ] = -1
        df_fin['kernal_value3'] = 0
        df_rec = df_sub[df_sub['total_working_hour'] != 0]

        hist1 = alg1.fit(np.reshape(list(df_rec['total_working_hour']), (-1,1)))

        df_rec['kernal_core3'] = hist1.labels_
        cl_1 = pd.concat(
                [pd.DataFrame(hist1.cluster_centers_), pd.Series(np.arange(0,nc))], axis = 1
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
                'kernal_core3'
                )['total_working_hour','kernal_value3'].diff(
                    axis = 1
                    ).drop('total_working_hour', axis = 1).round(3)
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
        mnb_kmeans_hr(i)
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
print("=================================calculate_1================================")
 
view = df_final[df_final['ou_code'] == 'CN-066'].sort_values('operation_day')
view['operation_day'].head(40)

"""
add outsource part
"""
def mnb_kmeans_hr2(ou_code):
        alg1 = cluster.MiniBatchKMeans(n_clusters = nc, random_state = 5290403)
        df_fin = pd.DataFrame()
        df_sub = df[df['ou_code'] == ou_code][['ou_code', 'operation_day', 'outsource_working_hour']]  
        df_fin = df_fin.append(df_sub[df_sub['outsource_working_hour'] == 0])
        df_fin['kernal_core4' ] = -1
        df_fin['kernal_value4'] = 0
        df_rec = df_sub[df_sub['outsource_working_hour'] != 0]
 
        hist1 = alg1.fit(np.reshape(list(df_rec['outsource_working_hour']), (-1,1)))

        df_rec['kernal_core4'] = hist1.labels_
        cl_1 = pd.concat(
                [pd.DataFrame(hist1.cluster_centers_), pd.Series(np.arange(0,nc))], axis = 1
                )
        
        cl_1.columns = ['kernal_value4', 'kernal_core4']

        df_rec = df_rec.merge(
                cl_1, on = 'kernal_core4', how = 'outer'
                )
        df_fin = df_fin.append(df_rec).reset_index().drop(['index'], axis = 1)
        df_fin['dis_core_os']=df_fin.groupby(
                'kernal_core4'
                )['outsource_working_hour','kernal_value4'].diff(
                        axis = 1
                        ).drop('outsource_working_hour', axis = 1).round(3)
        return df_fin


ou_codes
p = list()
for i in ou_codes:
    try: 
        # mnb_kmeans_in(i)
        # mnb_kmeans_out(i)
        # mnb_kmeans_hr(i)
        mnb_kmeans_hr2(i)
    except:
        
        p.append(i)


for i in p:
    ou_codes.remove(i)
 

df_final2 = pd.DataFrame()
for i in ou_codes:
        df_final2 = df_final2.append(
            mnb_kmeans_hr2(i)
        )

df_final = df_final.merge(df_final2, on =  ['ou_code', 'operation_day'], how = 'left').fillna(0)


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

df_final['flag_75_os'] =  df_final['outsource_working_hour'] -1.2*df_final['qt_75_os']
df_final['flag_75_os'] = [1 if a > 0 else 0 for a in df_final['flag_75_os']]


df_final['flag_75_wh'] =  df_final['total_working_hour'] -1.2*df_final['qt_75_wh']
df_final['flag_75_wh'] = [1 if a > 0 else 0 for a in df_final['flag_75_wh']]
# df_final['flag_75_wh'] = [1 if df_final['total_working_hour'][i]\
#     -1.2*np.abs(df_final['qt_75_wh'][i]) > 0
#     else 0 for i in np.arange(0, len(df_final))]
# df_final['flag_75_wh'] = [1 if df_final['dis_core_os'][i]>np.abs(df_final['qt_75_dis_core_os_outer'][i])\
#      else 0 for i in np.arange(0, len(df_final))]

print("=================================1================================")

df_final  = df_final.replace(float('inf'), 0) 



df_final0 = df_final[df_final['flag_75_wh'] == 0] 
df_final1 = df_final[df_final['flag_75_wh'] == 1] 
df_final0['dis_tt_kernel'] = 0
df_final1['dis_tt_kernel'] = np.abs(df_final1['total_working_hour']  - 1.2* df_final1['qt_75_wh'] )


df_final = pd.concat([df_final0, df_final1], axis = 0)

print("1.2 is the final flag boundary")

df_final0 = df_final[df_final['flag_75_os'] == 0] 
df_final1 = df_final[df_final['flag_75_os'] == 1] 
df_final0['dis_os_kernel'] = 0
df_final1['dis_os_kernel'] = np.abs(df_final1['outsource_working_hour']  - 1.2* df_final1['qt_75_os'] )


df_final = pd.concat([df_final0, df_final1], axis = 0)

print("1.2 is the os flag boundary")

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

