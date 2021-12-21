from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import subprocess
import shlex
import os


class MergeDFToTable:
    def __init__(self, table_name, df, pk_cols, order_cols, partition_cols=None):
        self.table_name = table_name
        self.src_df = df
        self.pk_cols = pk_cols
        self.order_cols = order_cols
        self.partition_cols = partition_cols
        app_name = "merge " + table_name
        self.spark = SparkSession.builder.appName(app_name).enableHiveSupport()\
            .config("hive.exec.dynamic.partition", "true")\
            .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()

    def merge(self):
        # get the table stored location
        sql = "desc formatted " + self.table_name
        table_meta_df = self.spark.sql(sql)
        table_meta_df.show(100,False)
        res_rows = table_meta_df.filter("col_name='Location' and data_type like 'hdfs:%'").collect()
        print(res_rows)
        tab_loc = res_rows[0]["data_type"]
        print(tab_loc)
        temp_loc = tab_loc.strip() + "_temp"
        print(temp_loc)
        # get the columns of table
        cols = self.spark.sql("select * from " + self.table_name + " where 1=0").columns
        select_cols_str = ",".join(cols)
        self.src_df.createOrReplaceTempView("inc_table")
        merge_sql =""
        if self.partition_cols is not None and self.partition_cols != "":
            # partition table
            # get partitions from incremental dataset
            sql = "select distinct " + self.partition_cols + " from inc_table"
            print(sql)
            inc_partition_df = self.spark.sql(sql)
            inc_partition_df.createOrReplaceTempView("inc_partitions")
            pt_cols = self.partition_cols.split(",")
            join_condition_str = ""
            for col in pt_cols:
                join_condition_str += " and p." + col.strip() + " = t." + col.strip()
            join_condition_str = join_condition_str[5:]
            merge_sql = """select """ + select_cols_str + """ from
            (
                select """ + select_cols_str + """ , row_number() over(partition by """ + self.pk_cols + """ 
                    order by """ + self.order_cols + """ desc) as rn  from
                (
                    select t.""" + ",t.".join(cols) + """ from inc_partitions p 
                    inner join """ + self.table_name + """ t on """ + join_condition_str + """
                    union all
                    select """ + select_cols_str + """ from inc_table
                ) a
            ) b where rn = 1
            """
        else:  # non-partitioned table
            merge_sql = """select """ + select_cols_str + """ from
            (
                select """ + select_cols_str + """ , row_number() over(partition by """ + self.pk_cols + """ 
                    order by """ + self.order_cols + """ desc) as rn  from
                (
                    select """ + select_cols_str + """ from  inc_table 
                    union all
                    select """ + select_cols_str + """ from """ + self.table_name + """
                ) a
            ) b where rn = 1
            """
        print(merge_sql)
        df = self.spark.sql(merge_sql)
        df.write.mode("overwrite").parquet(temp_loc)
        df = self.spark.read.parquet(temp_loc)
        print(df.columns)
        df.createOrReplaceTempView("merged_data")
        sql = """insert overwrite table """ + self.table_name 
        if self.partition_cols is not None and self.partition_cols != "":
            sql += """ partition(""" + self.partition_cols + """)""" 
        sql += """
                select """ + select_cols_str + """ from merged_data """
        print(sql)
        res = self.spark.sql(sql)
        res.show(1)
        # delete temp files
        cmd = "hdfs dfs -rm -r -f " + temp_loc
        args = shlex.split(cmd)
        new_env = os.environ.copy()
        p = subprocess.Popen(args, env=new_env)
        stdout, stderr = p.communicate()
        print("stdout:")
        print(stdout)
        print("stderr")
        print(stderr)





