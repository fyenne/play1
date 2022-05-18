#!/bin/bash
# pyspark python3 py2 
export PYSPARK_PYTHON="/app/anaconda3/bin/python"
export PYSPARK_DRIVER_PYTHON="/app/anaconda3/bin/python"
spark-submit --master yarn --deploy-mode client --queue root.dsc ./kmeans_qty_wkhour/kmeans_linear_reg.py  --start_date ${start_date} --end_date ${end_date} --outb_para ${outb_para} --std_para ${std_para} --std_para2 ${std_para2}