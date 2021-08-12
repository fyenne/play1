#!/bin/bash
# pyspark python3 py2 
export PYSPARK_PYTHON="/app/anaconda3/bin/python"
export PYSPARK_DRIVER_PYTHON="/app/anaconda3/bin/python"
spark-submit --master yarn --deploy-mode client --driver-memory 8G --executor-memory 8G --queue root.dsc ./dws_dsc_wh_ou_kpi_sum/dws_dsc_wh_ou_monthly_kpi_sum.py --start_month "${start_month}" --end_month "${end_month}"
