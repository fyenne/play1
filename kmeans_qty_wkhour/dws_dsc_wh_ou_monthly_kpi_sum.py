from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import argparse
from MergeDataFrameToTable import MergeDFToTable


def run_etl(start_month, end_month):
    spark = SparkSession.builder.appName("dws_dsc_wh_ou_monthly_kpi_sum").enableHiveSupport().getOrCreate()
    # ou mapping
    sql = """select lower(wms_warehouse_id) as wms_warehouse_id
                , lower(coalesce(company_id,'')) as company_id
                , max(ou_code) as ou_code
           from dsc_dim.dim_dsc_ou_whse_rel
           group by lower(wms_warehouse_id)
           , lower(coalesce(company_id,''))"""
    print(sql)
    ou_mapping_df = spark.sql(sql)
    ou_mapping_df.cache()
    ou_mapping_df.createOrReplaceTempView("t2")

    # inbound order
    sql = """
        select t1.inc_day, t2.ou_code
        , count(distinct t1.asn_id) as inbound_header_cnt 
        from (select asn_id
            , lower(coalesce(wms_warehouse_id, wms_warehouse_name)) as wms_warehouse_id2
            , lower(coalesce(wms_company_id,'')) as wms_company_id2
            , cast(substr(inc_day,1,6) as int) as inc_day
            from dsc_dwd.dwd_wh_dsc_inbound_header_dtl_di
            where substr(inc_day,1,6) between '""" + start_month + """' and '""" + end_month + """'
        ) t1
        join t2
        on t1.wms_warehouse_id2 = lower(t2.wms_warehouse_id)
        and t1.wms_company_id2 = lower(t2.company_id)
        group by t2.ou_code,inc_day
    """
    print(sql)
    inbound_order_df = spark.sql(sql)
    inbound_order_df.createOrReplaceTempView("inbound_order")

    # inbound order line
    sql = """
        select t1.inc_day, t2.ou_code
        , count(distinct concat(t1.asn_id,'|',t1.asn_line_id)) as inbound_line_cnt
        , sum(t1.original_qty) as inbound_original_qty
        , sum(t1.receive_qty) as inbound_receive_qty
        , sum(t1.receive_volume) as inbound_receive_volume
        , sum(t1.receive_weight) as inbound_receive_weight
        , sum(t1.std_receive_volume) as inbound_std_receive_volume
        , sum(t1.std_receive_weight) as inbound_std_receive_weight
        from (select asn_id, asn_line_id
            , original_qty
            , receive_qty
            , receive_volume 
            , receive_weight
            , std_receive_volume
            , std_receive_weight
            , cast(substr(inc_day,1,6) as int) as inc_day
            , lower(coalesce(wms_warehouse_id, wms_warehouse_name)) as wms_warehouse_id2
            , lower(coalesce(wms_company_id,'')) as wms_company_id2
              from dsc_dwd.dwd_wh_dsc_inbound_line_dtl_di
              where substr(inc_day,1,6) between '""" + start_month + """' and '""" + end_month + """'
              ) as t1
        join t2
        on t1.wms_warehouse_id2 = t2.wms_warehouse_id
        and t1.wms_company_id2 = t2.company_id
        group by t2.ou_code,t1.inc_day    
    """
    print(sql)
    inbound_line_df = spark.sql(sql)
    inbound_line_df.createOrReplaceTempView("inbound_order_line")

    # outbound order
    sql = """
    select t1.inc_day, t2.ou_code
    , count(distinct t1.order_id) as outbound_order_cnt
    from (select order_id
            , lower(coalesce(wms_warehouse_id, wms_warehouse_name)) as wms_warehouse_id2
            , lower(coalesce(wms_company_id,'')) as wms_company_id2
            , cast(substr(inc_day,1,6) as int) as inc_day
              from dsc_dwd.dwd_wh_dsc_outbound_header_dtl_di
              where substr(inc_day,1,6) between '""" + start_month + """' and '""" + end_month + """'
      ) as t1
    join t2
    on  t1.wms_warehouse_id2 = t2.wms_warehouse_id
        and t1.wms_company_id2 = t2.company_id
    group by t2.ou_code,t1.inc_day
    """
    print(sql)
    outbound_order_df = spark.sql(sql)
    outbound_order_df.createOrReplaceTempView("outbound_order")

    # outbound order line
    sql = """
        select t1.inc_day,t2.ou_code
        , count(distinct concat(order_id,'|',t1.order_line)) as outbound_line_cnt
        , sum(t1.original_order_qty) as outbound_original_order_qty
        , sum(t1.shipped_qty) as outbound_shipped_qty
        , sum(t1.shipped_volume) as outbound_shipped_volume
        , sum(t1.shipped_weight) as outbound_shipped_weight
        , sum(t1.std_shipped_volume) as outbound_std_shipped_volume
        , sum(t1.std_shipped_weight) as outbound_std_shipped_weight
        from (select order_id, order_line, original_order_qty, shipped_qty, shipped_volume, shipped_weight
            ,std_shipped_volume
            ,std_shipped_weight
            ,lower(coalesce(wms_warehouse_id, wms_warehouse_name)) as wms_warehouse_id2
            ,lower(coalesce(wms_company_id,'')) as wms_company_id2
            ,cast(substr(inc_day,1,6) as int) as inc_day
            from dsc_dwd.dwd_wh_dsc_outbound_line_dtl_di
            where substr(inc_day,1,6) between '""" + start_month + """' and '""" + end_month + """'
        ) as t1
        join t2
        on  t1.wms_warehouse_id2 = t2.wms_warehouse_id
          and t1.wms_company_id2 = t2.company_id
        group by t2.ou_code,t1.inc_day
       """
    print(sql)
    outbound_order_line_df = spark.sql(sql)
    outbound_order_line_df.createOrReplaceTempView("outbound_order_line")

    # inventory only used last day of each month
    sql = """
        select t1.inc_day,t2.ou_code
        ,count(distinct t1.location) as last_used_storage_location_count
        ,count(distinct lpn) as last_active_lpn_count
        ,sum(t1.on_hand_qty) as last_on_hand_qty
        ,sum(t1.in_transit_qty) as last_in_transit_qty
        ,sum(allocated_qty) as last_allocated_qty
        from (select cast(substr(inc_day,1,6) as int) as inc_day,location,lpn,on_hand_qty,in_transit_qty,allocated_qty
                ,lower(coalesce(wms_warehouse_id, wms_warehouse_name)) as wms_warehouse_id2
                ,lower(coalesce(wms_company_id,'')) as wms_company_id2
                from dsc_dwd.dwd_wh_dsc_inventory_dtl_di
                where inc_day in ( select distinct month_end_date from dsc_dim.dim_dsc_date_info           
                        where `month` between """ + start_month + """ and """ + end_month + """) 
            ) as t1
        join t2
        on t1.wms_warehouse_id2 = t2.wms_warehouse_id
        and t1.wms_company_id2 = t2.company_id
        group by t2.ou_code,t1.inc_day
    """
    print(sql)
    inventory_df = spark.sql(sql)
    inventory_df.createOrReplaceTempView("inventory")

    # average inventory of each month
    sql = """
        select t1.inc_day ,t2.ou_code
        ,t1.avg_used_storage_location_count
        ,t1.avg_active_lpn_count
        ,t1.avg_on_hand_qty
        ,t1.avg_in_transit_qty
        ,t1.avg_allocated_qty
        from (select wms_warehouse_id2, wms_company_id2, cast(substr(f.inc_day,1,6) as int) as inc_day
              ,sum(location_cnt)/count(f.inc_day) as avg_used_storage_location_count
              ,sum(lpn_cnt)/count(f.inc_day) as avg_active_lpn_count 
              ,sum(on_hand_qty)/count(f.inc_day) as avg_on_hand_qty
              ,sum(in_transit_qty)/count(f.inc_day) as avg_in_transit_qty
              ,sum(allocated_qty)/count(f.inc_day) as avg_allocated_qty
              from
                (select                 
                lower(coalesce(wms_warehouse_id, wms_warehouse_name)) as wms_warehouse_id2
                ,lower(coalesce(wms_company_id,'')) as wms_company_id2
                ,inc_day
                ,count(distinct location) as location_cnt
                ,count(distinct lpn) as lpn_cnt
                ,sum(on_hand_qty) as on_hand_qty
                ,sum(in_transit_qty) as in_transit_qty
                ,sum(allocated_qty) as allocated_qty
                from dsc_dwd.dwd_wh_dsc_inventory_dtl_di
                where substr(inc_day,1,6) between """ + start_month + """ and """ + end_month + """ 
                group by lower(coalesce(wms_warehouse_id, wms_warehouse_name))
                ,lower(coalesce(wms_company_id,''))
                ,inc_day
                ) f
                group by wms_warehouse_id2, wms_company_id2, cast(substr(f.inc_day,1,6) as int)
            ) as t1
        join t2
        on t1.wms_warehouse_id2 = t2.wms_warehouse_id
        and t1.wms_company_id2 = t2.company_id
    """
    print(sql)
    inventory_df = spark.sql(sql)
    inventory_df.createOrReplaceTempView("inventory_avg")



    # human resource
    sql = """
         select cast(substr(w.inc_day,1,6) as int) as inc_day, ou_code
        ,count(emp_code)/count(distinct substr(w.working_date,1,10)) as total_head_count
        ,sum(w.working_hours) as total_working_hours
        ,sum(case when lower(w.emp_type) = 'perm' then w.working_hours else 0 end) as perm_working_hours
        ,sum(case when lower(w.emp_type) = 'outsource' then w.working_hours else 0 end) as outsource_working_hours
        ,sum(case when lower(w.emp_type) not in ('perm','outsource') then w.working_hours else 0 end) as other_working_hours
        ,sum(case when lower(s.direct) = 'direct' then w.working_hours else 0 end) as direct_working_hour
        ,sum(case when lower(coalesce(s.direct,'indirect')) = 'indirect' then w.working_hours else 0 end) as indirect_working_hour
        from dsc_dwd.dwd_hr_dsc_working_hour_dtl_di w
        left join dsc_dim.dim_dsc_staff_info s
        on w.emp_code = s.staff_id
        where substr(inc_day,1,6) between '""" + start_month + """' and '""" + end_month + """'
        and ou_code <> ''
        and working_hours > 0 
        group by ou_code, substr(inc_day,1,6)
    """
    print(sql)
    hr_df = spark.sql(sql)
    hr_df.createOrReplaceTempView("hr")

    # billing
    sql = """
            SELECT cc.ou_code,cast(substr(b.bms_bill_end_date,1,6) as int) as  inc_day
            ,sum(b.bms_amount) as bms_bill_amount_exclude_tax
            ,sum(case when tp.category ='报关代理' then b.bms_amount else 0 end) as customs_agent_fee
            ,sum(case when tp.category ='操作' then b.bms_amount else 0 end) as operation_fee
            ,sum(case when tp.category ='场地' then b.bms_amount else 0 end) as warehouse_site_fee
            ,sum(case when tp.category ='短驳运输费' then b.bms_amount else 0 end) as short_barge_fee
            ,sum(case when tp.category ='人力' then b.bms_amount else 0 end) as labor_fee
            ,sum(case when tp.category ='设备使用' then b.bms_amount else 0 end) as equipment_usage_fee
            ,sum(case when tp.category ='增值' then b.bms_amount else 0 end) as value_added_fee
            FROM dsc_dwd.dwd_fact_warehouse_billing_detail_dtl b
            LEFT JOIN 
            dsc_dim.dim_dsc_billing_type_info tp
            ON b.bms_fee_type = tp.billing_fee_type_code
            LEFT JOIN dsc_dim.dim_dsc_ou_cc_rel cc
            ON b.cost_center = cc.cost_center_code
            WHERE substr(b.bms_bill_end_date,1,6) BETWEEN '""" + start_month + """' and '""" + end_month + """'
            GROUP BY
            cc.ou_code, substr(bms_bill_end_date,1,6)
     """
    print(sql)
    bill_df = spark.sql(sql)
    bill_df.createOrReplaceTempView("bill")

    # combine all data
    sql = """
    SELECT 
    ou_code
    ,`month`
    ,max(inbound_header_count) as inbound_header_count
    ,max(inbound_line_count) as inbound_line_count
    ,max(inbound_line_original_qty) as inbound_line_original_qty
    ,max(inbound_receive_qty) as inbound_receive_qty
    ,max(inbound_receive_volume) as inbound_receive_volume
    ,max(inbound_receive_weight) as inbound_receive_weight
    ,max(inbound_std_receive_volume) as inbound_std_receive_volume
    ,max(inbound_std_receive_weight) as inbound_std_receive_weight
    ,max(outbound_header_count) as outbound_header_count
    ,max(outbound_line_count) as outbound_line_count
    ,max(outbound_line_original_qty) as outbound_line_original_qty
    ,max(outbound_shipped_qty) as outbound_shipped_qty
    ,max(outbound_shipped_volume) as outbound_shipped_volume
    ,max(outbound_shipped_weight) as outbound_shipped_weight
    ,max(outbound_std_shipped_volume) as outbound_std_shipped_volume
    ,max(outbound_std_shipped_weight) as outbound_std_shipped_weight
    ,max(last_used_storage_location_count) as last_used_storage_location_count
    ,max(last_active_lpn_count) as last_active_lpn_count
    ,max(last_on_hand_qty) as last_on_hand_qty
    ,max(last_in_transit_qty) as last_in_transit_qty
    ,max(last_allocated_qty) as last_allocated_qty
    ,max(avg_used_storage_location_count) as avg_used_storage_location_count
    ,max(avg_active_lpn_count) as avg_active_lpn_count
    ,max(avg_on_hand_qty) as avg_on_hand_qty
    ,max(avg_in_transit_qty) as avg_in_transit_qty
    ,max(avg_allocated_qty) as avg_allocated_qty
    ,max(total_head_count) as total_head_count
    ,max(total_working_hour) as total_working_hour
    ,max(outsource_working_hour) as outsource_working_hour
    ,max(perm_working_hour) as perm_working_hour
    ,max(other_working_hour) as other_working_hour
    ,max(direct_working_hour) as direct_working_hour
    ,max(indirect_working_hour) as indirect_working_hour
    ,max(bms_bill_amount_exclude_tax) as bms_bill_amount_exclude_tax
    ,max(customs_agent_fee) as customs_agent_fee
    ,max(operation_fee) as operation_fee
    ,max(warehouse_site_fee) as warehouse_site_fee
    ,max(short_barge_fee) as short_barge_fee
    ,max(labor_fee) as labor_fee
    ,max(equipment_usage_fee) as equipment_usage_fee
    ,max(value_added_fee) as value_added_fee
    FROM 
    (
    SELECT 
        ou_code
        ,inc_day AS `month`
        ,inbound_header_cnt AS inbound_header_count
        ,NULL AS inbound_line_count
        ,NULL AS inbound_line_original_qty
        ,NULL AS inbound_receive_qty
        ,NULL AS inbound_receive_volume
        ,NULL AS inbound_receive_weight
        ,NULL AS inbound_std_receive_volume
        ,NULL AS inbound_std_receive_weight
        ,NULL AS outbound_header_count
        ,NULL AS outbound_line_count
        ,NULL AS outbound_line_original_qty
        ,NULL AS outbound_shipped_qty
        ,NULL AS outbound_shipped_volume
        ,NULL AS outbound_shipped_weight
        ,NULL AS outbound_std_shipped_volume
        ,NULL AS outbound_std_shipped_weight
        ,NULL AS last_used_storage_location_count
        ,NULL AS last_active_lpn_count
        ,NULL AS last_on_hand_qty
        ,NULL AS last_in_transit_qty
        ,NULL AS last_allocated_qty
        ,NULL as  avg_used_storage_location_count
        ,NULL as  avg_active_lpn_count
        ,NULL as  avg_on_hand_qty
        ,NULL as  avg_in_transit_qty
        ,NULL as  avg_allocated_qty
        ,NULL AS total_head_count
        ,NULL AS total_working_hour
        ,NULL AS outsource_working_hour
        ,NULL AS perm_working_hour
        ,NULL AS other_working_hour
        ,NULL as direct_working_hour
        ,NULL as indirect_working_hour
        ,NULL AS bms_bill_amount_exclude_tax
        ,NULL AS customs_agent_fee
        ,NULL AS operation_fee
        ,NULL AS warehouse_site_fee
        ,NULL AS short_barge_fee
        ,NULL AS labor_fee
        ,NULL AS equipment_usage_fee
        ,NULL AS value_added_fee
        FROM inbound_order

        UNION ALL
        
        SELECT
        ou_code
        ,inc_day AS `month`
        ,NULL AS inbound_header_count
        ,inbound_line_cnt AS inbound_line_count
        ,inbound_original_qty AS inbound_line_original_qty
        ,inbound_receive_qty
        ,inbound_receive_volume
        ,inbound_receive_weight
        ,inbound_std_receive_volume
        ,inbound_std_receive_weight
        ,NULL AS outbound_header_count
        ,NULL AS outbound_line_count
        ,NULL AS outbound_line_original_qty
        ,NULL AS outbound_shipped_qty
        ,NULL AS outbound_shipped_volume
        ,NULL AS outbound_shipped_weight
        ,NULL AS outbound_std_shipped_volume
        ,NULL AS outbound_std_shipped_weight
        ,NULL AS last_used_storage_location_count
        ,NULL AS last_active_lpn_count
        ,NULL AS last_on_hand_qty
        ,NULL AS last_in_transit_qty
        ,NULL AS last_allocated_qty
        ,NULL as  avg_used_storage_location_count
        ,NULL as  avg_active_lpn_count
        ,NULL as  avg_on_hand_qty
        ,NULL as  avg_in_transit_qty
        ,NULL as  avg_allocated_qty
        ,NULL AS total_head_count
        ,NULL AS total_working_hour
        ,NULL AS outsource_working_hour
        ,NULL AS perm_working_hour
        ,NULL AS other_working_hour
        ,NULL as direct_working_hour
        ,NULL as indirect_working_hour
        ,NULL AS bms_bill_amount_exclude_tax
        ,NULL AS customs_agent_fee
        ,NULL AS operation_fee
        ,NULL AS warehouse_site_fee
        ,NULL AS short_barge_fee
        ,NULL AS labor_fee
        ,NULL AS equipment_usage_fee
        ,NULL AS value_added_fee
        FROM inbound_order_line
        
        UNION ALL
        
        SELECT
        ou_code
        ,inc_day AS `month`
        ,NULL AS inbound_header_count
        ,NULL AS inbound_line_count
        ,NULL AS inbound_line_original_qty
        ,NULL AS inbound_receive_qty
        ,NULL AS inbound_receive_volume
        ,NULL AS inbound_receive_weight
        ,NULL AS inbound_std_receive_volume
        ,NULL AS inbound_std_receive_weight
        ,outbound_order_cnt AS outbound_header_count
        ,NULL AS outbound_line_count
        ,NULL AS outbound_line_original_qty
        ,NULL AS outbound_shipped_qty
        ,NULL AS outbound_shipped_volume
        ,NULL AS outbound_shipped_weight
        ,NULL AS outbound_std_shipped_volume
        ,NULL AS outbound_std_shipped_weight
        ,NULL AS last_used_storage_location_count
        ,NULL AS last_active_lpn_count
        ,NULL AS last_on_hand_qty
        ,NULL AS last_in_transit_qty
        ,NULL AS last_allocated_qty
        ,NULL as  avg_used_storage_location_count
        ,NULL as  avg_active_lpn_count
        ,NULL as  avg_on_hand_qty
        ,NULL as  avg_in_transit_qty
        ,NULL as  avg_allocated_qty
        ,NULL AS total_head_count
        ,NULL AS total_working_hour
        ,NULL AS outsource_working_hour
        ,NULL AS perm_working_hour
        ,NULL AS other_working_hour
        ,NULL as direct_working_hour
        ,NULL as indirect_working_hour
        ,NULL AS bms_bill_amount_exclude_tax
        ,NULL AS customs_agent_fee
        ,NULL AS operation_fee
        ,NULL AS warehouse_site_fee
        ,NULL AS short_barge_fee
        ,NULL AS labor_fee
        ,NULL AS equipment_usage_fee
        ,NULL AS value_added_fee
        FROM outbound_order
        
        UNION ALL
        
        SELECT
        ou_code
        ,inc_day AS `month`
        ,NULL AS inbound_header_count
        ,NULL AS inbound_line_count
        ,NULL AS inbound_line_original_qty
        ,NULL AS inbound_receive_qty
        ,NULL AS inbound_receive_volume
        ,NULL AS inbound_receive_weight
        ,NULL AS inbound_std_receive_volume
        ,NULL AS inbound_std_receive_weight
        ,NULL AS outbound_header_count
        ,outbound_line_cnt AS outbound_line_count
        ,outbound_original_order_qty AS outbound_line_original_qty
        ,outbound_shipped_qty
        ,outbound_shipped_volume AS outbound_shipped_volume
        ,outbound_shipped_weight AS outbound_shipped_weight
        ,outbound_std_shipped_volume AS outbound_std_shipped_volume
        ,outbound_std_shipped_weight AS outbound_std_shipped_weight
        ,NULL AS last_used_storage_location_count
        ,NULL AS last_active_lpn_count
        ,NULL AS last_on_hand_qty
        ,NULL AS last_in_transit_qty
        ,NULL AS last_allocated_qty
        ,NULL as  avg_used_storage_location_count
        ,NULL as  avg_active_lpn_count
        ,NULL as  avg_on_hand_qty
        ,NULL as  avg_in_transit_qty
        ,NULL as  avg_allocated_qty
        ,NULL AS total_head_count
        ,NULL AS total_working_hour
        ,NULL AS outsource_working_hour
        ,NULL AS perm_working_hour
        ,NULL AS other_working_hour
        ,NULL as direct_working_hour
        ,NULL as indirect_working_hour
        ,NULL AS bms_bill_amount_exclude_tax
        ,NULL AS customs_agent_fee
        ,NULL AS operation_fee
        ,NULL AS warehouse_site_fee
        ,NULL AS short_barge_fee
        ,NULL AS labor_fee
        ,NULL AS equipment_usage_fee
        ,NULL AS value_added_fee
        FROM outbound_order_line
        
        UNION ALL
        
        SELECT
        ou_code
        ,inc_day AS `month`
        ,NULL AS inbound_header_count
        ,NULL AS inbound_line_count
        ,NULL AS inbound_line_original_qty
        ,NULL AS inbound_receive_qty
        ,NULL AS inbound_receive_volume
        ,NULL AS inbound_receive_weight
        ,NULL AS inbound_std_receive_volume
        ,NULL AS inbound_std_receive_weight
        ,NULL AS outbound_header_count
        ,NULL AS outbound_line_count
        ,NULL AS outbound_line_original_qty
        ,NULL AS outbound_shipped_qty
        ,NULL AS outbound_shipped_volume
        ,NULL AS outbound_shipped_weight
        ,NULL AS outbound_std_shipped_volume
        ,NULL AS outbound_std_shipped_weight
        ,last_used_storage_location_count
        ,last_active_lpn_count
        ,last_on_hand_qty
        ,last_in_transit_qty
        ,last_allocated_qty
        ,NULL as  avg_used_storage_location_count
        ,NULL as  avg_active_lpn_count
        ,NULL as  avg_on_hand_qty
        ,NULL as  avg_in_transit_qty
        ,NULL as  avg_allocated_qty
        ,NULL AS total_head_count
        ,NULL AS total_working_hour
        ,NULL AS outsource_working_hour
        ,NULL AS perm_working_hour
        ,NULL AS other_working_hour
        ,NULL as direct_working_hour
        ,NULL as indirect_working_hour
        ,NULL AS bms_bill_amount_exclude_tax
        ,NULL AS customs_agent_fee
        ,NULL AS operation_fee
        ,NULL AS warehouse_site_fee
        ,NULL AS short_barge_fee
        ,NULL AS labor_fee
        ,NULL AS equipment_usage_fee
        ,NULL AS value_added_fee
        FROM inventory
        
        UNION ALL
        
        SELECT
        ou_code
        ,inc_day AS `month`
        ,NULL AS inbound_header_count
        ,NULL AS inbound_line_count
        ,NULL AS inbound_line_original_qty
        ,NULL AS inbound_receive_qty
        ,NULL AS inbound_receive_volume
        ,NULL AS inbound_receive_weight
        ,NULL AS inbound_std_receive_volume
        ,NULL AS inbound_std_receive_weight
        ,NULL AS outbound_header_count
        ,NULL AS outbound_line_count
        ,NULL AS outbound_line_original_qty
        ,NULL AS outbound_shipped_qty
        ,NULL AS outbound_shipped_volume
        ,NULL AS outbound_shipped_weight
        ,NULL AS outbound_std_shipped_volume
        ,NULL AS outbound_std_shipped_weight
        ,NULL AS last_used_storage_location_count
        ,NULL AS last_active_lpn_count
        ,NULL AS last_on_hand_qty
        ,NULL AS last_in_transit_qty
        ,NULL AS last_allocated_qty
        ,avg_used_storage_location_count
        ,avg_active_lpn_count 
        ,avg_on_hand_qty
        ,avg_in_transit_qty
        ,avg_allocated_qty        
        ,NULL AS total_head_count
        ,NULL AS total_working_hour
        ,NULL AS outsource_working_hour
        ,NULL AS perm_working_hour
        ,NULL AS other_working_hour
        ,NULL as direct_working_hour
        ,NULL as indirect_working_hour
        ,NULL AS bms_bill_amount_exclude_tax
        ,NULL AS customs_agent_fee
        ,NULL AS operation_fee
        ,NULL AS warehouse_site_fee
        ,NULL AS short_barge_fee
        ,NULL AS labor_fee
        ,NULL AS equipment_usage_fee
        ,NULL AS value_added_fee
        FROM inventory_avg       
        UNION ALL
        
        SELECT
        ou_code
        ,inc_day AS `month`
        ,NULL AS inbound_header_count
        ,NULL AS inbound_line_count
        ,NULL AS inbound_line_original_qty
        ,NULL AS inbound_receive_qty
        ,NULL AS inbound_receive_volume
        ,NULL AS inbound_receive_weight
        ,NULL AS inbound_std_receive_volume
        ,NULL AS inbound_std_receive_weight
        ,NULL AS outbound_header_count
        ,NULL AS outbound_line_count
        ,NULL AS outbound_line_original_qty
        ,NULL AS outbound_shipped_qty
        ,NULL AS outbound_shipped_volume
        ,NULL AS outbound_shipped_weight
        ,NULL AS outbound_std_shipped_volume
        ,NULL AS outbound_std_shipped_weight
        ,NULL AS last_used_storage_location_count
        ,NULL AS last_active_lpn_count
        ,NULL AS last_on_hand_qty
        ,NULL AS last_in_transit_qty
        ,NULL AS last_allocated_qty
        ,NULL as  avg_used_storage_location_count
        ,NULL as  avg_active_lpn_count
        ,NULL as  avg_on_hand_qty
        ,NULL as  avg_in_transit_qty
        ,NULL as  avg_allocated_qty
        ,total_head_count
        ,total_working_hours AS total_working_hour
        ,outsource_working_hours AS outsource_working_hour
        ,perm_working_hours  as perm_working_hour
        ,other_working_hours as other_working_hour
        ,direct_working_hour
        ,indirect_working_hour
        ,NULL AS bms_bill_amount_exclude_tax
        ,NULL AS customs_agent_fee
        ,NULL AS operation_fee
        ,NULL AS warehouse_site_fee
        ,NULL AS short_barge_fee
        ,NULL AS labor_fee
        ,NULL AS equipment_usage_fee
        ,NULL AS value_added_fee
        FROM hr
        
        UNION ALL
        
        SELECT
        ou_code
        ,inc_day AS `month`
        ,NULL AS inbound_header_count
        ,NULL AS inbound_line_count
        ,NULL AS inbound_line_original_qty
        ,NULL AS inbound_receive_qty
        ,NULL AS inbound_receive_volume
        ,NULL AS inbound_receive_weight
        ,NULL AS inbound_std_receive_volume
        ,NULL AS inbound_std_receive_weight
        ,NULL AS outbound_header_count
        ,NULL AS outbound_line_count
        ,NULL AS outbound_line_original_qty
        ,NULL AS outbound_shipped_qty
        ,NULL AS outbound_shipped_volume
        ,NULL AS outbound_shipped_weight
        ,NULL AS outbound_std_shipped_volume
        ,NULL AS outbound_std_shipped_weight
        ,NULL AS last_used_storage_location_count
        ,NULL AS last_active_lpn_count
        ,NULL AS last_on_hand_qty
        ,NULL AS last_in_transit_qty
        ,NULL AS last_allocated_qty
        ,NULL as  avg_used_storage_location_count
        ,NULL as  avg_active_lpn_count
        ,NULL as  avg_on_hand_qty
        ,NULL as  avg_in_transit_qty
        ,NULL as  avg_allocated_qty
        ,NULL AS total_head_count
        ,NULL AS total_working_hour
        ,NULL AS outsource_working_hour
        ,NULL AS perm_working_hour
        ,NULL AS other_working_hour
        ,NULL as direct_working_hour
        ,NULL as indirect_working_hour
        ,bms_bill_amount_exclude_tax
        ,customs_agent_fee
        ,operation_fee
        ,warehouse_site_fee
        ,short_barge_fee
        ,labor_fee
        ,equipment_usage_fee
        ,value_added_fee
        FROM bill
        ) total
    GROUP BY 
    ou_code, `month`
    """
    print(sql)
    comb_df = spark.sql(sql)
    comb_df.createOrReplaceTempView("combined_data")

    #warehouse location count
    sql = """select r.ou_code, l.location_cnt as total_storage_location_count
        from
        (
        select warehouse_id,count(*) as location_cnt from dsc_dim.dim_dsc_storage_location_info 
        group by warehouse_id
        ) l
        JOIN (select ou_code,wms_warehouse_id from dsc_dim.dim_dsc_ou_whse_rel group by ou_code,wms_warehouse_id ) r
        ON l.warehouse_id = r.wms_warehouse_id
    """
    print(sql)
    location_df = spark.sql(sql)
    location_df.createOrReplaceTempView("location")

    # hr cost
    sql = """select ou_code,term as `month`
        ,sum(coalesce(staff_cost_fixed,0) + coalesce(staff_cost_vari,0)) as perm_labor_cost
        ,sum(coalesce(staff_cost_os_fixed,0) + coalesce(staff_cost_os_vari,0)) as outsource_labor_cost
        ,sum(staff_cost_bizos) as labor_cost_for_business_outsource
        ,sum(coalesce(staff_cost_fixed,0) + coalesce(staff_cost_vari,0) + coalesce(staff_cost_os_fixed,0) 
        + coalesce(staff_cost_os_vari,0)) as total_labor_cost
        FROM dsc_dwd.dwd_dsc_sc_master_dtl
        WHERE term between '""" + start_month + """' and '""" + end_month + """'
        GROUP BY ou_code,term
        """
    print(sql)
    hr_cost_df = spark.sql(sql)
    hr_cost_df.createOrReplaceTempView("hr_cost")

    # calculated measure
    sql = """
    SELECT 
    cd.ou_code
    ,ou.ou_name
    ,bg.bg_code
    ,bg.bg_name_cn
    ,ou.customer_id
    ,ou.customer_name
    ,loc.total_storage_location_count
    ,cd.month
    ,cast(substr(cast(cd.month as string),1,4) as int)*100 
        + ceiling(cast(substr(cast(cd.month as string),5,2) as int)/3) as `quarter`
    ,cast(substr(cast(cd.month as string),1,4) as int) as `year`
    ,cd.inbound_header_count
    ,cd.inbound_line_count
    ,cd.inbound_line_original_qty
    ,cd.inbound_receive_qty
    ,cd.inbound_receive_volume
    ,cd.inbound_receive_weight
    ,cd.outbound_header_count
    ,cd.outbound_line_count
    ,cd.outbound_line_original_qty
    ,cd.outbound_shipped_qty
    ,cd.outbound_shipped_volume
    ,cd.outbound_shipped_weight
    ,cd.last_used_storage_location_count
    ,cd.last_active_lpn_count
    ,cd.last_on_hand_qty
    ,cd.last_in_transit_qty
    ,cd.last_allocated_qty
    ,cd.avg_used_storage_location_count
    ,cd.avg_active_lpn_count
    ,cd.avg_on_hand_qty
    ,cd.avg_in_transit_qty
    ,cd.avg_allocated_qty
    ,cd.total_head_count
    ,cd.total_working_hour
    ,cd.outsource_working_hour
    ,cd.perm_working_hour
    ,cd.other_working_hour
    ,cd.direct_working_hour
    ,cd.indirect_working_hour
    ,hc.total_labor_cost
    ,hc.outsource_labor_cost
    ,hc.labor_cost_for_business_outsource
    ,hc.perm_labor_cost
    ,cd.bms_bill_amount_exclude_tax
    ,cd.customs_agent_fee
    ,cd.operation_fee
    ,cd.warehouse_site_fee
    ,cd.short_barge_fee
    ,cd.labor_fee
    ,cd.equipment_usage_fee
    ,cd.value_added_fee
    ,cd.inbound_std_receive_volume
    ,cd.inbound_std_receive_weight
    ,cd.outbound_std_shipped_volume
    ,cd.outbound_std_shipped_weight
    ,coalesce(cd.outbound_shipped_qty,0)/cd.inbound_receive_qty as outbound_Inbound_qty_ratio
    ,cd.perm_working_hour/cd.total_working_hour as perm_working_hour_ratio
    ,cd.total_working_hour/cd.total_head_count as working_hour_per_head
    ,hc.total_labor_cost/cd.total_working_hour as average_labor_cost_per_hour
    ,hc.outsource_labor_cost/cd.outsource_working_hour as average_labor_cost_per_hour_outsourcing
    ,hc.perm_labor_cost/cd.perm_working_hour as average_labor_cost_per_hour_perm
    ,cd.avg_used_storage_location_count/loc.total_storage_location_count as location_usage_rate
    ,1 - (cd.avg_used_storage_location_count/loc.total_storage_location_count) as location_idle_rate
    ,coalesce(cd.operation_fee,0.0) + coalesce(cd.labor_fee,0.0) as total_handling_income
    ,(coalesce(cd.operation_fee,0.0) + coalesce(cd.labor_fee,0.0))/cd.bms_bill_amount_exclude_tax as total_handling_income_ratio
    ,cd.warehouse_site_fee/cd.bms_bill_amount_exclude_tax as warehouse_site_fee_ratio
    ,cd.value_added_fee/cd.bms_bill_amount_exclude_tax as value_added_fee_ratio
    ,(coalesce(cd.operation_fee,0.0) + coalesce(cd.labor_fee,0.0))/cd.total_working_hour as labor_productivity
    ,NULL as estimated_working_hour
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd hh:mm:ss') as update_time
    FROM combined_data cd
    LEFT JOIN  dsc_dim.dim_dsc_ou_info ou
    ON cd.ou_code = ou.ou_code
    LEFT JOIN  dsc_dim.dim_dsc_business_group_info bg
    ON ou.bg_code = bg.bg_code
    LEFT JOIN hr_cost hc
    ON cd.ou_code = hc.ou_code
    AND cd.`month` = hc.`month`
    LEFT JOIN location loc
    ON cd.ou_code = loc.ou_code
    """
    print(sql)
    inc_df = spark.sql(sql)

    merge_data = MergeDFToTable("dsc_dws.dws_dsc_wh_ou_monthly_kpi_sum", inc_df, "ou_code,month", "update_time")
    merge_data.merge()


def main():
    args = argparse.ArgumentParser()
    month_num = datetime.now().month -1
    default_month =""
    if month_num < 0:
        default_month = str((datetime.now().year - 1) * 100 + 12 + month_num)
    else:
        default_month = str(datetime.now().year * 100 + month_num)
    args.add_argument("--start_month", help="start month for refresh data, format: yyyyMM"
                      , default=[default_month], nargs="*")
    args.add_argument("--end_month", help="end month for refresh data, format: yyyyMM"
                      , default=[default_month], nargs="*")

    args_parse = args.parse_args()
    start_month = args_parse.start_month[0]
    end_month = args_parse.end_month[0]

    run_etl(start_month, end_month)


if __name__ == '__main__':
    main()


