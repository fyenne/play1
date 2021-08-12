from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import argparse
from MergeDataFrameToTable import MergeDFToTable


def run_etl(start_date, end_date):
    spark = SparkSession.builder.appName("dws_dsc_wh_ou_daily_kpi_sum").enableHiveSupport().getOrCreate()
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
            , inc_day
            from dsc_dwd.dwd_wh_dsc_inbound_header_dtl_di
            where inc_day between '""" + start_date + """' and '""" + end_date + """'
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
        from (select asn_id, asn_line_id, original_qty, receive_qty, receive_volume 
            , receive_weight
            , std_receive_volume 
            , std_receive_weight
            , inc_day
            , lower(coalesce(wms_warehouse_id, wms_warehouse_name)) as wms_warehouse_id2
            , lower(coalesce(wms_company_id,'')) as wms_company_id2
              from dsc_dwd.dwd_wh_dsc_inbound_line_dtl_di
              where inc_day between '""" + start_date + """' and '""" + end_date + """'
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
            , inc_day
              from dsc_dwd.dwd_wh_dsc_outbound_header_dtl_di
              where inc_day between '""" + start_date + """' and '""" + end_date + """'
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
            , inc_day
            from dsc_dwd.dwd_wh_dsc_outbound_line_dtl_di
            where inc_day between '""" + start_date + """' and '""" + end_date + """'
        ) as t1
        join t2
        on  t1.wms_warehouse_id2 = t2.wms_warehouse_id
          and t1.wms_company_id2 = t2.company_id
        group by t2.ou_code,t1.inc_day
       """
    print(sql)
    outbound_order_line_df = spark.sql(sql)
    outbound_order_line_df.createOrReplaceTempView("outbound_order_line")

    # inventory
    sql = """
        select t1.inc_day,t2.ou_code
        ,count(distinct t1.location) as used_storage_location_count
        ,count(distinct lpn) as active_lpn_count
        ,sum(t1.on_hand_qty) as on_hand_qty
        ,sum(t1.in_transit_qty) as in_transit_qty
        ,sum(allocated_qty) as allocated_qty
        from (select inc_day,location,lpn,on_hand_qty,in_transit_qty,allocated_qty
                ,lower(coalesce(wms_warehouse_id, wms_warehouse_name)) as wms_warehouse_id2
                ,lower(coalesce(wms_company_id,'')) as wms_company_id2
                from dsc_dwd.dwd_wh_dsc_inventory_dtl_di
                where inc_day between '""" + start_date + """' and '""" + end_date + """'
            ) as t1
        join t2
        on t1.wms_warehouse_id2 = t2.wms_warehouse_id
        and t1.wms_company_id2 = t2.company_id
        group by t2.ou_code,t1.inc_day
    """
    print(sql)
    inventory_df = spark.sql(sql)
    inventory_df.createOrReplaceTempView("inventory")

    # human resource
    sql = """
         select inc_day, ou_code
        ,count(w.emp_code) as total_head_count
        ,sum(w.working_hours) as total_working_hours
        ,sum(case when lower(w.emp_type) = 'perm' then w.working_hours else 0 end) as perm_working_hours
        ,sum(case when lower(w.emp_type) = 'outsource' then w.working_hours else 0 end) as outsource_working_hours
        ,sum(case when lower(w.emp_type) not in ('perm','outsource') then w.working_hours else 0 end) as other_working_hour
        ,sum(case when lower(s.direct) = 'direct' then w.working_hours else 0 end) as direct_working_hour
        ,sum(case when lower(coalesce(s.direct,'indirect')) = 'indirect' then w.working_hours else 0 end) as indirect_working_hour
        from dsc_dwd.dwd_hr_dsc_working_hour_dtl_di w
        left join dsc_dim.dim_dsc_staff_info s
        on w.emp_code = s.staff_id
        where inc_day between '""" + start_date + """' and '""" + end_date + """'
        and ou_code <> ''
        and working_hours > 0 
        group by ou_code, inc_day
    """
    print(sql)
    hr_df = spark.sql(sql)
    hr_df.createOrReplaceTempView("hr")

    # combine all data
    sql = """
        SELECT 
            ou_code
            ,operation_day
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
            ,max(outbound_shipped_volume) AS outbound_shipped_volume
            ,max(outbound_shipped_weight) AS outbound_shipped_weight
            ,max(outbound_std_shipped_volume) as outbound_std_shipped_volume
            ,max(outbound_std_shipped_weight) as outbound_std_shipped_weight
            ,max(used_storage_location_count) as used_storage_location_count
            ,max(active_lpn_count) as active_lpn_count
            ,max(on_hand_qty) as on_hand_qty
            ,max(in_transit_qty) as in_transit_qty
            ,max(allocated_qty) as allocated_qty
            ,max(total_head_count) as total_head_count
            ,max(total_working_hour) as total_working_hour
            ,max(outsource_working_hour) as outsource_working_hour
            ,max(perm_working_hour) as perm_working_hour
            ,max(other_working_hour) as other_working_hour
            ,max(direct_working_hour) as direct_working_hour
            ,max(indirect_working_hour) as indirect_working_hour
            FROM
            (
                SELECT 
                ou_code
                ,inc_day AS operation_day
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
                ,NULL AS used_storage_location_count
                ,NULL AS active_lpn_count
                ,NULL AS on_hand_qty
                ,NULL AS in_transit_qty
                ,NULL AS allocated_qty
                ,NULL AS total_head_count
                ,NULL AS total_working_hour
                ,NULL AS outsource_working_hour
                ,NULL AS perm_working_hour
                ,NULL as other_working_hour
                ,NULL as direct_working_hour
                ,NULL as indirect_working_hour
                FROM inbound_order
            
                UNION ALL
            
                SELECT
                ou_code
                ,inc_day AS operation_day
                ,NULL AS inbound_header_count
                ,inbound_line_cnt AS inbound_line_count
                ,inbound_original_qty AS inbound_line_original_qty
                ,inbound_receive_qty
                ,inbound_receive_volume AS inbound_receive_volume
                ,inbound_receive_weight AS inbound_receive_weight
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
                ,NULL AS used_storage_location_count
                ,NULL AS active_lpn_count
                ,NULL AS on_hand_qty
                ,NULL AS in_transit_qty
                ,NULL AS allocated_qty
                ,NULL AS total_head_count
                ,NULL AS total_working_hour
                ,NULL AS outsource_working_hour
                ,NULL AS perm_working_hour
                ,NULL as other_working_hour
                ,NULL as direct_working_hour
                ,NULL as indirect_working_hour
                FROM inbound_order_line
            
                UNION ALL
            
                SELECT
                ou_code
                ,inc_day AS operation_day
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
                ,NULL AS used_storage_location_count
                ,NULL AS active_lpn_count
                ,NULL AS on_hand_qty
                ,NULL AS in_transit_qty
                ,NULL AS allocated_qty
                ,NULL AS total_head_count
                ,NULL AS total_working_hour
                ,NULL AS outsource_working_hour
                ,NULL AS perm_working_hour
                ,NULL as other_working_hour
                ,NULL as direct_working_hour
                ,NULL as indirect_working_hour
                FROM outbound_order
            
                UNION ALL
            
                SELECT
                ou_code
                ,inc_day AS operation_day
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
                ,NULL AS used_storage_location_count
                ,NULL AS active_lpn_count
                ,NULL AS on_hand_qty
                ,NULL AS in_transit_qty
                ,NULL AS allocated_qty
                ,NULL AS total_head_count
                ,NULL AS total_working_hour
                ,NULL AS outsource_working_hour
                ,NULL AS perm_working_hour
                ,NULL as other_working_hour
                ,NULL as direct_working_hour
                ,NULL as indirect_working_hour
                FROM outbound_order_line
            
                UNION ALL
            
                SELECT
                ou_code
                ,inc_day AS operation_day
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
                ,used_storage_location_count
                ,active_lpn_count
                ,on_hand_qty
                ,in_transit_qty
                ,allocated_qty
                ,NULL AS total_head_count
                ,NULL AS total_working_hour
                ,NULL AS outsource_working_hour
                ,NULL AS perm_working_hour
                ,NULL as other_working_hour
                ,NULL as direct_working_hour
                ,NULL as indirect_working_hour
                FROM inventory
            
                UNION ALL
            
                SELECT
                ou_code
                ,inc_day AS operation_day
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
                ,NULL AS used_storage_location_count
                ,NULL AS active_lpn_count
                ,NULL AS on_hand_qty
                ,NULL AS in_transit_qty
                ,NULL AS allocated_qty
                ,total_head_count
                ,total_working_hours AS total_working_hour
                ,outsource_working_hours AS outsource_working_hour
                ,perm_working_hours AS perm_working_hour
                ,other_working_hour
                ,direct_working_hour
                ,indirect_working_hour
                FROM hr
            )
            GROUP BY 
            ou_code
            ,operation_day 
    """
    print(sql)
    combined_df = spark.sql(sql)
    combined_df.createOrReplaceTempView("combined_data")

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


    sql = """
        SELECT cd.ou_code
            ,ou.ou_name
            ,bg.bg_code
            ,bg.bg_name_cn
            ,ou.customer_id
            ,ou.customer_name
            ,l.total_storage_location_count
            ,cd.operation_day
            ,d.week
            ,d.month
            ,d.quarter
            ,d.year
            ,d.is_holiday
            ,cd.inbound_header_count
            ,cd.inbound_line_count
            ,cd.inbound_line_original_qty
            ,cd.inbound_receive_qty
            ,cd.inbound_receive_volume
            ,cd.inbound_receive_weight
            ,cd.inbound_std_receive_volume
            ,cd.inbound_std_receive_weight
            ,cd.outbound_header_count
            ,cd.outbound_line_count
            ,cd.outbound_line_original_qty
            ,cd.outbound_shipped_qty
            ,cd.outbound_shipped_volume
            ,cd.outbound_shipped_weight
            ,cd.outbound_std_shipped_volume
            ,cd.outbound_std_shipped_weight
            ,cd.used_storage_location_count
            ,cd.active_lpn_count
            ,cd.on_hand_qty
            ,cd.in_transit_qty
            ,cd.allocated_qty
            ,cd.total_head_count
            ,cd.total_working_hour
            ,cd.outsource_working_hour
            ,cd.perm_working_hour
            ,cd.other_working_hour
            ,cd.direct_working_hour
            ,cd.indirect_working_hour
            ,coalesce(cd.outbound_shipped_qty,0)/cd.inbound_receive_qty as outbound_Inbound_qty_ratio
            ,cd.perm_working_hour/cd.total_working_hour as perm_working_hour_ratio
            ,cd.total_working_hour/cd.total_head_count as working_hour_per_head
            ,cd.used_storage_location_count/l.total_storage_location_count as location_usage_rate
            ,1 - (cd.used_storage_location_count/l.total_storage_location_count) as location_idle_rate
            ,from_unixtime(unix_timestamp(),'yyyy-MM-dd hh:mm:ss') as update_time
        FROM combined_data cd
        LEFT JOIN  dsc_dim.dim_dsc_ou_info ou
        ON cd.ou_code = ou.ou_code
        LEFT JOIN  dsc_dim.dim_dsc_business_group_info bg
        ON ou.bg_code = bg.bg_code
        LEFT JOIN location l
        on cd.ou_code = l.ou_code
        LEFT JOIN (SELECT date_id,`week`, `month`,`quarter`,`year`,is_holiday FROM dsc_dim.dim_dsc_date_info
            Where date_id between '""" + start_date + """' and '""" + end_date + """' and calendar_type = 0
        ) d
        ON cd.operation_day = d.date_id
    """
    print(sql)
    inc_df = spark.sql(sql)

    merge_data = MergeDFToTable("dsc_dws.dws_dsc_wh_ou_daily_kpi_sum", inc_df, "ou_code,operation_day", "update_time")
    merge_data.merge()


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--start_date", help="start date for refresh data, format: yyyyMMdd"
                          , default=[(datetime.now() - timedelta(days=1)).strftime("%Y%m%d")], nargs="*")
    args.add_argument("--end_date", help="end date for refresh data, format: yyyyMMdd"
                      , default=[(datetime.now() - timedelta(days=1)).strftime("%Y%m%d")], nargs="*")

    args_parse = args.parse_args()
    start_date = args_parse.start_date[0]
    end_date = args_parse.end_date[0]

    run_etl(start_date, end_date)


if __name__ == '__main__':
    main()


