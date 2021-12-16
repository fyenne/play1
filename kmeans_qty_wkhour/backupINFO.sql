/*|backupINFO|*/


`ou_code`|string|COMMENT|'单位名称_单位名称',
`bg_code`|string|COMMENT|'bg的代码',
`bg_name_cn`|string|COMMENT|'bg的名字',
`operation_day`|string|COMMENT|'操作时间_操作时间',
`is_holiday`|bigint|COMMENT|'假期_假期',
`inbound_receive_qty`|double|COMMENT|'入库数量_入库数量',
`kernal_core1`|bigint|COMMENT|'聚类_聚类1入库',
`kernal_value1`|double|COMMENT|'聚类中心_聚类中心1',
`outbound_shipped_qty`|double|COMMENT|'出库数量_出库数量',
`kernal_core2`|bigint|COMMENT|'聚类2_聚类2出库',
`kernal_value2`|double|COMMENT|'聚类中心2_聚类中心2',
`total_working_hour`|double|COMMENT|'总工时_总工时',
`kernal_core3`|bigint|COMMENT|'聚类3_总工时',
`kernal_value3`|double|COMMENT|'聚类中心3_聚类中心3',
`outsource_working_hour`|double|COMMENT|'外包os工时',
`kernal_core4`|bigint|COMMENT|'聚类4,外包os的聚类',
`kernal_value4`|double|COMMENT|'外包os聚类中心3_聚类中心3',
`total_head_count`|double|COMMENT|'总人头_总人头',
`working_hour_per_head`|double|COMMENT|'人均工时_人均工时',
`max_wh`|double|COMMENT|'聚类中最大工时_聚类中最大工时',
`min_wh`|double|COMMENT|'聚类中最小工时_聚类中最小工时',
`median_wh`|double|COMMENT|'聚类中中位数工时_聚类中中位数工时',
`mean_wh`|double|COMMENT|'聚类中平均数工时_聚类中平均数工时',
`qt_75_wh`|double|COMMENT|'聚类中1/4quantile工时',
`qt_75_os`|double|COMMENT|'qt75外包',
`flag_75_os`|string|COMMENT|'外包os标注flag',
`flag_75_wh`|string|COMMENT|'总工时标注flag',
`dis_tt_kernel`|double|COMMENT|'distance总工时到75*1.2的位置',
`dis_os_kernel`|double|COMMENT|'distance外包工时到75*1.2的位置',
`date_stamp`|string|COMMENT|'更新时间'




select
ou_code
,bg_code
,bg_name_cn
,operation_day
,is_holiday
,inbound_receive_qty
,kernal_core1
,kernal_value1
,outbound_shipped_qty
,kernal_core2
,kernal_value2
,total_working_hour
,kernal_core3
,kernal_value3
,outsource_working_hour
,kernal_core4
,kernal_value4
,total_head_count
,working_hour_per_head
,max_wh
,min_wh
,median_wh
,mean_wh
,qt_75_wh
,qt_75_os
,flag_75_os
,flag_75_wh
,dis_tt_kernel
,dis_os_kernel
,date_stamp
from
dsc_dws.dws_qty_working_hour_labeling_sum_df
order by operation_day desc