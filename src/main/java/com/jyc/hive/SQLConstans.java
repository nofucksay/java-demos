package com.jyc.hive;

/**
 * Created by jyc on 2018/7/16.
 */
public class SQLConstans {

    public static final String SQL_15 = "with\n" +
            "A as (\n" +
            "SELECT  \n" +
            "case \n" +
            "when\n" +
            "\tconcat(\n" +
            "\tCONCAT(\n" +
            "\t\t case when hour(estimate_board_time)<10 then CONCAT( '0',hour(estimate_board_time)) else hour(estimate_board_time) end\n" +
            "\t\t ,\n" +
            "\t\t case when minute (estimate_board_time)>=0 and minute (estimate_board_time)<30 then '00' else '30' end \n" +
            "\t\t ),\n" +
            "\n" +
            "\n" +
            "\tCONCAT(\n" +
            "\t\t case when hour( from_unixtime(unix_timestamp(estimate_board_time)+30*60))<10 then CONCAT( '0',hour( from_unixtime(unix_timestamp(estimate_board_time)+30*60))) else hour( from_unixtime(unix_timestamp(estimate_board_time)+30*60)) end\n" +
            "\t\t ,\n" +
            "\t\t case when minute (from_unixtime(unix_timestamp(estimate_board_time)+30*60))>=0 and minute (from_unixtime(unix_timestamp(estimate_board_time)+30*60))<30 then '00' else '30' end \n" +
            "\t\t ) \n" +
            "\t\t ) = '00000030'\n" +
            "then 300000\n" +
            "else\n" +
            " cast(\n" +
            " concat(\n" +
            "\tCONCAT(\n" +
            "\t\t  case when hour(estimate_board_time)<10 then CONCAT( '0',hour(estimate_board_time)) else hour(estimate_board_time) end\n" +
            "\t\t ,\n" +
            "\t\t case when minute (estimate_board_time)>=0 and minute (estimate_board_time)<30 then '00' else '30' end \n" +
            "\t\t ),\n" +
            "\n" +
            "\n" +
            "\tCONCAT(\n" +
            "\t\t case when hour( from_unixtime(unix_timestamp(estimate_board_time)+30*60)) < 10 then CONCAT( '0',hour( from_unixtime(unix_timestamp(estimate_board_time)+30*60))) else hour( from_unixtime(unix_timestamp(estimate_board_time)+30*60)) end\n" +
            "\t\t ,\n" +
            "\t\tcase when minute (from_unixtime(unix_timestamp(estimate_board_time)+30*60))>=0 and minute (from_unixtime(unix_timestamp(estimate_board_time)+30*60))<30 then '00' else '30' end \n" +
            "\t\t ) \n" +
            "\t\t )\n" +
            "\tas int\n" +
            " )\n" +
            "end  dayhour_id, \n" +
            "\t start_city_id,\n" +
            "\t count(1) as order_cnt,\n" +
            "\t \n" +
            "\t  count(\n" +
            "\t\t\t\t   case \n" +
            "\t\t\t\t   when orde.status IN ( 5, 6, 15, 9, 17 ) then 1 \n" +
            "\t\t\t\t   else null\n" +
            "\t\t\t\t   end \n" +
            "\t\t   ) as order_deal_cnt,\n" +
            "\t\t   \n" +
            "\t  count(\n" +
            "\t\t\t\t   case \n" +
            "\t\t\t\t   when orde.status IN ( 6, 15, 9, 17 ) then 1 \n" +
            "\t\t\t\t   else null\n" +
            "\t\t\t\t   end \n" +
            "\t\t   ) as order_no_deal_cnt,\n" +
            "      count(\n" +
            "\t         \t   case \n" +
            "\t\t\t\t   when orde.status = 8 and orde.driver_id is not null and ccl.id is not null then 1 \n" +
            "\t\t\t\t   else null\n" +
            "\t\t\t\t   end \n" +
            "\t\t  ) as cancel_order_cnt,\n" +
            "\t  count(\n" +
            "\t         \t   case \n" +
            "\t\t\t\t   when orde.status = 8 and orde.driver_id is null then 1 \n" +
            "\t\t\t\t   else null\n" +
            "\t\t\t\t   end \n" +
            "\t  ) as before_dis_cancel_order_cnt,\n" +
            "\t\t   \n" +
            "    from_unixtime(unix_timestamp(estimate_board_time),'yyyyMMdd') date_id\n" +
            "\t \n" +
            "\tFROM   (select * from ucar.t_scd_order_all where dt > date_add('$sDate',-120) and dt <= '$eDate') orde\n" +
            "\t  left join\n" +
            "         (select * from bi_ucar.bas_dim_order_cancel_or_not where type = 1) ccl \n" +
            "      on ccl.id = coalesce(orde.cancel_reason_id, -99999)\n" +
            "\tWHERE   \n" +
            "\t\t  estimate_board_time >= '$sDate' and estimate_board_time <   date_add('$eDate',1)   \n" +
            "\tgroup by  from_unixtime(unix_timestamp(estimate_board_time),'yyyyMMdd'), \n" +
            "case \n" +
            "\n" +
            "when\n" +
            "\tconcat(\n" +
            "\tCONCAT(\n" +
            "\t\t case when hour(estimate_board_time)<10 then CONCAT( '0',hour(estimate_board_time)) else hour(estimate_board_time) end\n" +
            "\t\t ,\n" +
            "\t\t case when minute (estimate_board_time)>=0 and minute (estimate_board_time)<30 then '00' else '30' end \n" +
            "\t\t ),\n" +
            "\n" +
            "\n" +
            "\tCONCAT(\n" +
            "\t\t case when hour( from_unixtime(unix_timestamp(estimate_board_time)+30*60)) <10 then CONCAT( '0',hour( from_unixtime(unix_timestamp(estimate_board_time)+30*60))) else hour( from_unixtime(unix_timestamp(estimate_board_time)+30*60)) end\n" +
            "\t\t ,\n" +
            "\t\t case when minute (from_unixtime(unix_timestamp(estimate_board_time)+30*60))>=0 and minute (from_unixtime(unix_timestamp(estimate_board_time)+30*60))<30 then '00' else '30' end \n" +
            "\t\t ) \n" +
            "\t\t ) = '00000030'\n" +
            "then 300000\n" +
            "else\n" +
            " cast(\n" +
            " concat(\n" +
            "\tCONCAT(\n" +
            "\t\t  case when hour(estimate_board_time)<10 then CONCAT( '0',hour(estimate_board_time)) else hour(estimate_board_time) end\n" +
            "\t\t ,\n" +
            "\t\t case when minute (estimate_board_time)>=0 and minute (estimate_board_time)<30 then '00' else '30' end \n" +
            "\t\t ),\n" +
            "\n" +
            "\n" +
            "\tCONCAT(\n" +
            "\t\t case when hour( from_unixtime(unix_timestamp(estimate_board_time)+30*60))<10 then CONCAT( '0',hour( from_unixtime(unix_timestamp(estimate_board_time)+30*60))) else hour( from_unixtime(unix_timestamp(estimate_board_time)+30*60)) end\n" +
            "\t\t ,\n" +
            "\t\tcase when minute (from_unixtime(unix_timestamp(estimate_board_time)+30*60))>=0 and minute (from_unixtime(unix_timestamp(estimate_board_time)+30*60))<30 then '00' else '30' end \n" +
            "\t\t ) \n" +
            "\t\t )\n" +
            "\tas int\n" +
            " )\n" +
            "end ,\n" +
            "\tstart_city_id\n" +
            "),\n" +
            "lastWeek AS (\n" +
            "   select date_id,dayhour_id,start_city_id,order_cnt,order_deal_cnt,order_no_deal_cnt,cancel_order_cnt,before_dis_cancel_order_cnt from A\n" +
            "   union all\n" +
            "   select date_id,dayhour_id,start_city_id,order_cnt,order_deal_cnt,order_no_deal_cnt,cancel_order_cnt,before_dis_cancel_order_cnt from bi_ucar.topic_order_stat \n" +
            "   where date_id>=from_unixtime(unix_timestamp(date_add('$sDate',-7),'yyyy-MM-dd'),'yyyyMMdd') and date_id<from_unixtime(unix_timestamp('$sDate','yyyy-MM-dd'),'yyyyMMdd')\n" +
            "),\n" +
            "yes AS (\n" +
            "select \n" +
            "  COALESCE(A.date_id,from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(cast(lastWeek.date_id as string),'yyyyMMdd'),'yyyy-MM-dd'),1),'yyyy-MM-dd'),'yyyyMMdd')) date_id,\n" +
            "  COALESCE(A.dayhour_id,lastWeek.dayhour_id) dayhour_id,\n" +
            "  COALESCE(A.start_city_id,lastWeek.start_city_id) start_city_id,\n" +
            "  COALESCE(A.order_cnt,0) order_cnt,\n" +
            "  COALESCE(A.order_deal_cnt,0) order_deal_cnt,\n" +
            "  COALESCE(A.order_no_deal_cnt) order_no_deal_cnt,\n" +
            "  COALESCE(A.cancel_order_cnt,0) cancel_order_cnt,\n" +
            "  COALESCE(A.before_dis_cancel_order_cnt,0) before_dis_cancel_order_cnt,\n" +
            "  COALESCE(lastWeek.order_cnt,0) yes_order_cnt,\n" +
            "  COALESCE(lastWeek.order_deal_cnt,0) yes_order_deal_cnt \n" +
            "from A\n" +
            "full join lastWeek\n" +
            "on A.date_id = from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(cast(lastWeek.date_id as string),'yyyyMMdd'),'yyyy-MM-dd'),1),'yyyy-MM-dd'),'yyyyMMdd') \n" +
            "   and A.start_city_id = lastWeek.start_city_id and A.dayhour_id = lastWeek.dayhour_id\n" +
            "where COALESCE(A.date_id,from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(cast(lastWeek.date_id as string),'yyyyMMdd'),'yyyy-MM-dd'),1),'yyyy-MM-dd'),'yyyyMMdd')) >=from_unixtime(unix_timestamp('$sDate','yyyy-MM-dd'),'yyyyMMdd') \n" +
            ")\n" +
            "\n" +
            "insert overwrite table bi_ucar.topic_order_stat partition (date_id)\n" +
            "select \n" +
            "  COALESCE(A.dayhour_id,lastWeek.dayhour_id) dayhour_id,\n" +
            "  COALESCE(A.start_city_id,lastWeek.start_city_id) start_city_id,\n" +
            "  COALESCE(A.order_cnt,0) order_cnt,\n" +
            "  COALESCE(A.order_deal_cnt,0) order_deal_cnt,\n" +
            "  COALESCE(A.order_no_deal_cnt) order_no_deal_cnt,\n" +
            "  COALESCE(A.cancel_order_cnt,0) cancel_order_cnt,\n" +
            "  COALESCE(A.before_dis_cancel_order_cnt,0) before_dis_cancel_order_cnt,\n" +
            "  COALESCE(A.yes_order_cnt,0) yes_order_cnt,\n" +
            "  COALESCE(A.yes_order_deal_cnt,0) yes_order_deal_cnt,\n" +
            "  COALESCE(lastWeek.order_cnt,0) week_order_cnt,\n" +
            "  COALESCE(lastWeek.order_deal_cnt,0) week_order_deal_cnt,\n" +
            "  COALESCE(A.date_id,from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(cast(lastWeek.date_id as string),'yyyyMMdd'),'yyyy-MM-dd'),7),'yyyy-MM-dd'),'yyyyMMdd')) date_id\n" +
            "from yes AS A\n" +
            "full join lastWeek\n" +
            "on A.date_id = from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(cast(lastWeek.date_id as string),'yyyyMMdd'),'yyyy-MM-dd'),7),'yyyy-MM-dd'),'yyyyMMdd') \n" +
            "   and A.start_city_id = lastWeek.start_city_id and A.dayhour_id = lastWeek.dayhour_id\n" +
            "where COALESCE(A.date_id,from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(cast(lastWeek.date_id as string),'yyyyMMdd'),'yyyy-MM-dd'),7),'yyyy-MM-dd'),'yyyyMMdd')) >=from_unixtime(unix_timestamp('$sDate','yyyy-MM-dd'),'yyyyMMdd') ";

    public static final String SQL_14 = "WITH info1\n" +
            "     AS (SELECT dept_id,\n" +
            "                date_id,\n" +
            "                employee_id,\n" +
            "                channel_type,\n" +
            "                member_id,\n" +
            "                distribute_date,\n" +
            "                concat(date_add(to_date(distribute_date),1),' 09:00:00') as distribute_date_torm,\n" +
            "                tracker_date,\n" +
            "                rank ()\n" +
            "                   OVER (PARTITION BY dept_id,\n" +
            "                                      date_id,\n" +
            "                                      employee_id,\n" +
            "                                      channel_type,\n" +
            "                                      member_id,\n" +
            "                                      distribute_date\n" +
            "                         ORDER BY tracker_date)\n" +
            "                   rnb,\n" +
            "                dt\n" +
            "           FROM bi_dw.dw_mmc_sales_base_info info\n" +
            "          WHERE info.dt >= '$partition' AND dt <= date_add ('$partition', 31)\n" +
            "          AND employee_id = oper_employee_id\n" +
            "          ),\n" +
            "     info2\n" +
            "     AS (SELECT /*+ MAPJOIN(wk)*/\n" +
            "               a.dept_id,\n" +
            "                a.date_id,\n" +
            "                a.employee_id,\n" +
            "                a.channel_type,\n" +
            "                a.dt,\n" +
            "                a.member_id,\n" +
            "                CASE\n" +
            "                   WHEN  substring (a.distribute_date, 1, 10)=substring (a.tracker_date, 1, 10) THEN \n" +
            "\t\t\t\t   CASE WHEN \n" +
            "\t\t\t\t   unix_timestamp(a.distribute_date) + 15 * 60 >=\n" +
            "                               unix_timestamp (\n" +
            "                                  concat (\n" +
            "                                     substring (a.distribute_date, 1, 10),\n" +
            "                                     ' ',\n" +
            "                                     coalesce (wk.work_end_time, ' 18:00:00')))\n" +
            "                        AND   unix_timestamp (a.tracker_date)\n" +
            "                            - unix_timestamp (a.distribute_date) <=\n" +
            "                               15 * 60 * 60 + 15 * 60\n" +
            "                   THEN\n" +
            "                      1\n" +
            "                   WHEN   unix_timestamp (a.tracker_date)\n" +
            "                        - unix_timestamp (a.distribute_date) <= 15 * 60\n" +
            "                   THEN\n" +
            "                      1\n" +
            "                   ELSE\n" +
            "                      0\n" +
            "\t\t\t\t   END\n" +
            "\t\t\t\tELSE\n" +
            "\t\t\t\t\tCASE WHEN \n" +
            "\t\t\t\t   unix_timestamp (a.distribute_date_torm) + 15 * 60 >=\n" +
            "                               unix_timestamp (\n" +
            "                                  concat (\n" +
            "                                     substring (a.distribute_date_torm, 1, 10),\n" +
            "                                     ' ',\n" +
            "                                     coalesce (wk.work_end_time, ' 18:00:00')))\n" +
            "                        AND   unix_timestamp (a.tracker_date)\n" +
            "                            - unix_timestamp (a.distribute_date_torm) <=\n" +
            "                               15 * 60 * 60 + 15 * 60\n" +
            "                   THEN\n" +
            "                      1\n" +
            "                   WHEN   unix_timestamp (a.tracker_date)\n" +
            "                        - unix_timestamp (a.distribute_date_torm) <= 15 * 60\n" +
            "                   THEN\n" +
            "                      1\n" +
            "                   ELSE\n" +
            "                      0\n" +
            "\t\t\t\t   END\n" +
            "\t\t\t\tEND \n" +
            "                   AS is_timely\n" +
            "           FROM info1 AS a\n" +
            "                LEFT JOIN bi_dm.dim_work_time wk ON a.dept_id = wk.dept_id\n" +
            "          WHERE a.rnb = 1)\n" +
            "INSERT overwrite table bi_dw.dw_mmc_sales_trace_info partition(dt)\n" +
            "SELECT /*+ MAPJOIN(info_channel)*/\n" +
            "      infos.dept_id,\n" +
            "       infos.date_id,\n" +
            "       infos.employee_id,\n" +
            "       infos.member_id,\n" +
            "       CASE\n" +
            "          WHEN info_channel.type = 1 THEN 1\n" +
            "          WHEN info_channel.channel_id = 1407 THEN 2\n" +
            "       END\n" +
            "          AS channel_type,\n" +
            "       count (DISTINCT infos.member_id) online_trace,\n" +
            "       sum (info2.is_timely) online_trace_timely,\n" +
            "       infos.dt\n" +
            "  FROM (SELECT dept_id,\n" +
            "               date_id,\n" +
            "               employee_id,\n" +
            "               member_id,\n" +
            "               channel_type,\n" +
            "               dt\n" +
            "          FROM bi_dw.dw_mmc_sales_base_info\n" +
            "         WHERE dt >= '$partition' AND dt <= date_add ('$partition', 31)\n" +
            "        GROUP BY dept_id,\n" +
            "                 date_id,\n" +
            "                 employee_id,\n" +
            "                 member_id,\n" +
            "                 channel_type,\n" +
            "                 dt) infos\n" +
            "       LEFT JOIN info2\n" +
            "          ON     infos.dept_id = info2.dept_id\n" +
            "             AND infos.date_id = info2.date_id\n" +
            "             AND infos.employee_id = info2.employee_id\n" +
            "             AND infos.channel_type = info2.channel_type\n" +
            "             AND infos.member_id = info2.member_id\n" +
            "       INNER JOIN bi_dm.dim_channel_type info_channel\n" +
            "          ON infos.channel_type = info_channel.channel_id\n" +
            "GROUP BY infos.dept_id,\n" +
            "         infos.date_id,\n" +
            "         infos.employee_id,\n" +
            "         CASE\n" +
            "            WHEN info_channel.type = 1 THEN 1\n" +
            "            WHEN info_channel.channel_id = 1407 THEN 2\n" +
            "         END,\n" +
            "         infos.member_id,\n" +
            "         infos.dt";

    public static final String SQL_13 = "INSERT OVERWRITE TABLE bi_ucar.dm_member_recharge_coupon_summary PARTITION(partition_dt='2018-01-01')\n" +
            "SELECT \n" +
            "    t1.member_id,\n" +
            "    t1.register_time,\n" +
            "    t1.mobile_attribution,\n" +
            "    (CASE WHEN t2.member_id IS NOT NULL THEN 1 ELSE 0 END) AS is_bind_creditcard,\n" +
            "    t3.first_recharge_time,\n" +
            "    IF(t3.recharge_num IS NULL, 0, t3.recharge_num) AS recharge_num,\n" +
            "    IF(t3.recharge_money IS NULL, 0, t3.recharge_money) AS recharge_money,\n" +
            "    IF(t3.gift_money IS NULL, 0, t3.gift_money) AS gift_money,\n" +
            "    t1.remain_money,\n" +
            "    t1.remain_gift_money,\n" +
            "    IF(t4.total_coupon_num IS NULL, 0, t4.total_coupon_num) AS total_coupon_num,\n" +
            "    IF(t4.out_coupon_num IS NULL, 0, t4.out_coupon_num) AS out_coupon_num\n" +
            "FROM ( --用户基本信息及帐户可用金额情况\n" +
            "    SELECT \n" +
            "        t11.id AS member_id,\n" +
            "        t11.create_time AS register_time,\n" +
            "        t12.name AS mobile_attribution,\n" +
            "        t11.remain_money AS remain_money,\n" +
            "        IF(t11.gift_remain_money IS NULL, 0, t11.gift_remain_money) - IF(t11.gift_freeze_money IS NULL, 0, t11.gift_freeze_money) AS remain_gift_money\n" +
            "    FROM ucar.t_scd_member AS t11\n" +
            "    LEFT JOIN default.t_b_city AS t12\n" +
            "    ON t11.mobile_city_id=t12.id\n" +
            ") AS t1\n" +
            "--------------------------------------------------------------------------------\n" +
            "LEFT JOIN ( --用户绑定信用卡情况，即绑定信用卡的用户\n" +
            "    SELECT  payer_id AS member_id\n" +
            "    FROM bi_ucar.bas_creditcard_bind  \n" +
            "    WHERE is_driver = 0 \n" +
            "      AND status = 1\n" +
            "    GROUP BY payer_id\n" +
            ") AS t2\n" +
            "ON t1.member_id=t2.member_id\n" +
            "--------------------------------------------------------------------------------\n" +
            "LEFT JOIN( --用户充值与获赠情况\n" +
            "    SELECT \n" +
            "        member_id,\n" +
            "        SUM(CASE WHEN money>0 THEN 1 ELSE 0 END) AS recharge_num,\n" +
            "        SUM(money) AS recharge_money,\n" +
            "        SUM(gift_money) AS gift_money,\n" +
            "        MIN(CASE WHEN money>0 THEN create_time ELSE NULL END) AS first_recharge_time\n" +
            "    FROM ucar.t_scd_member_recharge\n" +
            "    WHERE create_time < DATE_ADD('$sDate', 1)\n" +
            "    GROUP BY member_id\n" +
            ") AS t3\n" +
            "ON t1.member_id=t3.member_id\n" +
            "--------------------------------------------------------------------------------\n" +
            "LEFT JOIN( --用户优惠券持有情况\n" +
            "    SELECT \n" +
            "        member_id,\n" +
            "        SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END ) AS out_coupon_num,\n" +
            "        COUNT(1) AS total_coupon_num \n" +
            "    FROM bi_ucar.bas_member_coupon\n" +
            "    WHERE create_time < DATE_ADD('$sDate', 1)\n" +
            "    GROUP BY member_id\n" +
            ") AS t4\n" +
            "ON t1.member_id=t4.member_id\n" +
            "\n";


    public static final String SQL_12 = "insert into table bi_zuche.bas_vehicle_sync_by_hour PARTITION(dt)\n" +
            "select \n" +
            "v.xid                           ,\n" +
            "v.xvehicleno                    ,\n" +
            "v.xframeno                      ,\n" +
            "v.xengineno                     ,\n" +
            "v.del_xmodel_id                ,\n" +
            "v.xcolor                        ,\n" +
            "v.xseat                         ,\n" +
            "v.xexhaustscale                 ,\n" +
            "v.xuseyears                     ,\n" +
            "v.xrunmilesinput                ,\n" +
            "v.xproduction                   ,\n" +
            "v.xoperator_id                  ,\n" +
            "v.xregtime                      ,\n" +
            "v.xdepartment_id                ,\n" +
            "v.xnowdepartment_id             ,\n" +
            "v.xremark                       ,\n" +
            "v.insurer                      ,\n" +
            "v.xordercar                     ,\n" +
            "v.xgps                          ,\n" +
            "v.xvehicleowner                 ,\n" +
            "v.xvehiclefrom                  ,\n" +
            "v.xgettime                      ,\n" +
            "v.xgetformno                    ,\n" +
            "v.xgetformaddress               ,\n" +
            "v.xclientname                   ,\n" +
            "v.del_xvehiclesubmode_id       ,\n" +
            "v.xvehiclelicensemode           ,\n" +
            "v.xcarusetype                   ,\n" +
            "v.xlasttimeattendmile           ,\n" +
            "v.xnexttimeattendmile           ,\n" +
            "v.xgpscompanymanage_id          ,\n" +
            "v.xcontractno                   ,\n" +
            "v.v_status_1st                 ,\n" +
            "v.v_status_2nd                 ,\n" +
            "v.v_status_3rd                 ,\n" +
            "v.modify_emp                   ,\n" +
            "v.modify_time                  ,\n" +
            "v.next_inspecte_time           ,\n" +
            "v.check_time                   ,\n" +
            "v.sim_no                       ,\n" +
            "v.doc_no                       ,\n" +
            "v.use_nature                   ,\n" +
            "v.vehicleno_city_id            ,\n" +
            "v.peccancy_query_belong_dept   ,\n" +
            "v.peccancy_last_query_time     ,\n" +
            "v.assign_time                  ,\n" +
            "v.assign_emp                   ,\n" +
            "v.garage_type                  ,\n" +
            "v.registerno                   ,\n" +
            "v.env_standard                 ,\n" +
            "v.assets_depreciation_rate     ,\n" +
            "v.vehicle_no_before_transfer   ,\n" +
            "v.archive_serial_no            ,\n" +
            "v.archive_no                   ,\n" +
            "v.is_mortgage                  ,\n" +
            "v.car_owner_id                 ,\n" +
            "v.handle_time                  ,\n" +
            "v.short_model_id               ,\n" +
            "v.model_id                     ,\n" +
            "v.quit_run_time                ,\n" +
            "v.park_dept_id                 ,\n" +
            "v.garage_type_repair           ,\n" +
            "v.business_type                ,\n" +
            "v.self_status_1st              ,\n" +
            "v.self_status_2nd              ,\n" +
            "v.self_status_3rd              ,\n" +
            "v.is_hertz                     ,\n" +
            "v.last_quit_run_time           ,\n" +
            "v.last_approve_sale_time       ,\n" +
            "v.first_transfer_ownership_time,\n" +
            "v.is_financing                 ,\n" +
            "v.audit_status                 ,\n" +
            "v.is_offline_process           ,\n" +
            "v.stop_run_time                ,\n" +
            "v.is_vip                       ,\n" +
            "v.is_ucar                      ,\n" +
            "v.gps_online_status            ,\n" +
            "v.ucar_rent_type               ,\n" +
            "v.last_vehicle_purpose         ,\n" +
            "v.is_lease                     ,\n" +
            "v.garage_id                    ,\n" +
            "v.business_id                  ,\n" +
            "v.business_change_time         ,\n" +
            "v.will_business_id             ,\n" +
            "v.will_business_change_time    ,\n" +
            "v.check_result                 ,\n" +
            "v.allocation_status            ,\n" +
            "v.is_inside_scrap              ,\n" +
            "v.xordercar_his                 ,\n" +
            "v.last_maintain_pakage         ,\n" +
            "v.now_city_id_online           ,\n" +
            "v.is_shared                   ,\n" +
            "v.oil_volume,\n" +
            "s.short_model_name,\n" +
            "m.model_name,\n" +
            "m.fuel_tank,\n" +
            "d.xcity part_city_id \n" +
            ",CASE WHEN CAST(substring(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') ,12, 2) AS INT)   = 0 THEN 23 ELSE\n" +
            "   CAST(substring(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') ,12, 2) AS INT) -1   END time_split_tag\n" +
            "\n" +
            ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') create_time\n" +
            "\n" +
            ",CASE WHEN CAST(substring(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') ,12, 2) AS INT)   = 0  THEN\n" +
            "\n" +
            "substring(date_sub(to_date(CONCAT(substring(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') ,0, 10) , ' 00:00:00')),1) , 0  , 10)\n" +
            "\n" +
            "ELSE substring(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') ,0, 10) END dt\n" +
            "from zuche.vehicle_hour v\n" +
            "left join zuche.t_v_model m on v.model_id = m.id\n" +
            "left join zuche.t_v_short_model s on v.short_model_id = s.id\n" +
            "left join zuche.department  d on  v.xnowdepartment_id = d.xid\n";

    public static final String SQL_11 = "insert overwrite table bi_zuche.fact_sco_member_debt_money PARTITION(dt)\n" +
            "select\n" +
            " order_id\n" +
            ",order_no\n" +
            ",member_id\n" +
            ",member.name\n" +
            ",order_status\n" +
            ",sum(rent_debt_money)rent_debt_money\n" +
            ",sum(violation_debt_money)violation_debt_money\n" +
            ",sum(maintenance_debt_money)maintenance_debt_money\n" +
            ",exempt_type --免押类型\n" +
            ",exempt_status\n" +
            ",substring(date_sub(to_date(CONCAT(substring(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') ,0, 10) , ' 00:00:00')),1) , 0  , 10) dt\n" +
            "from (\n" +
            "select \n" +
            " balance_id\n" +
            ",balance_no\n" +
            ",order_id\n" +
            ",order_no\n" +
            ",member_id\n" +
            ",balance_type\n" +
            ",income_receivable\n" +
            ",income_received\n" +
            ",case balance_type  when 1 then   arear_amount  else 0 end rent_debt_money\n" +
            ",case balance_type  when 2 then   arear_amount  else 0 end violation_debt_money\n" +
            ",case balance_type  when 3 then   arear_amount  else 0 end maintenance_debt_money\n" +
            ",exempt_type\n" +
            ",order_status\n" +
            ",exempt_status\n" +
            "from (\n" +
            "select \n" +
            "balance.id balance_id\n" +
            ",balance.balance_no\n" +
            ",balance.order_id\n" +
            ",balance.order_no\n" +
            ",balance.member_id\n" +
            ",balance.balance_type\n" +
            ",income_receivable\n" +
            ",income_received\n" +
            ",arear_amount\n" +
            ",exempt_type\n" +
            ",order_status\n" +
            ",exempt_status\n" +
            ",row_number() over(partition by balance.balance_type ,balance.order_id  order by  balance.balance_no desc) max_row\n" +
            " from \n" +
            "zuche.t_sbalance_statement balance\n" +
            "LEFT JOIN zuche.t_spay_order_fi  pay_order_fi\n" +
            "on balance.order_id = pay_order_fi.order_id \n" +
            " ) t  where max_row = 1 ) dept \n" +
            "left join bi_zuche.bas_member member \n" +
            "on dept.member_id = member.id\n" +
            "where rent_debt_money> 0 or  violation_debt_money> 0 or  maintenance_debt_money> 0 \n" +
            "group by \n" +
            "order_id\n" +
            ",order_no\n" +
            ",member_id\n" +
            ",member.name\n" +
            ",balance_type\n" +
            ",exempt_type\n" +
            ",order_status\n" +
            ",exempt_status";

    public static final String SQL_TEST = "with tvrh15 as(\n" +
            "select min(create_date) as create_date,repair_sheet_id from uc.t_vrms_repair_his tvrh where type = 15 group by repair_sheet_id \n" +
            "),\n" +
            "tvrh19 as(\n" +
            "select min(create_date) as create_date,repair_sheet_id from uc.t_vrms_repair_his tvrh where type = 19 group by repair_sheet_id \n" +
            "),\n" +
            "tvrh5 as(\n" +
            "select min(create_date) as create_date,repair_sheet_id from uc.t_vrms_repair_his tvrh where type = 5 group by repair_sheet_id \n" +
            ") \n" +
            "insert overwrite table bi_mmc.fact_repair_project_info partition(dt) \n" +
            "select  a.repairSheetNo,a.zucheRepairNo,a.costsAttCityName,a.factoryName,a.repairMode,a.sendRepairType,a.vehicleNature,a.vehiclePurpose,a.vehicleNo,\n" +
            "        a.frameNo,modelName,a.orderSource,a.zcTypeName,a.paymentType,a.projectTypeName,a.projectName,a.repairWorkerName,a.workTime,a.univalence,a.workTimeFree,\n" +
            "        a.zucheLossAssMoney,a.careOutside,a.workshopName,a.outMoney,a.claimNos,a.componentName,a.specification,a.unit,a.componentNum,a.componentUnivalence,a.componentFree,\n" +
            "        a.returnDate,a.endTime,a.projectFree,a.repairType,a.is_reduce,a.repair_conclusion,substr(a.endTime,1,10) as dt from(\n" +
            "select  rs.id as id,  -- 维修单id\n" +
            "    rs.repair_sheet_no as repairSheetNo, -- 维修单号 \n" +
            "    tvrszi.zuche_repair_no as zucheRepairNo, -- 租车维修单号\n" +
            "    tvrszi.costs_attach_city_name as costsAttCityName,-- 成本归属城市\n" +
            "    factory.factory_name as factoryName, -- 维修厂名称   \n" +
            "    case rs.repair_mode \n" +
            "        WHEN 1 THEN '进厂维修'\n" +
            "        WHEN 2 THEN '上门维保'\n" +
            "        WHEN 3 THEN '只定损' \n" +
            "        WHEN 4 THEN '补单' \n" +
            "        ELSE '' \n" +
            "    end as repairMode, -- 维修方式 1进厂维修 2上门维保 3只定损 4补单 \n" +
            "    case tvrszi.send_repair_type \n" +
            "        WHEN 1 THEN '自主送取' \n" +
            "        WHEN 2 THEN '上门取送' \n" +
            "        ELSE '' \n" +
            "    end as sendRepairType, -- 送修方式 1自主送取 2上门取送 \n" +
            "    case when \n" +
            "        rs.repair_vehicle_id > 0 \n" +
            "        then \n" +
            "        bvrv.vehicle_nature \n" +
            "        else \n" +
            "        bvv.vehicle_nature \n" +
            "    end as vehicleNature, -- 车辆性质    \n" +
            "    --case when \n" +
            "    --  rs.repair_vehicle_id > 0 \n" +
            "    --  then \n" +
            "    --  bvrv.vehicle_purpose \n" +
            "    --  else \n" +
            "    --  bvv.vehicle_purpose \n" +
            "    --end as vehiclePurpose, -- 车辆用途\n" +
            "    case trb.ordercar \n" +
            "            when 0 then '短租' \n" +
            "            when 1 then '长租' \n" +
            "            when 3 then '融资租赁' \n" +
            "            when 4 then '试驾' \n" +
            "            when 5 then '公务车' \n" +
            "            when 7 then '专车' \n" +
            "            when 8 then '优驾' \n" +
            "    end as vehiclePurpose,--车辆用途\n" +
            "    case when \n" +
            "        rs.repair_vehicle_id > 0 \n" +
            "        then \n" +
            "        bvrv.vehicle_no \n" +
            "        else \n" +
            "        bvv.vehicle_no \n" +
            "    end as vehicleNo, -- 维修车辆车牌号\n" +
            "    case when \n" +
            "        rs.repair_vehicle_id > 0 \n" +
            "        then \n" +
            "        bvrv.frame_no \n" +
            "        else \n" +
            "        bvv.frame_no \n" +
            "    end as frameNo, -- 维修车辆车架号\n" +
            "    case when \n" +
            "        rs.repair_vehicle_id > 0 \n" +
            "        then \n" +
            "        bvrv.model_name \n" +
            "        else \n" +
            "        bvv.model_name \n" +
            "    end as modelName, -- 车型名称\n" +
            "    case rs.repair_sheet_source \n" +
            "        WHEN 1 THEN '租车' \n" +
            "        WHEN 2 THEN '专车' \n" +
            "        WHEN 3 THEN '闪贷' \n" +
            "        WHEN 4 THEN '买买车'  \n" +
            "        ELSE '' \n" +
            "    end as orderSource, -- 订单来源: 1 租车 2专车 3闪贷 4买买车\n" +
            "    tvrptzc.type_name as zcTypeName, -- 租车工单类型    \n" +
            "    case tvrspr.payment_type \n" +
            "        WHEN 1 THEN '客户付费' \n" +
            "        WHEN 2 THEN '保险付费' \n" +
            "        WHEN 3 THEN '公司付费'  \n" +
            "        ELSE '' \n" +
            "    end  as paymentType,-- 付费类型 1客户付费 2保险付费 3公司付费\n" +
            "    tvrspr.project_type_name as projectTypeName, -- 项目类型名称\n" +
            "    tvrp.project_name as projectName, -- 项目名称\n" +
            "    tvrspr.repair_worker_name as repairWorkerName, -- 主修人\n" +
            "    tvrspr.work_time as workTime, -- 工时\n" +
            "    tvrspr.univalence as univalence, -- 单价\n" +
            "    tvrspr.work_time_fee as workTimeFree,-- 工时费\n" +
            "    tvrspr.zuche_loss_assessment_money as zucheLossAssMoney, -- 租车定损金额\n" +
            "    case tvrspr.care_outside  \n" +
            "        WHEN 0 THEN '否' \n" +
            "        WHEN 1 THEN '是' \n" +
            "    end as careOutside, -- 托外(0:未选择,1:选择)\n" +
            "    tvrw.workshop_name as workshopName, -- 外托维修厂\n" +
            "    tvrspr.out_money as outMoney, -- 外托金额\n" +
            "    tvrspr.claim_nos as claimNos, -- 关联理赔单号\n" +
            "    tvrscr.component_name as componentName, -- 配件名称\n" +
            "    tvrscr.specification as specification, -- 配件规格\n" +
            "    tvrscr.unit as unit, -- 单位\n" +
            "    tvrscr.component_num as componentNum, -- 配件数量\n" +
            "    tvrscr.univalence as componentUnivalence, -- 配件单价\n" +
            "    tvrscr.component_fee as componentFree, -- 配件费\n" +
            "    from_unixtime(unix_timestamp(tvrszi.return_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as returnDate, -- 实际还车时间\n" +
            "    case WHEN rs.repair_mode = 1 AND tvrszi.send_repair_type=1  THEN from_unixtime(unix_timestamp(t15.create_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         WHEN rs.repair_mode = 1 AND tvrszi.send_repair_type=2 THEN from_unixtime(unix_timestamp(tvrszi.return_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         WHEN rs.repair_mode = 3  then from_unixtime(unix_timestamp(t19.create_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         WHEN rs.repair_mode = 2  then from_unixtime(unix_timestamp(t5.create_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         WHEN rs.repair_mode = 4  then from_unixtime(unix_timestamp(t5.create_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         ELSE from_unixtime(unix_timestamp(rs.update_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "    end as endTime,-- 工单结束时间\n" +
            "    case \n" +
            "        when row_number()over(partition by rs.id,tvrspr.id order by tvrscr.component_fee desc ) > 1 \n" +
            "        then tvrscr.component_fee \n" +
            "        else nvl(tvrspr.work_time_fee,0) + nvl(tvrscr.component_fee,0) + nvl(tvrspr.out_money,0) -- nvl(tvrspr.out_money,0)yuchen.jia在规则修改后添加\n" +
            "    end as projectFree, -- 项目费用\n" +
            "    case rs.type  \n" +
            "        WHEN 0 THEN '无' \n" +
            "        WHEN 1 THEN '整备' \n" +
            "        WHEN 2 THEN '保险' \n" +
            "        WHEN 3 THEN '维修' \n" +
            "        WHEN 4 THEN '整车检查' \n" +
            "        WHEN 5 THEN 'PDI检测' \n" +
            "        WHEN 6 THEN '常规保养' \n" +
            "        WHEN 7 THEN '免费保养' \n" +
            "        WHEN 8 THEN 'GPS安装' \n" +
            "        WHEN 9 THEN '精洗美容' \n" +
            "        WHEN 10 THEN '深化养护' \n" +
            "        WHEN 11 THEN '精品装璜' \n" +
            "        WHEN 12 THEN '市场营销' \n" +
            "        WHEN 13 THEN '机电维修' \n" +
            "        WHEN 14 THEN '故障诊断' \n" +
            "        WHEN 15 THEN '事故车维修（不含保险）' \n" +
            "        WHEN 16 THEN '保修'\n" +
            "        ELSE '' \n" +
            "    end as repairType, -- 维修类别.1:维修,2:整备\n" +
            "\n" +
            "    case when tvrscr.is_reduce is not null \n" +
            "    then \n" +
            "        case tvrscr.is_reduce\n" +
            "            when 0 then '否'\n" +
            "            when 1 then '是'\n" +
            "        end \n" +
            "    else \n" +
            "        case tvrspr.is_reduce\n" +
            "            when 0 then '否'\n" +
            "            when 1 then '是'\n" +
            "        end \n" +
            "    end as is_reduce, -- 是否做过减项\n" +
            "\n" +
            "    case when tvrscr.repair_conclusion is not null\n" +
            "    then\n" +
            "        case tvrscr.repair_conclusion\n" +
            "            when 0 then '无需维修'\n" +
            "            when 1 then '需要维修'\n" +
            "        end \n" +
            "    else\n" +
            "        case tvrspr.repair_conclusion\n" +
            "            when 0 then '无需维修'\n" +
            "            when 1 then '需要维修'\n" +
            "        end\n" +
            "    end as repair_conclusion -- 维修结论\n" +
            "from uc.t_vrms_repair_sheet rs\n" +
            "left join bi_mmc.bas_vrms_vehicle bvv on rs.vehicle_id = bvv.vehicle_id \n" +
            "left join bi_mmc.bas_vrms_repair_vehicle bvrv on rs.repair_vehicle_id = bvrv.vehicle_id  \n" +
            "left join uc.t_vrms_repair_factory factory on factory.id = rs.repair_factory_id \n" +
            "left join uc.t_vrms_repair_sheet_zuche_info tvrszi on rs.id = tvrszi.repair_sheet_id \n" +
            "left join zuche.t_rm_bill trb on tvrszi.zuche_repair_no = trb.bill_no and trb.is_delete = 0 \n" +
            "left join uc.t_vrms_repair_project_type tvrptzc on tvrszi.repair_project_type_id = tvrptzc.id \n" +
            "left join uc.t_vrms_repair_sheet_pro_re tvrspr on rs.id = tvrspr.repair_sheet_id \n" +
            "  and tvrspr.is_deleted = 0 and tvrspr.id is not null \n" +
            "left join uc.t_vrms_repair_sheet_component_re tvrscr on rs.id = tvrscr.repair_sheet_id \n" +
            "  and  tvrspr.id = tvrscr.repair_project_id  and tvrscr.is_deleted = 0 \n" +
            "left join uc.t_vrms_repair_project tvrp on tvrspr.repair_project_id = tvrp.id \n" +
            "left join uc.t_vrms_repair_workshop tvrw on tvrspr.out_repair_workshop = tvrw.id  \n" +
            "left join tvrh15 t15 on t15.repair_sheet_id = rs.id \n" +
            "left join tvrh19 t19 on t19.repair_sheet_id = rs.id \n" +
            "left join tvrh5 t5 on t5.repair_sheet_id = rs.id \n" +
            "where tvrspr.id is not null and rs.is_deleted = 0 and rs.STATUS = 7 \n" +
            "union all \n" +
            "select  rs.id as id,  -- 维修单id\n" +
            "    rs.repair_sheet_no as repairSheetNo, -- 维修单号 \n" +
            "    tvrszi.zuche_repair_no as zucheRepairNo, -- 租车维修单号\n" +
            "    tvrszi.costs_attach_city_name as costsAttCityName,-- 成本归属城市\n" +
            "    factory.factory_name as factoryName, -- 维修厂名称   \n" +
            "    case rs.repair_mode \n" +
            "        WHEN 1 THEN '进厂维修'\n" +
            "        WHEN 2 THEN '上门维保'\n" +
            "        WHEN 3 THEN '只定损' \n" +
            "        WHEN 4 THEN '补单' \n" +
            "        ELSE '' \n" +
            "    end as repairMode, -- 维修方式 1进厂维修 2上门维保 3只定损 4补单 \n" +
            "    case tvrszi.send_repair_type \n" +
            "        WHEN 1 THEN '自主送取' \n" +
            "        WHEN 2 THEN '上门取送' \n" +
            "        ELSE '' \n" +
            "    end as sendRepairType, -- 送修方式 1自主送取 2上门取送 \n" +
            "    case when \n" +
            "        rs.repair_vehicle_id > 0 \n" +
            "        then \n" +
            "        bvrv.vehicle_nature \n" +
            "        else \n" +
            "        bvv.vehicle_nature \n" +
            "    end as vehicleNature, -- 车辆性质    \n" +
            "    --case when \n" +
            "    --  rs.repair_vehicle_id > 0 \n" +
            "    --  then \n" +
            "    --  bvrv.vehicle_purpose \n" +
            "    --  else \n" +
            "    --  bvv.vehicle_purpose \n" +
            "    --end as vehiclePurpose, -- 车辆用途\n" +
            "    case trb.ordercar \n" +
            "            when 0 then '短租' \n" +
            "            when 1 then '长租' \n" +
            "            when 3 then '融资租赁' \n" +
            "            when 4 then '试驾' \n" +
            "            when 5 then '公务车' \n" +
            "            when 7 then '专车' \n" +
            "            when 8 then '优驾' \n" +
            "    end as vehiclePurpose,--车辆用途\n" +
            "    case when \n" +
            "        rs.repair_vehicle_id > 0 \n" +
            "        then \n" +
            "        bvrv.vehicle_no \n" +
            "        else \n" +
            "        bvv.vehicle_no \n" +
            "    end as vehicleNo, -- 维修车辆车牌号\n" +
            "    case when \n" +
            "        rs.repair_vehicle_id > 0 \n" +
            "        then \n" +
            "        bvrv.frame_no \n" +
            "        else \n" +
            "        bvv.frame_no \n" +
            "    end as frameNo, -- 维修车辆车架号\n" +
            "    case when \n" +
            "        rs.repair_vehicle_id > 0 \n" +
            "        then \n" +
            "        bvrv.model_name \n" +
            "        else \n" +
            "        bvv.model_name \n" +
            "    end as modelName, -- 车型名称\n" +
            "    case rs.repair_sheet_source \n" +
            "        WHEN 1 THEN '租车' \n" +
            "        WHEN 2 THEN '专车' \n" +
            "        WHEN 3 THEN '闪贷' \n" +
            "        WHEN 4 THEN '买买车'  \n" +
            "        ELSE '' \n" +
            "    end as orderSource, -- 订单来源: 1 租车 2专车 3闪贷 4买买车\n" +
            "    tvrptzc.type_name as zcTypeName, -- 租车工单类型    \n" +
            "    case tvrspr.payment_type \n" +
            "        WHEN 1 THEN '客户付费' \n" +
            "        WHEN 2 THEN '保险付费' \n" +
            "        WHEN 3 THEN '公司付费'  \n" +
            "        ELSE '' \n" +
            "    end  as paymentType,-- 付费类型 1客户付费 2保险付费 3公司付费\n" +
            "    tvrspr.project_type_name as projectTypeName, -- 项目类型名称\n" +
            "    tvrp.project_name as projectName, -- 项目名称\n" +
            "    tvrspr.repair_worker_name as repairWorkerName, -- 主修人\n" +
            "    tvrspr.work_time as workTime, -- 工时\n" +
            "    tvrspr.univalence as univalence, -- 单价\n" +
            "    tvrspr.work_time_fee as workTimeFree,-- 工时费\n" +
            "    tvrspr.zuche_loss_assessment_money as zucheLossAssMoney, -- 租车定损金额\n" +
            "    case tvrspr.care_outside  \n" +
            "        WHEN 0 THEN '否' \n" +
            "        WHEN 1 THEN '是' \n" +
            "    end as careOutside, -- 托外(0:未选择,1:选择)\n" +
            "    tvrw.workshop_name as workshopName, -- 外托维修厂\n" +
            "    tvrspr.out_money as outMoney, -- 外托金额\n" +
            "    tvrspr.claim_nos as claimNos, -- 关联理赔单号\n" +
            "    tvrscr.component_name as componentName, -- 配件名称\n" +
            "    tvrscr.specification as specification, -- 配件规格\n" +
            "    tvrscr.unit as unit, -- 单位\n" +
            "    tvrscr.component_num as componentNum, -- 配件数量\n" +
            "    tvrscr.univalence as componentUnivalence, -- 配件单价\n" +
            "    tvrscr.component_fee as componentFree, -- 配件费\n" +
            "    from_unixtime(unix_timestamp(tvrszi.return_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as returnDate, -- 实际还车时间\n" +
            "    case WHEN rs.repair_mode = 1 AND tvrszi.send_repair_type=1  THEN from_unixtime(unix_timestamp(t15.create_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         WHEN rs.repair_mode = 1 AND tvrszi.send_repair_type=2 THEN from_unixtime(unix_timestamp(tvrszi.return_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         WHEN rs.repair_mode = 3  then from_unixtime(unix_timestamp(t19.create_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         WHEN rs.repair_mode = 2  then from_unixtime(unix_timestamp(t5.create_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         WHEN rs.repair_mode = 4  then from_unixtime(unix_timestamp(t5.create_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         ELSE from_unixtime(unix_timestamp(rs.update_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "    end as endTime,-- 工单结束时间\n" +
            "    tvrscr.component_fee as projectFree, -- 项目费用\n" +
            "    case rs.type  \n" +
            "        WHEN 0 THEN '无' \n" +
            "        WHEN 1 THEN '整备' \n" +
            "        WHEN 2 THEN '保险' \n" +
            "        WHEN 3 THEN '维修' \n" +
            "        WHEN 4 THEN '整车检查' \n" +
            "        WHEN 5 THEN 'PDI检测' \n" +
            "        WHEN 6 THEN '常规保养' \n" +
            "        WHEN 7 THEN '免费保养' \n" +
            "        WHEN 8 THEN 'GPS安装' \n" +
            "        WHEN 9 THEN '精洗美容' \n" +
            "        WHEN 10 THEN '深化养护' \n" +
            "        WHEN 11 THEN '精品装璜' \n" +
            "        WHEN 12 THEN '市场营销' \n" +
            "        WHEN 13 THEN '机电维修' \n" +
            "        WHEN 14 THEN '故障诊断' \n" +
            "        WHEN 15 THEN '事故车维修（不含保险）' \n" +
            "        WHEN 16 THEN '保修'\n" +
            "        ELSE '' \n" +
            "    end as repairType, -- 维修类别.1:维修,2:整备\n" +
            "    case tvrscr.is_reduce\n" +
            "        when 0 then '否'\n" +
            "        when 1 then '是'\n" +
            "    end as is_reduce, -- 是否做过减项\n" +
            "    case tvrscr.repair_conclusion\n" +
            "        when 0 then '无需维修'\n" +
            "        when 1 then '需要维修'\n" +
            "    end as repair_conclusion -- 维修结论\n" +
            "from uc.t_vrms_repair_sheet rs\n" +
            "left join bi_mmc.bas_vrms_vehicle bvv on rs.vehicle_id = bvv.vehicle_id \n" +
            "left join bi_mmc.bas_vrms_repair_vehicle bvrv on rs.repair_vehicle_id = bvrv.vehicle_id  \n" +
            "left join uc.t_vrms_repair_factory factory on factory.id = rs.repair_factory_id \n" +
            "left join uc.t_vrms_repair_sheet_zuche_info tvrszi on rs.id = tvrszi.repair_sheet_id \n" +
            "left join zuche.t_rm_bill trb on tvrszi.zuche_repair_no = trb.bill_no and trb.is_delete = 0 \n" +
            "left join uc.t_vrms_repair_project_type tvrptzc on tvrszi.repair_project_type_id = tvrptzc.id \n" +
            "left join uc.t_vrms_repair_sheet_pro_re tvrspr on rs.id = tvrspr.repair_sheet_id and tvrspr.id is not null \n" +
            "left join uc.t_vrms_repair_sheet_component_re tvrscr on rs.id = tvrscr.repair_sheet_id \n" +
            "   and tvrscr.is_deleted = 0 and tvrscr.repair_project_id is not null \n" +
            "left join uc.t_vrms_repair_project tvrp on tvrspr.repair_project_id = tvrp.id \n" +
            "left join uc.t_vrms_repair_workshop tvrw on tvrspr.out_repair_workshop = tvrw.id  \n" +
            "left join tvrh15 t15 on t15.repair_sheet_id = rs.id \n" +
            "left join tvrh19 t19 on t19.repair_sheet_id = rs.id \n" +
            "left join tvrh5 t5 on t5.repair_sheet_id = rs.id \n" +
            "where tvrspr.id is null and tvrscr.repair_project_id is not null and rs.is_deleted = 0 and rs.STATUS = 7 \n" +
            "union all \n" +
            "select  rs.id as id,  -- 维修单id\n" +
            "    rs.repair_sheet_no as repairSheetNo, -- 维修单号 \n" +
            "    tvrszi.zuche_repair_no as zucheRepairNo, -- 租车维修单号\n" +
            "    tvrszi.costs_attach_city_name as costsAttCityName,-- 成本归属城市\n" +
            "    factory.factory_name as factoryName, -- 维修厂名称   \n" +
            "    case rs.repair_mode \n" +
            "        WHEN 1 THEN '进厂维修'\n" +
            "        WHEN 2 THEN '上门维保'\n" +
            "        WHEN 3 THEN '只定损' \n" +
            "        WHEN 4 THEN '补单' \n" +
            "        ELSE '' \n" +
            "    end as repairMode, -- 维修方式 1进厂维修 2上门维保 3只定损 4补单 \n" +
            "    case tvrszi.send_repair_type \n" +
            "        WHEN 1 THEN '自主送取' \n" +
            "        WHEN 2 THEN '上门取送' \n" +
            "        ELSE '' \n" +
            "    end as sendRepairType, -- 送修方式 1自主送取 2上门取送 \n" +
            "    case when \n" +
            "        rs.repair_vehicle_id > 0 \n" +
            "        then \n" +
            "        bvrv.vehicle_nature \n" +
            "        else \n" +
            "        bvv.vehicle_nature \n" +
            "    end as vehicleNature, -- 车辆性质    \n" +
            "    --case when \n" +
            "    --  rs.repair_vehicle_id > 0 \n" +
            "    --  then \n" +
            "    --  bvrv.vehicle_purpose \n" +
            "    --  else \n" +
            "    --  bvv.vehicle_purpose \n" +
            "    --end as vehiclePurpose, -- 车辆用途\n" +
            "    case trb.ordercar \n" +
            "            when 0 then '短租' \n" +
            "            when 1 then '长租' \n" +
            "            when 3 then '融资租赁' \n" +
            "            when 4 then '试驾' \n" +
            "            when 5 then '公务车' \n" +
            "            when 7 then '专车' \n" +
            "            when 8 then '优驾' \n" +
            "    end as vehiclePurpose,--车辆用途\n" +
            "    case when \n" +
            "        rs.repair_vehicle_id > 0 \n" +
            "        then \n" +
            "        bvrv.vehicle_no \n" +
            "        else \n" +
            "        bvv.vehicle_no \n" +
            "    end as vehicleNo, -- 维修车辆车牌号\n" +
            "    case when \n" +
            "        rs.repair_vehicle_id > 0 \n" +
            "        then \n" +
            "        bvrv.frame_no \n" +
            "        else \n" +
            "        bvv.frame_no \n" +
            "    end as frameNo, -- 维修车辆车架号\n" +
            "    case when \n" +
            "        rs.repair_vehicle_id > 0 \n" +
            "        then \n" +
            "        bvrv.model_name \n" +
            "        else \n" +
            "        bvv.model_name \n" +
            "    end as modelName, -- 车型名称\n" +
            "    case rs.repair_sheet_source \n" +
            "        WHEN 1 THEN '租车' \n" +
            "        WHEN 2 THEN '专车' \n" +
            "        WHEN 3 THEN '闪贷' \n" +
            "        WHEN 4 THEN '买买车'  \n" +
            "        ELSE '' \n" +
            "    end as orderSource, -- 订单来源: 1 租车 2专车 3闪贷 4买买车\n" +
            "    tvrptzc.type_name as zcTypeName, -- 租车工单类型    \n" +
            "    case tvrspr.payment_type \n" +
            "        WHEN 1 THEN '客户付费' \n" +
            "        WHEN 2 THEN '保险付费' \n" +
            "        WHEN 3 THEN '公司付费'  \n" +
            "        ELSE '' \n" +
            "    end  as paymentType,-- 付费类型 1客户付费 2保险付费 3公司付费\n" +
            "    '' as projectTypeName, -- 项目类型名称\n" +
            "    tvrap.additional_project_name as projectName, -- 附件项目名称\n" +
            "    '' as repairWorkerName, -- 主修人\n" +
            "    '' as workTime, -- 工时\n" +
            "    '' as univalence, -- 单价\n" +
            "    tvrspr.additional_project_fee as workTimeFree,-- 金额\n" +
            "    tvrspr.zuche_loss_assessment_money as zucheLossAssMoney, -- 租车定损金额\n" +
            "    case tvrspr.care_outside  \n" +
            "        WHEN 0 THEN '否' \n" +
            "        WHEN 1 THEN '是' \n" +
            "    end as careOutside, -- 托外(0:未选择,1:选择)\n" +
            "    tvrw.workshop_name as workshopName, -- 外托维修厂\n" +
            "    tvrspr.out_money as outMoney, -- 外托金额\n" +
            "    tvrspr.claim_nos as claimNos, -- 关联理赔单号\n" +
            "    '' as componentName, -- 配件名称\n" +
            "    '' as specification, -- 配件规格\n" +
            "    '' as unit, -- 单位\n" +
            "    '' as componentNum, -- 配件数量\n" +
            "    '' as componentUnivalence, -- 配件单价\n" +
            "    '' as componentFree, -- 配件费\n" +
            "    from_unixtime(unix_timestamp(tvrszi.return_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as returnDate, -- 实际还车时间\n" +
            "    case WHEN rs.repair_mode = 1 AND tvrszi.send_repair_type=1  THEN from_unixtime(unix_timestamp(t15.create_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         WHEN rs.repair_mode = 1 AND tvrszi.send_repair_type=2 THEN from_unixtime(unix_timestamp(tvrszi.return_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         WHEN rs.repair_mode = 3  then from_unixtime(unix_timestamp(t19.create_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         WHEN rs.repair_mode = 2  then from_unixtime(unix_timestamp(t5.create_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') \n" +
            "         WHEN rs.repair_mode = 4  then from_unixtime(unix_timestamp(t5.create_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')  \n" +
            "         ELSE from_unixtime(unix_timestamp(rs.update_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss')   \n" +
            "    end as endTime,-- 工单结束时间\n" +
            "    tvrspr.additional_project_fee as projectFree, -- 项目费用\n" +
            "    case rs.type  \n" +
            "        WHEN 0 THEN '无' \n" +
            "        WHEN 1 THEN '整备' \n" +
            "        WHEN 2 THEN '保险' \n" +
            "        WHEN 3 THEN '维修' \n" +
            "        WHEN 4 THEN '整车检查' \n" +
            "        WHEN 5 THEN 'PDI检测' \n" +
            "        WHEN 6 THEN '常规保养' \n" +
            "        WHEN 7 THEN '免费保养' \n" +
            "        WHEN 8 THEN 'GPS安装' \n" +
            "        WHEN 9 THEN '精洗美容' \n" +
            "        WHEN 10 THEN '深化养护' \n" +
            "        WHEN 11 THEN '精品装璜' \n" +
            "        WHEN 12 THEN '市场营销' \n" +
            "        WHEN 13 THEN '机电维修' \n" +
            "        WHEN 14 THEN '故障诊断' \n" +
            "        WHEN 15 THEN '事故车维修（不含保险）' \n" +
            "        WHEN 16 THEN '保修'\n" +
            "        ELSE '' \n" +
            "    end as repairType, -- 维修类别.1:维修,2:整备\n" +
            "    case tvrspr.is_reduce\n" +
            "        when 0 then '否'\n" +
            "        when 1 then '是'\n" +
            "    end as is_reduce, -- 是否做过减项\n" +
            "    case tvrspr.repair_conclusion\n" +
            "        when 0 then '无需维修'\n" +
            "        when 1 then '需要维修'\n" +
            "    end as repair_conclusion -- 维修结论\n" +
            "from uc.t_vrms_repair_sheet rs\n" +
            "left join bi_mmc.bas_vrms_vehicle bvv on rs.vehicle_id = bvv.vehicle_id \n" +
            "left join bi_mmc.bas_vrms_repair_vehicle bvrv on rs.repair_vehicle_id = bvrv.vehicle_id  \n" +
            "left join uc.t_vrms_repair_factory factory on factory.id = rs.repair_factory_id \n" +
            "left join uc.t_vrms_repair_sheet_zuche_info tvrszi on rs.id = tvrszi.repair_sheet_id \n" +
            "left join zuche.t_rm_bill trb on tvrszi.zuche_repair_no = trb.bill_no and trb.is_delete = 0 \n" +
            "left join uc.t_vrms_repair_project_type tvrptzc on tvrszi.repair_project_type_id = tvrptzc.id \n" +
            "left join uc.t_vrms_repair_sheet_additional_project_re tvrspr on rs.id = tvrspr.repair_sheet_id \n" +
            "  and tvrspr.is_deleted = 0 and tvrspr.id is not null \n" +
            "left join uc.t_vrms_repair_workshop tvrw on tvrspr.out_repair_workshop = tvrw.id \n" +
            "left join uc.t_vrms_repair_additional_project tvrap on tvrspr.additional_project_id = tvrap.id \n" +
            "left join tvrh15 t15 on t15.repair_sheet_id = rs.id \n" +
            "left join tvrh19 t19 on t19.repair_sheet_id = rs.id \n" +
            "left join tvrh5 t5 on t5.repair_sheet_id = rs.id \n" +
            "where tvrspr.id is not null and rs.is_deleted = 0 and rs.STATUS = 7 ) a where  a.endTime >='$sDate' and a.endTime <= '$eDate'\n";


    public static final String SQL_09 = "-- 上上七天活跃用户\n" +
            "with blw AS(\n" +
            "select \n" +
            " app_version, -- app版本\n" +
            " case when device_channel = '' then '未知'else device_channel end device_channel, -- 渠道\n" +
            " device_type, -- 系统类型\n" +
            " user_id\n" +
            "from bi_ucar.bas_app_behavior_event \n" +
            "where dt >= date_add('$sDate',-20) and dt <= date_add('$sDate',-14) and event_code = 'APP_start'\n" +
            "group by app_version,device_channel,device_type,user_id\n" +
            "),\n" +
            "-- 上七天活跃用户\n" +
            "lw AS(\n" +
            "select\n" +
            " app_version, -- app版本\n" +
            " case when device_channel = '' then '未知'else device_channel end device_channel, -- 渠道\n" +
            " device_type, -- 系统类型 \n" +
            " user_id\n" +
            "from bi_ucar.bas_app_behavior_event \n" +
            "where dt >= date_add('$sDate',-13) and dt <= date_add('$sDate',-7) and event_code = 'APP_start'\n" +
            "group by app_version,device_channel,device_type,user_id\n" +
            "),\n" +
            "-- 近七天活跃用户\n" +
            "tw AS(\n" +
            "select \n" +
            " app_version, -- app版本\n" +
            " case when device_channel = '' then '未知'else device_channel end device_channel, -- 渠道\n" +
            " device_type, -- 系统类型\n" +
            " user_id\n" +
            "from bi_ucar.bas_app_behavior_event \n" +
            "where dt >= date_add('$sDate',-6) and dt <= '$sDate' and event_code = 'APP_start'\n" +
            "group by app_version,device_channel,device_type,user_id\n" +
            "),\n" +
            "\n" +
            "-- 上上七天活跃上七天不活跃用户\n" +
            "lwna AS (\n" +
            "select\n" +
            " t1.app_version, -- app版本\n" +
            " t1.device_channel, -- 渠道\n" +
            " t1.device_type, -- 系统类型 \n" +
            " t1.user_id\n" +
            "from blw t1\n" +
            "left join lw t2 on t1.app_version = t2.app_version and t1.device_channel = t2.device_channel and t1.device_type = t2.device_type and t1.user_id = t2.user_id\n" +
            "where  t2.user_id is null\n" +
            "),\n" +
            "-- 回流用户数（上上七天活跃上七天不活跃近七天又活跃的用户数）\n" +
            "bu AS (\n" +
            "select\n" +
            " t1.app_version, -- app版本\n" +
            " t1.device_channel, -- 渠道\n" +
            " t1.device_type, -- 系统类型 \n" +
            " count(1) back_num\n" +
            "from lwna t1\n" +
            "inner join tw t2 on t1.app_version = t2.app_version and t1.device_channel = t2.device_channel and t1.device_type = t2.device_type and t1.user_id = t2.user_id\n" +
            "group by  t1.app_version, t1.device_channel, t1.device_type\n" +
            "),\n" +
            "-- 流失用户(上七天有近七天没有活跃的用户数)\n" +
            "lu AS (\n" +
            "select\n" +
            " t1.app_version, -- app版本\n" +
            " t1.device_channel, -- 渠道\n" +
            " t1.device_type, -- 系统类型 \n" +
            " count(1) lost_num\n" +
            "from lw t1\n" +
            "left join tw t2 on t1.app_version = t2.app_version and t1.device_channel = t2.device_channel and t1.device_type = t2.device_type and t1.user_id = t2.user_id\n" +
            "where  t2.user_id is null\n" +
            "group by  t1.app_version, t1.device_channel, t1.device_type\n" +
            ")\n" +
            "\n" +
            "insert OVERWRITE TABLE  bi_ucar.topic_user_back_lost_stat PARTITION(stat_date) \n" +
            "select \n" +
            " t1.app_version, -- app版本\n" +
            " t1.device_channel, -- 渠道\n" +
            " t1.device_type, -- 系统类型 \n" +
            " t1.back_num,\n" +
            " COALESCE(t2.lost_num,0) lost_num,\n" +
            " '$sDate' stat_date\n" +
            "from bu t1\n" +
            "left join lu t2 on t1.app_version = t2.app_version and t1.device_channel = t2.device_channel and t1.device_type = t2.device_type\n" +
            "union all\n" +
            "select \n" +
            " t1.app_version, -- app版本\n" +
            " t1.device_channel, -- 渠道\n" +
            " t1.device_type, -- 系统类型 \n" +
            "  0 back_num,\n" +
            " t1.lost_num,\n" +
            " '$sDate' stat_date\n" +
            "from lu t1\n" +
            "left join bu t2 on t1.app_version = t2.app_version and t1.device_channel = t2.device_channel and t1.device_type = t2.device_type\n" +
            "where t2.back_num is null";

    public static final String SQL_08 = "with dau AS( \n" +
            "select \n" +
            "app_version, \n" +
            "case when device_channel = '' then '66'else device_channel end device_channel, \n" +
            "device_type, \n" +
            "count(distinct user_id) cnt \n" +
            ",oo.order_id\n" +
            ",hh.order_type\n" +
            ",hh.order_num\n" +
            ",cc.city_id\n" +
            ",dd.dept_id\n" +
            "from bi_ucar.bas_app_behavior_event ee \n" +
            "left join (\n" +
            "select \n" +
            "order_id\n" +
            ",order_type\n" +
            ",order_num\n" +
            "from (\n" +
            "select \n" +
            "order_id\n" +
            ",order_type\n" +
            ",sum(order_cnt)order_num\n" +
            "from \n" +
            "default.t_scd_order_h\n" +
            "group by order_id , order_type )   table_order_num \n" +
            ") hh\n" +
            "on ee.order_id = hh.order_id \n" +
            "left join default.fact_city_order cc\n" +
            "on ee.order_id = cc.order_id \n" +
            "left join default.fact_dept_order dd\n" +
            "on ee.order_id = cc.order_id \n" +
            "left join default.t_scd_order oo \n" +
            "on ee.order_id = oo.order_id \n" +
            "where dt = '$sDate' and event_code = 'APP_start'\n" +
            "group by app_version,device_channel,device_type\n" +
            "),\n" +
            "wau AS (\n" +
            "select \n" +
            "app_version, \n" +
            "case when device_channel = '' then '66'else device_channel end device_channel,\n" +
            "device_type, \n" +
            "count(distinct user_id) cnt \n" +
            "from dau \n" +
            "where dt >= date_add('$sDate',-6) and dt <= '$sDate' and event_code = 'APP_start'\n" +
            "group by app_version,device_channel,device_type\n" +
            "),\n" +
            "mau AS (\n" +
            "select \n" +
            "app_version, \n" +
            "case when device_channel = '' then '66'else device_channel end device_channel, \n" +
            "device_type, \n" +
            "count(distinct user_id) cnt \n" +
            "from bi_ucar.bas_app_behavior_event  \n" +
            "where dt >= date_add('$sDate',-30) and dt <= '$sDate' and event_code = 'APP_start'\n" +
            "group by app_version,device_channel,device_type\n" +
            ")\n" +
            "\n" +
            "insert OVERWRITE TABLE  bi_ucar.topic_active_user_stat PARTITION(stat_date) \n" +
            "select \n" +
            "mau.app_version,\n" +
            "mau.device_channel,\n" +
            "mau.device_type,\n" +
            "COALESCE(dau.cnt,0) dau,\n" +
            "COALESCE(wau.cnt,0) wau,\n" +
            "COALESCE(mau.cnt,0) mau,\n" +
            "'$sDate' stat_date\n" +
            "from mau\n" +
            "left join wau on mau.app_version = wau.app_version and mau.device_channel = wau.device_channel and mau.device_type = wau.device_type\n" +
            "left join dau on mau.app_version = dau.app_version and mau.device_channel = dau.device_channel and mau.device_type = dau.device_type";

    public static final String SQL_07 = "insert overwrite table bi_zuche.fact_sco_member_debt_money PARTITION(dt)\n" +
            "select\n" +
            " order_id\n" +
            ",order_no\n" +
            ",member_id\n" +
            ",member.name\n" +
            ",order_status\n" +
            ",sum(rent_debt_money)rent_debt_money\n" +
            ",sum(violation_debt_money)violation_debt_money\n" +
            ",sum(maintenance_debt_money)maintenance_debt_money\n" +
            ",exempt_type --免押类型\n" +
            ",exempt_status\n" +
            ",substring(date_sub(to_date(CONCAT(substring(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') ,0, 10) , ' 00:00:00')),1) , 0  , 10) dt\n" +
            "from (\n" +
            "select \n" +
            " balance_id\n" +
            ",balance_no\n" +
            ",order_id\n" +
            ",order_no\n" +
            ",member_id\n" +
            ",balance_type\n" +
            ",income_receivable\n" +
            ",income_received\n" +
            ",case balance_type  when 1 then   arear_amount  else 0 end rent_debt_money\n" +
            ",case balance_type  when 2 then   arear_amount  else 0 end violation_debt_money\n" +
            ",case balance_type  when 3 then   arear_amount  else 0 end maintenance_debt_money\n" +
            ",exempt_type\n" +
            ",order_status\n" +
            ",exempt_status\n" +
            "from (\n" +
            "select \n" +
            "balance.id balance_id\n" +
            ",balance.balance_no\n" +
            ",balance.order_id\n" +
            ",balance.order_no\n" +
            ",balance.member_id\n" +
            ",balance.balance_type\n" +
            ",income_receivable\n" +
            ",income_received\n" +
            ",arear_amount\n" +
            ",exempt_type\n" +
            ",order_status\n" +
            ",exempt_status\n" +
            ",row_number() over(partition by balance.balance_type ,balance.order_id  order by  balance.balance_no desc) max_row\n" +
            " from \n" +
            "zuche.t_sbalance_statement balance\n" +
            "LEFT JOIN zuche.t_spay_order_fi  pay_order_fi\n" +
            "on balance.order_id = pay_order_fi.order_id \n" +
            " ) t  where max_row = 1 ) dept \n" +
            "left join bi_zuche.bas_member member \n" +
            "on dept.member_id = member.id\n" +
            "where rent_debt_money> 0 or  violation_debt_money> 0 or  maintenance_debt_money> 0 \n" +
            "group by \n" +
            "order_id\n" +
            ",order_no\n" +
            ",member_id\n" +
            ",member.name\n" +
            ",balance_type\n" +
            ",exempt_type\n" +
            ",order_status\n" +
            ",exempt_status";

    public static final String SQL_06 = "with A as ( -- t_vrms_repair_sheet\n" +
            "    select\n" +
            "        t1.repair_sheet_no `维修单号`,\n" +
            "        case t1.repair_mode \n" +
            "            when 1 then '进厂维修'\n" +
            "            when 2 then '上门维保'\n" +
            "            when 3 then '只定损'\n" +
            "            when 4 then '补单'\n" +
            "        end `维修方式`, \n" +
            "        t1.origin_km `进厂里程`,\n" +
            "        t1.leave_repair_km `离厂里程`,\n" +
            "        t1.id,\n" +
            "        t1.update_date `工单完成时间`,\n" +
            "        t1.repair_vehicle_id,\n" +
            "        t1.vehicle_id,\n" +
            "        t1.repair_factory_id,\n" +
            "        t1.dept_id,\n" +
            "        t1.dept_name `维修单所属门店`,\n" +
            "        t3.name `维修单所属城市`\n" +
            "    from\n" +
            "        uc.t_vrms_repair_sheet t1\n" +
            "    left join\n" +
            "        uc.t_department t2\n" +
            "    on\n" +
            "        t1.dept_id = t2.id\n" +
            "    left join\n" +
            "        uc.t_b_city t3\n" +
            "    on\n" +
            "        t2.local_city = t3.id\n" +
            "    where\n" +
            "        t1.status = 7 -- 已结束\n" +
            "    and\n" +
            "        t1.is_deleted = 0 -- 未删除\n" +
            "    and\n" +
            "    t1.update_date >= '$sDate'\n" +
            "    and\n" +
            "    t1.update_date < '$eDate'\n" +
            "),\n" +
            "B as (SELECT \n" +
            "      repair_sheet_id,\n" +
            "      SUM(payment1) AS payment1,\n" +
            "      SUM(payment2) AS payment2,\n" +
            "      SUM(payment3) AS payment3 \n" +
            "    FROM(SELECT \n" +
            "      repair_sheet_id,\n" +
            "      SUM(payment1) + SUM(payment2)  AS payment1,\n" +
            "      0 AS payment2,\n" +
            "      0 AS payment3 \n" +
            "    FROM\n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        IF(\n" +
            "          r.care_outside = 1,\n" +
            "          SUM(\n" +
            "            IF(r.out_money IS NULL, 0, r.out_money)\n" +
            "          ),\n" +
            "          SUM(\n" +
            "            IF(\n" +
            "              r.work_time_fee IS NULL,\n" +
            "              0,\n" +
            "              r.work_time_fee\n" +
            "            ) + IF(trc.fee IS NULL, 0, trc.fee)\n" +
            "          )\n" +
            "        ) AS payment1,\n" +
            "        0 AS payment2\n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_pro_re r \n" +
            "        LEFT JOIN \n" +
            "          (SELECT \n" +
            "            repair_project_id,\n" +
            "            SUM(rc.component_fee) AS fee \n" +
            "          FROM\n" +
            "            uc.t_vrms_repair_sheet_component_re rc \n" +
            "          WHERE rc.is_deleted = 0 \n" +
            "          GROUP BY repair_project_id) trc \n" +
            "          ON r.id = trc.repair_project_id \n" +
            "      WHERE payment_type = 2 \n" +
            "        AND r.is_deleted = 0 \n" +
            "        AND r.is_reduce = 0 \n" +
            "        AND (\n" +
            "          r.repair_conclusion IS NULL \n" +
            "          OR r.repair_conclusion = 1\n" +
            "        ) \n" +
            "      GROUP BY r.repair_sheet_id,\n" +
            "        r.care_outside \n" +
            "      UNION\n" +
            "      SELECT \n" +
            "        repair_sheet_id,\n" +
            "        0 AS payment1,\n" +
            "        IF(\n" +
            "          r.care_outside = 1,\n" +
            "          SUM(\n" +
            "            IF(r.out_money IS NULL, 0, r.out_money)\n" +
            "          ),\n" +
            "          SUM(\n" +
            "            IF(\n" +
            "              r.additional_project_fee IS NULL,\n" +
            "              0,\n" +
            "              r.additional_project_fee\n" +
            "            )\n" +
            "          )\n" +
            "        ) AS payment2 \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_additional_project_re r \n" +
            "      WHERE payment_type = 2 \n" +
            "        AND r.is_deleted = 0 \n" +
            "        AND r.is_reduce = 0 \n" +
            "        AND (\n" +
            "          r.repair_conclusion IS NULL \n" +
            "          OR r.repair_conclusion = 1\n" +
            "        ) \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        care_outside) AS repair_sheet_payment1 \n" +
            "    GROUP BY repair_sheet_id \n" +
            "    UNION\n" +
            "    SELECT \n" +
            "      repair_sheet_id,\n" +
            "      0 AS payment1,\n" +
            "      SUM(payment1) + SUM(payment2)  AS payment2,\n" +
            "      0 AS payment3 \n" +
            "    FROM\n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        IF(\n" +
            "          r.care_outside = 1,\n" +
            "          SUM(\n" +
            "            IF(r.out_money IS NULL, 0, r.out_money)\n" +
            "          ),\n" +
            "          SUM(\n" +
            "            IF(\n" +
            "              r.work_time_fee IS NULL,\n" +
            "              0,\n" +
            "              r.work_time_fee\n" +
            "            ) + IF(trc.fee IS NULL, 0, trc.fee)\n" +
            "          )\n" +
            "        ) AS payment1,\n" +
            "        0 AS payment2\n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_pro_re r \n" +
            "        LEFT JOIN \n" +
            "          (SELECT \n" +
            "            repair_project_id,\n" +
            "            SUM(rc.component_fee) AS fee \n" +
            "          FROM\n" +
            "            uc.t_vrms_repair_sheet_component_re rc \n" +
            "          WHERE rc.is_deleted = 0 \n" +
            "          GROUP BY repair_project_id) trc \n" +
            "          ON r.id = trc.repair_project_id \n" +
            "      WHERE payment_type = 1 \n" +
            "        AND r.is_deleted = 0 \n" +
            "        AND r.is_reduce = 0 \n" +
            "        AND (\n" +
            "          r.repair_conclusion IS NULL \n" +
            "          OR r.repair_conclusion = 1\n" +
            "        ) \n" +
            "      GROUP BY r.repair_sheet_id,\n" +
            "        r.care_outside \n" +
            "      UNION\n" +
            "      SELECT \n" +
            "        repair_sheet_id,\n" +
            "        0 AS payment1,\n" +
            "        IF(\n" +
            "          r.care_outside = 1,\n" +
            "          SUM(\n" +
            "            IF(r.out_money IS NULL, 0, r.out_money)\n" +
            "          ),\n" +
            "          SUM(\n" +
            "            IF(\n" +
            "              r.additional_project_fee IS NULL,\n" +
            "              0,\n" +
            "              r.additional_project_fee\n" +
            "            )\n" +
            "          )\n" +
            "        ) AS payment2 \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_additional_project_re r \n" +
            "      WHERE payment_type = 1 \n" +
            "        AND r.is_deleted = 0 \n" +
            "        AND r.is_reduce = 0 \n" +
            "        AND (\n" +
            "          r.repair_conclusion IS NULL \n" +
            "          OR r.repair_conclusion = 1\n" +
            "        ) \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        r.care_outside) AS repair_sheet_payment2 \n" +
            "    GROUP BY repair_sheet_id \n" +
            "    UNION\n" +
            "    SELECT \n" +
            "      repair_sheet_id,\n" +
            "      0 AS payment1,\n" +
            "      0 AS payment2,\n" +
            "      SUM(payment1) + SUM(payment2)  AS payment3 \n" +
            "    FROM\n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        IF(\n" +
            "          r.care_outside = 1,\n" +
            "          SUM(\n" +
            "            IF(r.out_money IS NULL, 0, r.out_money)\n" +
            "          ),\n" +
            "          SUM(\n" +
            "            IF(\n" +
            "              r.work_time_fee IS NULL,\n" +
            "              0,\n" +
            "              r.work_time_fee\n" +
            "            ) + IF(trc.fee IS NULL, 0, trc.fee)\n" +
            "          )\n" +
            "        ) AS payment1,\n" +
            "        0 AS payment2\n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_pro_re r \n" +
            "        LEFT JOIN \n" +
            "          (SELECT \n" +
            "            repair_project_id,\n" +
            "            SUM(rc.component_fee) AS fee \n" +
            "          FROM\n" +
            "            uc.t_vrms_repair_sheet_component_re rc \n" +
            "          WHERE rc.is_deleted = 0 \n" +
            "          GROUP BY repair_project_id) trc \n" +
            "          ON r.id = trc.repair_project_id \n" +
            "      WHERE payment_type = 3 \n" +
            "        AND r.is_deleted = 0 \n" +
            "        AND r.is_reduce = 0 \n" +
            "        AND (\n" +
            "          r.repair_conclusion IS NULL \n" +
            "          OR r.repair_conclusion = 1\n" +
            "        ) \n" +
            "      GROUP BY r.repair_sheet_id,\n" +
            "        r.care_outside \n" +
            "      UNION\n" +
            "      SELECT \n" +
            "        repair_sheet_id,\n" +
            "        0 AS payment1,\n" +
            "        IF(\n" +
            "          r.care_outside = 1,\n" +
            "          SUM(\n" +
            "            IF(r.out_money IS NULL, 0, r.out_money)\n" +
            "          ),\n" +
            "          SUM(\n" +
            "            IF(\n" +
            "              r.additional_project_fee IS NULL,\n" +
            "              0,\n" +
            "              r.additional_project_fee\n" +
            "            )\n" +
            "          )\n" +
            "        ) AS payment2 \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_additional_project_re r \n" +
            "      WHERE payment_type = 3 \n" +
            "        AND r.is_deleted = 0 \n" +
            "        AND r.is_reduce = 0 \n" +
            "        AND (\n" +
            "          r.repair_conclusion IS NULL \n" +
            "          OR r.repair_conclusion = 1\n" +
            "        ) \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        r.care_outside) AS repair_sheet_payment3 \n" +
            "    GROUP BY repair_sheet_id) AS repair_sheet_payment \n" +
            "    GROUP BY repair_sheet_id),\n" +
            "C as ( -- t_vrms_repair_sheet_zuche_info \n" +
            "    select\n" +
            "        t1.zuche_repair_no `租车维保单号`,\n" +
            "        t1.return_date `还车时间`,\n" +
            "        t1.apply_city_name `工单申请城市`,\n" +
            "        t1.apply_dept_name `工单申请门店`,\n" +
            "        t1.park_dept_name `停放租车门店`,\n" +
            "        t1.park_city_name `停放城市`,\n" +
            "        t1.costs_attach_city_name `成本归属城市`,\n" +
            "        t1.costs_attach_dept_name `成本归属门店`,\n" +
            "        case t1.send_repair_type\n" +
            "            when 1 then '自主送取'\n" +
            "            when 2 then '上门取送'\n" +
            "        end `送修方式`,\n" +
            "        t1.claim_nos `理赔单号`,\n" +
            "        t1.repair_sheet_id,\n" +
            "        t1.repair_project_type_id\n" +
            "    from\n" +
            "        uc.t_vrms_repair_sheet_zuche_info t1\n" +
            "\n" +
            "),\n" +
            "D as ( -- t_rm_bill\n" +
            "    select \n" +
            "        t1.id,\n" +
            "        t1.bill_no,\n" +
            "        case t1.ordercar \n" +
            "            when 0 then '短租' \n" +
            "            when 1 then '长租' \n" +
            "            when 3 then '融资租赁' \n" +
            "            when 4 then '试驾' \n" +
            "            when 5 then '公务车' \n" +
            "            when 7 then '专车' \n" +
            "            when 8 then '优驾' \n" +
            "        end `车辆用途`,\n" +
            "        t3.model_name `车型`\n" +
            "    from \n" +
            "        zuche.t_rm_bill t1\n" +
            "    left join\n" +
            "        zuche.vehicle t2\n" +
            "    on\n" +
            "        t1.vehicle_id = t2.xid\n" +
            "    left join\n" +
            "        zuche.t_v_model t3\n" +
            "    on\n" +
            "        t2.model_id = t3.id\n" +
            "),\n" +
            "E as ( -- t_vrms_repair_vehicle\n" +
            "    select\n" +
            "        t1.id,\n" +
            "        t1.vehicle_no,\n" +
            "        t1.model_id,\n" +
            "        case t1.vehicle_nature\n" +
            "            when 1 then '神州租车'\n" +
            "            when 2 then '神州专车'\n" +
            "            when 3 then '神州闪贷'\n" +
            "            when 4 then '神州畅行'\n" +
            "            when 5 then '买买车二手车'\n" +
            "            when 6 then '外部车辆'\n" +
            "        end `车辆性质` -- 2018年2月2日新增字段 车辆性质\n" +
            "    from\n" +
            "        uc.t_vrms_repair_vehicle t1\n" +
            "),\n" +
            "F as ( -- t_vrms_vehicle\n" +
            "    select\n" +
            "        t1.id,\n" +
            "        t1.vehicle_no,\n" +
            "        t1.model_id,\n" +
            "        case t1.vehicle_nature\n" +
            "            when 1 then '神州租车'\n" +
            "            when 2 then '神州专车'\n" +
            "            when 3 then '神州闪贷'\n" +
            "            when 4 then '神州畅行'\n" +
            "            when 5 then '买买车二手车'\n" +
            "            when 6 then '外部车辆'\n" +
            "        end `车辆性质` -- 2018年2月2日新增字段 车辆性质\n" +
            "    from\n" +
            "        uc.t_vrms_vehicle t1\n" +
            "),\n" +
            "-- G as ( -- t_v_model\n" +
            "--     select\n" +
            "--         t1.id,\n" +
            "--         t1.model_name `车型`\n" +
            "--     from\n" +
            "--         uc.t_v_model t1\n" +
            "-- ),\n" +
            "H as ( -- t_vrms_repair_factory\n" +
            "    select\n" +
            "        t1.id,\n" +
            "        t1.factory_name `维修厂`,\n" +
            "        t2.aptitude_name `维修厂资质`\n" +
            "    from\n" +
            "        uc.t_vrms_repair_factory t1\n" +
            "    left join\n" +
            "        uc.t_vrms_repair_factory_aptitude t2\n" +
            "    on\n" +
            "        t1.aptitude_id = t2.id\n" +
            "),\n" +
            "I as ( -- t_vrms_repair_his\n" +
            "    select distinct\n" +
            "        t1.id,\n" +
            "        t1.create_date,\n" +
            "        t1.creator_name,\n" +
            "        t1.dept_name,\n" +
            "        t1.type,\n" +
            "        t1.status,\n" +
            "        t1.repair_sheet_id\n" +
            "    from \n" +
            "        uc.t_vrms_repair_his t1\n" +
            "),\n" +
            "J as ( -- t_vrms_repair_project_type\n" +
            "    select\n" +
            "        t1.id,\n" +
            "        t1.type_name `租车工单类型`\n" +
            "    from\n" +
            "        uc.t_vrms_repair_project_type t1\n" +
            "),\n" +
            "K as ( -- t_vrms_repair_project && t_vrms_repair_sheet_pro_re\n" +
            "    select\n" +
            "        t1.repair_sheet_id,\n" +
            "        case when count(case when t2.update_mileage = 1 then 1 else null end) > 0 then '是' else '否' end `是否更新待保养里程`\n" +
            "    from\n" +
            "        uc.t_vrms_repair_sheet_pro_re t1\n" +
            "    left join\n" +
            "        uc.t_vrms_repair_project t2\n" +
            "    on\n" +
            "        t1.repair_project_id = t2.id\n" +
            "    group by \n" +
            "        t1.repair_sheet_id\n" +
            "),\n" +
            "L as ( -- 2018年2月2日新增字段 工单新建人\n" +
            "    select\n" +
            "        t1.repair_sheet_id,\n" +
            "        t1.creator_name `工单新建人`\n" +
            "    from\n" +
            "        I t1\n" +
            "    where\n" +
            "        t1.type=1\n" +
            "),\n" +
            "TIME_01_01 as ( -- 方案通过操作人-前置\n" +
            "    select\n" +
            "        t1.repair_sheet_id,\n" +
            "        max(t1.id) as id\n" +
            "    from\n" +
            "        I t1\n" +
            "    where\n" +
            "        t1.type = 4\n" +
            "    group by \n" +
            "        t1.repair_sheet_id\n" +
            "),\n" +
            "TIME_01 as ( -- 方案通过操作人\n" +
            "    select \n" +
            "        t1.repair_sheet_id,\n" +
            "        t2.creator_name `方案通过操作人`,\n" +
            "        t2.dept_name `方案通过操作人部门`\n" +
            "    from\n" +
            "        TIME_01_01 t1\n" +
            "    inner join\n" +
            "        I t2\n" +
            "    on\n" +
            "        t1.id = t2.id\n" +
            "),\n" +
            "TIME_02_01 as ( -- 首次提报方案时间,获取首次提报方案的时间和历史维修单记录的id,为下一步获取 首次提报方案操作人 和 首次提报方案操作部门 做准备\n" +
            "    select\n" +
            "        t1.repair_sheet_id,\n" +
            "        min(t1.id) as id\n" +
            "    from\n" +
            "        I t1\n" +
            "    where\n" +
            "        t1.type = 14\n" +
            "    group by \n" +
            "        t1.repair_sheet_id\n" +
            "),\n" +
            "TIME_02 as ( -- 首次提报方案时间\n" +
            "    select \n" +
            "        t1.repair_sheet_id,\n" +
            "        t2.creator_name `首次提报方案操作人`,\n" +
            "        t2.dept_name `首次提报方案操作部门`\n" +
            "    from\n" +
            "        TIME_02_01 t1\n" +
            "    inner join\n" +
            "        I t2\n" +
            "    on\n" +
            "        t1.id = t2.id\n" +
            "),\n" +
            "-- TIME_04 as ( -- 维修完毕操作时间\n" +
            "--     select\n" +
            "--         t1.repair_sheet_id,\n" +
            "--         min(t1.create_date) `维修完毕操作时间`\n" +
            "--     from\n" +
            "--         I t1\n" +
            "--     where\n" +
            "--         t1.type = 5\n" +
            "--     group by \n" +
            "--         t1.repair_sheet_id\n" +
            "-- ),\n" +
            "-- TIME_05 as ( -- 离场操作时间\n" +
            "--     select\n" +
            "--         t1.repair_sheet_id,\n" +
            "--         min(t1.create_date) `离场操作时间`\n" +
            "--     from\n" +
            "--         I t1\n" +
            "--     where\n" +
            "--         t1.type = 15\n" +
            "--     group by \n" +
            "--         t1.repair_sheet_id\n" +
            "-- ),\n" +
            "-- TIME_08 as ( -- 确认定损操作时间\n" +
            "--     select\n" +
            "--         t1.repair_sheet_id,\n" +
            "--         min(t1.create_date) `确认定损操作时间`\n" +
            "--     from\n" +
            "--         I t1\n" +
            "--     where\n" +
            "--         t1.type = 19\n" +
            "--     group by \n" +
            "--         t1.repair_sheet_id\n" +
            "-- ),\n" +
            "-- TIME_09 as ( -- 变更成维修单结束的时间\n" +
            "--     select\n" +
            "--         t1.repair_sheet_id,\n" +
            "--         min(t1.create_date) `变更成维修单结束的时间`\n" +
            "--     from\n" +
            "--         I t1\n" +
            "--     where\n" +
            "--         t1.status = 7\n" +
            "--     group by \n" +
            "--         t1.repair_sheet_id\n" +
            "-- ),\n" +
            "FEE_01 as ( -- 保养项目金额\n" +
            "SELECT \n" +
            "  SUM(fee) AS fee,\n" +
            "  repair_sheet_id \n" +
            "FROM\n" +
            "  (SELECT \n" +
            "    r.repair_sheet_id,\n" +
            "    CASE\n" +
            "      WHEN r.care_outside = 1 \n" +
            "      THEN SUM(r.out_money + coalesce(fee, 0)) \n" +
            "      ELSE SUM(work_time_fee + coalesce(fee, 0)) \n" +
            "    END AS fee \n" +
            "  FROM\n" +
            "    uc.t_vrms_repair_sheet_pro_re r \n" +
            "    LEFT JOIN uc.t_vrms_repair_project p \n" +
            "      ON p.id = r.repair_project_id \n" +
            "    LEFT JOIN uc.t_vrms_repair_project_type tp \n" +
            "      ON tp.id = p.project_type_id \n" +
            "    LEFT JOIN \n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        repair_project_id,\n" +
            "        SUM(coalesce(rc.component_fee, 0)) AS fee \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_component_re rc \n" +
            "      WHERE rc.is_deleted = 0 \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        repair_project_id) trc \n" +
            "      ON r.id = trc.repair_project_id \n" +
            "  WHERE r.is_deleted = 0 \n" +
            "    AND r.is_reduce = 0 \n" +
            "    AND payment_type != 2\n" +
            "    AND tp.id = 4\n" +
            "  GROUP BY r.repair_sheet_id,\n" +
            "    r.care_outside) ttt\n" +
            "GROUP BY repair_sheet_id \n" +
            "),\n" +
            "FEE_02 as ( -- 故障维修项目金额\n" +
            "SELECT \n" +
            "  SUM(fee) AS fee,\n" +
            "  repair_sheet_id \n" +
            "FROM\n" +
            "  (SELECT \n" +
            "    r.repair_sheet_id,\n" +
            "    CASE\n" +
            "      WHEN r.care_outside = 1 \n" +
            "      THEN SUM(r.out_money + coalesce(fee, 0)) \n" +
            "      ELSE SUM(work_time_fee + coalesce(fee, 0)) \n" +
            "    END AS fee \n" +
            "  FROM\n" +
            "    uc.t_vrms_repair_sheet_pro_re r \n" +
            "    LEFT JOIN uc.t_vrms_repair_project p \n" +
            "      ON p.id = r.repair_project_id \n" +
            "    LEFT JOIN uc.t_vrms_repair_project_type tp \n" +
            "      ON tp.id = p.project_type_id \n" +
            "    LEFT JOIN \n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        repair_project_id,\n" +
            "        SUM(coalesce(rc.component_fee, 0)) AS fee \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_component_re rc \n" +
            "      WHERE rc.is_deleted = 0 \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        repair_project_id) trc \n" +
            "      ON r.id = trc.repair_project_id \n" +
            "  WHERE r.is_deleted = 0 \n" +
            "    AND r.is_reduce = 0 \n" +
            "    AND payment_type != 2\n" +
            "    AND tp.id = 5 \n" +
            "  GROUP BY r.repair_sheet_id,\n" +
            "    r.care_outside) ttt\n" +
            "GROUP BY repair_sheet_id \n" +
            "),\n" +
            "FEE_03 as ( -- 事故项目金额\n" +
            "SELECT \n" +
            "  SUM(fee) AS fee,\n" +
            "  repair_sheet_id \n" +
            "FROM\n" +
            "  (SELECT \n" +
            "    r.repair_sheet_id,\n" +
            "    CASE\n" +
            "      WHEN r.care_outside = 1 \n" +
            "      THEN SUM(r.out_money + coalesce(fee, 0)) \n" +
            "      ELSE SUM(work_time_fee + coalesce(fee, 0)) \n" +
            "    END AS fee \n" +
            "  FROM\n" +
            "    uc.t_vrms_repair_sheet_pro_re r \n" +
            "    LEFT JOIN uc.t_vrms_repair_project p \n" +
            "      ON p.id = r.repair_project_id \n" +
            "    LEFT JOIN uc.t_vrms_repair_project_type tp \n" +
            "      ON tp.id = p.project_type_id \n" +
            "    LEFT JOIN \n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        repair_project_id,\n" +
            "        SUM(coalesce(rc.component_fee, 0)) AS fee \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_component_re rc \n" +
            "      WHERE rc.is_deleted = 0 \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        repair_project_id) trc \n" +
            "      ON r.id = trc.repair_project_id \n" +
            "  WHERE r.is_deleted = 0 \n" +
            "    AND r.is_reduce = 0 \n" +
            "    AND \n" +
            "    CASE\n" +
            "      WHEN payment_type != 2 \n" +
            "      THEN tp.id = 7 \n" +
            "      ELSE 1 = 1 \n" +
            "    END \n" +
            "  GROUP BY r.repair_sheet_id,\n" +
            "    r.care_outside) ttt\n" +
            "GROUP BY repair_sheet_id \n" +
            "),\n" +
            "FEE_04 as ( -- 美容项目金额\n" +
            "SELECT \n" +
            "  SUM(fee) AS fee,\n" +
            "  repair_sheet_id \n" +
            "FROM\n" +
            "  (SELECT \n" +
            "    r.repair_sheet_id,\n" +
            "    CASE\n" +
            "      WHEN r.care_outside = 1 \n" +
            "      THEN SUM(r.out_money + coalesce(fee, 0)) \n" +
            "      ELSE SUM(work_time_fee + coalesce(fee, 0)) \n" +
            "    END AS fee \n" +
            "  FROM\n" +
            "    uc.t_vrms_repair_sheet_pro_re r \n" +
            "    LEFT JOIN uc.t_vrms_repair_project p \n" +
            "      ON p.id = r.repair_project_id \n" +
            "    LEFT JOIN uc.t_vrms_repair_project_type tp \n" +
            "      ON tp.id = p.project_type_id \n" +
            "    LEFT JOIN \n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        repair_project_id,\n" +
            "        SUM(coalesce(rc.component_fee, 0)) AS fee \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_component_re rc \n" +
            "      WHERE rc.is_deleted = 0 \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        repair_project_id) trc \n" +
            "      ON r.id = trc.repair_project_id \n" +
            "  WHERE r.is_deleted = 0 \n" +
            "    AND r.is_reduce = 0 \n" +
            "    AND payment_type != 2\n" +
            "    AND tp.id = 1 \n" +
            "  GROUP BY r.repair_sheet_id,\n" +
            "    r.care_outside) ttt\n" +
            "GROUP BY repair_sheet_id \n" +
            "),\n" +
            "FEE_05 as ( -- 外观维修项目金额\n" +
            "SELECT \n" +
            "  SUM(fee) AS fee,\n" +
            "  repair_sheet_id \n" +
            "FROM\n" +
            "  (SELECT \n" +
            "    r.repair_sheet_id,\n" +
            "    CASE\n" +
            "      WHEN r.care_outside = 1 \n" +
            "      THEN SUM(r.out_money + coalesce(fee, 0)) \n" +
            "      ELSE SUM(work_time_fee + coalesce(fee, 0)) \n" +
            "    END AS fee \n" +
            "  FROM\n" +
            "    uc.t_vrms_repair_sheet_pro_re r \n" +
            "    LEFT JOIN uc.t_vrms_repair_project p \n" +
            "      ON p.id = r.repair_project_id \n" +
            "    LEFT JOIN uc.t_vrms_repair_project_type tp \n" +
            "      ON tp.id = p.project_type_id \n" +
            "    LEFT JOIN \n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        repair_project_id,\n" +
            "        SUM(coalesce(rc.component_fee, 0)) AS fee \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_component_re rc \n" +
            "      WHERE rc.is_deleted = 0 \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        repair_project_id) trc \n" +
            "      ON r.id = trc.repair_project_id \n" +
            "  WHERE r.is_deleted = 0 \n" +
            "    AND r.is_reduce = 0 \n" +
            "    AND payment_type != 2\n" +
            "    AND tp.id = 3 \n" +
            "  GROUP BY r.repair_sheet_id,\n" +
            "    r.care_outside) ttt\n" +
            "GROUP BY repair_sheet_id \n" +
            "),\n" +
            "FEE_06 as ( -- 易损件维修项目金额\n" +
            "SELECT \n" +
            "  SUM(fee) AS fee,\n" +
            "  repair_sheet_id \n" +
            "FROM\n" +
            "  (SELECT \n" +
            "    r.repair_sheet_id,\n" +
            "    CASE\n" +
            "      WHEN r.care_outside = 1 \n" +
            "      THEN SUM(r.out_money + coalesce(fee, 0)) \n" +
            "      ELSE SUM(work_time_fee + coalesce(fee, 0)) \n" +
            "    END AS fee \n" +
            "  FROM\n" +
            "    uc.t_vrms_repair_sheet_pro_re r \n" +
            "    LEFT JOIN uc.t_vrms_repair_project p \n" +
            "      ON p.id = r.repair_project_id \n" +
            "    LEFT JOIN uc.t_vrms_repair_project_type tp \n" +
            "      ON tp.id = p.project_type_id \n" +
            "    LEFT JOIN \n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        repair_project_id,\n" +
            "        SUM(coalesce(rc.component_fee, 0)) AS fee \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_component_re rc \n" +
            "      WHERE rc.is_deleted = 0 \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        repair_project_id) trc \n" +
            "      ON r.id = trc.repair_project_id \n" +
            "  WHERE r.is_deleted = 0 \n" +
            "    AND r.is_reduce = 0 \n" +
            "    AND payment_type != 2\n" +
            "    AND tp.id = 2 \n" +
            "  GROUP BY r.repair_sheet_id,\n" +
            "    r.care_outside) ttt\n" +
            "GROUP BY repair_sheet_id \n" +
            "),\n" +
            "FEE_07 as ( -- 附加项目金额-保险 附加项目金额-客户 附加项目金额-公司\n" +
            "    SELECT\n" +
            "        t1.repair_sheet_id,\n" +
            "        sum(case when t1.payment_type = 1 then (t1.out_money+t1.additional_project_fee) else 0 end) `附加项目金额-客户`,\n" +
            "        sum(case when t1.payment_type = 2 then (t1.out_money+t1.additional_project_fee) else 0 end) `附加项目金额-保险`,\n" +
            "        sum(case when t1.payment_type = 3 then (t1.out_money+t1.additional_project_fee) else 0 end) `附加项目金额-公司`\n" +
            "    FROM\n" +
            "        uc.t_vrms_repair_sheet_additional_project_re t1\n" +
            "    where\n" +
            "        t1.is_deleted = 0\n" +
            "    and\n" +
            "        t1.is_reduce = 0\n" +
            "    group by\n" +
            "        t1.repair_sheet_id\n" +
            "),\n" +
            "Result as (\n" +
            "    select\n" +
            "        A.`维修单号`,\n" +
            "        A.`维修方式`, \n" +
            "        A.`进厂里程`,\n" +
            "        A.`离厂里程`,\n" +
            "        B.payment1 `理赔款付费金额` ,\n" +
            "        B.payment2 `合同付费金额` ,\n" +
            "        B.payment3 `公司付费金额` ,\n" +
            "        B.payment1 + B.payment2 + B.payment3 `实际费用` ,\n" +
            "        FEE_01.fee `保养项目金额` ,\n" +
            "        FEE_02.fee `故障维修项目金额` ,\n" +
            "        FEE_03.fee `事故项目金额` ,\n" +
            "        FEE_04.fee `美容项目金额` ,\n" +
            "        FEE_05.fee `外观维修项目金额` ,\n" +
            "        FEE_06.fee `易损件维修项目金额` ,\n" +
            "        C.`租车维保单号`,\n" +
            "        C.`还车时间`,\n" +
            "        C.`工单申请城市`,\n" +
            "        C.`工单申请门店`,\n" +
            "        C.`停放租车门店`,\n" +
            "        C.`停放城市`,\n" +
            "        C.`成本归属城市`,\n" +
            "        C.`成本归属门店`,\n" +
            "        D.`车辆用途`,\n" +
            "        coalesce(E.vehicle_no,F.vehicle_no) `车牌号`,\n" +
            "        D.`车型`,\n" +
            "        H.`维修厂`,\n" +
            "        A.`工单完成时间`,\n" +
            "        J.`租车工单类型`,\n" +
            "        TIME_02.`首次提报方案操作人`,\n" +
            "        TIME_02.`首次提报方案操作部门`,\n" +
            "        TIME_01.`方案通过操作人`,\n" +
            "        TIME_01.`方案通过操作人部门`,\n" +
            "        C.`理赔单号`,\n" +
            "        K.`是否更新待保养里程`,\n" +
            "        A.`维修单所属门店`,\n" +
            "        A.`维修单所属城市`,\n" +
            "        FEE_07.`附加项目金额-客户`,\n" +
            "        FEE_07.`附加项目金额-保险`,\n" +
            "        FEE_07.`附加项目金额-公司`,\n" +
            "        H.`维修厂资质`,\n" +
            "        case when A.repair_vehicle_id is not null then E.`车辆性质` else F.`车辆性质` end `车辆性质`, -- 优先使用t_vrms_repair_vehicle表\n" +
            "        L.`工单新建人`\n" +
            "    from\n" +
            "        A \n" +
            "    left join\n" +
            "        B\n" +
            "    on\n" +
            "        A.id = B.repair_sheet_id\n" +
            "    left join\n" +
            "        C\n" +
            "    on\n" +
            "        A.id = C.repair_sheet_id\n" +
            "    left join\n" +
            "        D\n" +
            "    on\n" +
            "        C.`租车维保单号` = D.bill_no\n" +
            "    left join\n" +
            "        E\n" +
            "    on\n" +
            "        A.repair_vehicle_id = E.id\n" +
            "    left join\n" +
            "        F\n" +
            "    on\n" +
            "        A.vehicle_id = F.id\n" +
            "    -- left join\n" +
            "    --     G\n" +
            "    -- on\n" +
            "    --     G.id = case when A.repair_vehicle_id is not null then E.model_id else F.model_id end -- 优先使用t_vrms_repair_vehicle表的model_id\n" +
            "    left join\n" +
            "        H\n" +
            "    on\n" +
            "        A.repair_factory_id = H.id\n" +
            "    left join\n" +
            "        TIME_01\n" +
            "    on\n" +
            "        TIME_01.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        TIME_02\n" +
            "    on\n" +
            "        TIME_02.repair_sheet_id = A.id\n" +
            "    -- left join\n" +
            "    --  TIME_04\n" +
            "    -- on\n" +
            "    --  TIME_04.repair_sheet_id = A.id\n" +
            "    -- left join\n" +
            "    --  TIME_05\n" +
            "    -- on\n" +
            "    --  TIME_05.repair_sheet_id = A.id\n" +
            "    -- left join\n" +
            "    --  TIME_08\n" +
            "    -- on\n" +
            "    --  TIME_08.repair_sheet_id = A.id\n" +
            "    -- left join\n" +
            "    --  TIME_09\n" +
            "    -- on\n" +
            "    --  TIME_09.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        J\n" +
            "    on\n" +
            "        J.id = C.repair_project_type_id\n" +
            "    left join\n" +
            "        K\n" +
            "    on\n" +
            "        K.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_01\n" +
            "    on\n" +
            "        Fee_01.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_02\n" +
            "    on\n" +
            "        Fee_02.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_03\n" +
            "    on\n" +
            "        Fee_03.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_04\n" +
            "    on\n" +
            "        Fee_04.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_05\n" +
            "    on\n" +
            "        Fee_05.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_06\n" +
            "    on\n" +
            "        Fee_06.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_07\n" +
            "    on\n" +
            "        Fee_07.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        L\n" +
            "    on\n" +
            "        L.repair_sheet_id = A.id\n" +
            "\n" +
            ")\n" +
            "insert into table bi_mmc.rpt_vrms_fee_statistics_all\n" +
            "select \n" +
            "    t1.`维修厂`,\n" +
            "    t1.`维修单号`,\n" +
            "    t1.`租车维保单号`,\n" +
            "    t1.`车牌号`,\n" +
            "    t1.`车型`,\n" +
            "    t1.`还车时间`,\n" +
            "    t1.`工单申请城市`,\n" +
            "    t1.`工单申请门店`,\n" +
            "    t1.`停放租车门店`,\n" +
            "    t1.`停放城市`,\n" +
            "    t1.`维修方式`,\n" +
            "    t1.`理赔单号`,\n" +
            "    t1.`车辆用途`,\n" +
            "    t1.`进厂里程`,\n" +
            "    t1.`离厂里程`,\n" +
            "    t1.`理赔款付费金额`,\n" +
            "    t1.`理赔款付费金额`, -- t1.`理赔款维保支出额度`, 相同口径\n" +
            "    t1.`合同付费金额`,\n" +
            "    t1.`公司付费金额`,\n" +
            "    t1.`实际费用`,\n" +
            "    t1.`保养项目金额`,\n" +
            "    t1.`故障维修项目金额`,\n" +
            "    t1.`事故项目金额`,\n" +
            "    t1.`美容项目金额`,\n" +
            "    t1.`外观维修项目金额`,\n" +
            "    t1.`易损件维修项目金额`,\n" +
            "    t1.`是否更新待保养里程`,\n" +
            "    t1.`首次提报方案操作人`,\n" +
            "    t1.`首次提报方案操作部门`,\n" +
            "    t1.`方案通过操作人`,\n" +
            "    t1.`方案通过操作人部门`,\n" +
            "    t1.`工单完成时间`,\n" +
            "    t1.`成本归属城市`,\n" +
            "    t1.`成本归属门店`,\n" +
            "    t1.`租车工单类型`,\n" +
            "    null, -- id\n" +
            "    t1.`维修单所属门店`,\n" +
            "    t1.`维修单所属城市`,\n" +
            "    t1.`附加项目金额-保险`,\n" +
            "    t1.`附加项目金额-客户`,\n" +
            "    t1.`附加项目金额-公司`,\n" +
            "    t1.`维修厂资质`, -- 2018年2月2日新增字段\n" +
            "    t1.`车辆性质`, -- 2018年2月2日新增字段\n" +
            "    t1.`工单新建人` -- 2018年2月2日新增字段\n" +
            "from \n" +
            "    Result t1\n";



    public static final String SQL_05 = "with a as (\n" +
            "\tselect distinct \n" +
            "\t\tt1.user_id uid,\n" +
            "\t\tt1.device_imei dimei\n" +
            "\tfrom \n" +
            "\t\tbi_ucar.bas_app_behavior_event t1 \n" +
            "\twhere \n" +
            "\t\tdt >='$sDate'\n" +
            "\tand\n" +
            "\t\tdt < '$eDate' \n" +
            "\tand \n" +
            "\t\t(length(trim(t1.user_id))>0 and t1.user_id is not null)\n" +
            "\tand \n" +
            "\t\t(length(trim(t1.device_imei))>0 and t1.device_imei is not null)\n" +
            "),\n" +
            "b as (\n" +
            "\tselect\n" +
            "\t\t*\n" +
            "\tfrom\n" +
            "\t\ta t1\n" +
            "\tleft join\n" +
            "\t\tbi_ucar.bas_mapping_user_device t2\n" +
            "\ton\n" +
            "\t\tt1.uid = cast(user_id as string)\n" +
            "\tand\n" +
            "\t\tt1.dimei = device_imei\n" +
            "\twhere\n" +
            "\t\tuser_id is null\n" +
            ")\n" +
            "INSERT into table bi_ucar.bas_mapping_user_device\n" +
            "select \n" +
            "\tuser_id,\n" +
            "\tcoalesce(device_imei,uid)\n" +
            "from b";


    public static final String SQL_04 = "with a as (\n" +
            "\tselect distinct \n" +
            "\t\tt1.user_id uid,\n" +
            "\t\tt1.device_imei dimei\n" +
            "\tfrom \n" +
            "\t\tbi_ucar.bas_app_behavior_event t1 \n" +
            "\twhere \n" +
            "\t\tdt >='$sDate'\n" +
            "\tand\n" +
            "\t\tdt < '$eDate' \n" +
            "\tand \n" +
            "\t\t(length(trim(t1.user_id))>0 and t1.user_id is not null)\n" +
            "\tand \n" +
            "\t\t(length(trim(t1.device_imei))>0 and t1.device_imei is not null)\n" +
            "),\n" +
            "b as (\n" +
            "\tselect\n" +
            "\t\t*\n" +
            "\tfrom\n" +
            "\t\ta t1\n" +
            "\tleft join\n" +
            "\t\tbi_ucar.bas_mapping_user_device t2\n" +
            "\ton\n" +
            "\t\tt1.uid = cast(user_id as string)\n" +
            "\tand\n" +
            "\t\tt1.dimei = device_imei\n" +
            "\twhere\n" +
            "\t\tuser_id is null\n" +
            ")\n" +
            "INSERT into table bi_ucar.bas_mapping_user_device\n" +
            "select \n" +
            "\tuser_id,\n" +
            "\tcoalesce(device_imei,uid) ussus\n" +
            "from b";

    public static final String SQL_03 = "with a as (\n" +
            "\tselect distinct \n" +
            "\t\tt1.user_id uid,\n" +
            "\t\tt1.device_imei dimei\n" +
            "\tfrom \n" +
            "\t\tbi_ucar.bas_app_behavior_event t1 \n" +
            "\twhere \n" +
            "\t\tdt >='$sDate'\n" +
            "\tand\n" +
            "\t\tdt < '$eDate' \n" +
            "\tand \n" +
            "\t\t(length(trim(t1.user_id))>0 and t1.user_id is not null)\n" +
            "\tand \n" +
            "\t\t(length(trim(t1.device_imei))>0 and t1.device_imei is not null)\n" +
            "),\n" +
            "b as (\n" +
            "\tselect\n" +
            "\t\tt2.*,\n" +
            "        t1.*\n" +
            "\tfrom\n" +
            "\t\ta t1\n" +
            "\tleft join\n" +
            "\t\tbi_ucar.bas_mapping_user_device t2\n" +
            "\ton\n" +
            "\t\tt1.uid = cast(user_id as string)\n" +
            "\tand\n" +
            "\t\tt1.dimei = device_imei\n" +
            "\twhere\n" +
            "\t\tuser_id is null\n" +
            ")\n" +
            "INSERT into table bi_ucar.bas_mapping_user_device\n" +
            "select \n" +
            "\tuser_id,\n" +
            "\tcoalesce(device_imei,uid) ussus\n" +
            "from b";


    public static final String SQL_02 = "with a as (\n" +
            "\tselect distinct \n" +
            "\t\tt1.user_id uid,\n" +
            "\t\tt1.device_imei dimei\n" +
            "\tfrom \n" +
            "\t\tbi_ucar.bas_app_behavior_event t1 \n" +
            "\twhere \n" +
            "\t\tdt >='$sDate'\n" +
            "\tand\n" +
            "\t\tdt < '$eDate' \n" +
            "\tand \n" +
            "\t\t(length(trim(t1.user_id))>0 and t1.user_id is not null)\n" +
            "\tand \n" +
            "\t\t(length(trim(t1.device_imei))>0 and t1.device_imei is not null)\n" +
            "),\n" +
            "b as (\n" +
            "\tselect\n" +
            "\t\t*\n" +
            "\tfrom\n" +
            "\t\ta t1\n" +
            "\tleft join\n" +
            "\t\tbi_ucar.bas_mapping_user_device\n" +
            "\ton\n" +
            "\t\tt1.uid = cast(user_id as string)\n" +
            "\tand\n" +
            "\t\tt1.dimei = device_imei\n" +
            "\twhere\n" +
            "\t\tuser_id is null\n" +
            ")\n" +
            "INSERT into table bi_ucar.bas_mapping_user_device\n" +
            "select \n" +
            "\tuser_id,\n" +
            "\tcoalesce(device_imei,uid) ussus\n" +
            "from b";




    public static final String SQL_01 = "with A as ( -- 开始结束时间内的注册用户明细\n" +
            "    select distinct\n" +
            "        t1.id,\n" +
            "        date(t1.create_time) as regist_date\n" +
            "    from\n" +
            "        bi_ucar.bas_member t1\n" +
            "    where\n" +
            "        t1.create_time >= date('$start_date')\n" +
            "    and\n" +
            "        t1.create_time < date_add(date('$end_date'),1)\n" +
            "),\n" +
            "BB as ( -- 启动app的事件明细\n" +
            "    select distinct\n" +
            "        t1.user_id,\n" +
            "        t1.device_imei,\n" +
            "        if(length(trim(t1.device_channel))=0 or t1.device_channel is null or trim(t1.device_channel)='other','other', trim(t1.device_channel)) device_channel,\n" +
            "        t1.device_type,\n" +
            "        date(t1.dt) as app_start_date\n" +
            "    from\n" +
            "        bi_ucar.bas_app_behavior_event t1\n" +
            "    where\n" +
            "        t1.event_code='APP_start'\n" +
            "    and\n" +
            "        date(t1.dt) >= date('$start_date')\n" +
            "    and\n" +
            "        date(t1.dt) <= date_add(date('$end_date'),30)\n" +
            "),\n" +
            "B as ( -- 关联mapping表,补充user_id\n" +
            "    select distinct\n" +
            "        coalesce(t1.user_id, cast(t2.user_id as string)) user_id,\n" +
            "        t1.device_imei,\n" +
            "        t1.device_channel,\n" +
            "        t1.device_type,\n" +
            "        t1.app_start_date\n" +
            "    from\n" +
            "        BB t1\n" +
            "    left join\n" +
            "        bi_ucar.bas_mapping_user_device t2\n" +
            "    on\n" +
            "        t1.device_imei = t2.device_imei\n" +
            "),\n" +
            "B0 as ( -- 用户的设备渠道,设备类型\n" +
            "    select distinct\n" +
            "        t1.user_id,\n" +
            "        t1.device_channel,\n" +
            "        t1.device_type\n" +
            "    from\n" +
            "        B t1\n" +
            "),\n" +
            "B1 as ( -- 每个注册日，每个渠道，每个设备类型 的注册用户信息\n" +
            "    select distinct\n" +
            "        t1.regist_date,\n" +
            "        coalesce(t2.device_channel, 'other') device_channel,\n" +
            "        coalesce(t2.device_type, 'other') device_type,\n" +
            "        t1.id\n" +
            "    from\n" +
            "        A t1\n" +
            "    left join\n" +
            "        B0 t2\n" +
            "    on\n" +
            "        cast(t1.id as string) = t2.user_id\n" +
            "),\n" +
            "B2 as ( -- 计算每天，每个渠道，每个设备类型 的去重注册用户数\n" +
            "    select\n" +
            "        t1.regist_date,\n" +
            "        t1.device_channel,\n" +
            "        t1.device_type,\n" +
            "        count(distinct t1.id) as total_users\n" +
            "    from\n" +
            "        B1 t1\n" +
            "    group by\n" +
            "        t1.regist_date,\n" +
            "        t1.device_channel,\n" +
            "        t1.device_type\n" +
            "),\n" +
            "B3 as ( -- 参数表\n" +
            "    select 1 as after_days \n" +
            "    union all\n" +
            "    select 2 as after_days \n" +
            "    union all\n" +
            "    select 3 as after_days \n" +
            "    union all\n" +
            "    select 4 as after_days \n" +
            "    union all\n" +
            "    select 5 as after_days \n" +
            "    union all\n" +
            "    select 6 as after_days \n" +
            "    union all\n" +
            "    select 7 as after_days \n" +
            "    union all\n" +
            "    select 14 as after_days\n" +
            "    union all\n" +
            "    select 30 as after_days\n" +
            "),\n" +
            "B4 as ( -- 计算间隔日期\n" +
            "    select\n" +
            "        t1.regist_date,\n" +
            "        t1.device_channel,\n" +
            "        t1.device_type,\n" +
            "        t1.total_users,\n" +
            "        t2.after_days,\n" +
            "        date_add(t1.regist_date,t2.after_days) app_start_date\n" +
            "    from\n" +
            "        B2 t1\n" +
            "    inner join\n" +
            "        B3 t2\n" +
            "    on\n" +
            "        1=1\n" +
            "),\n" +
            "C as ( -- 获取注册用户在后期每日的启动情况\n" +
            "    select\n" +
            "        t1.regist_date,\n" +
            "        t2.app_start_date,\n" +
            "        coalesce(t2.device_channel,'other') device_channel,\n" +
            "        coalesce(t2.device_type,'other') device_type,\n" +
            "        count(distinct t2.user_id) app_start_channel_users\n" +
            "    from\n" +
            "        B1 t1\n" +
            "    left join\n" +
            "        B t2\n" +
            "    on\n" +
            "        cast(t1.id as string) = t2.user_id\n" +
            "    and\n" +
            "        t1.device_channel = t2.device_channel\n" +
            "    and\n" +
            "        t1.device_type = t2.device_type\n" +
            "    where\n" +
            "        t2.app_start_date is not null\n" +
            "    and\n" +
            "        t1.regist_date < t2.app_start_date\n" +
            "    group by\n" +
            "        t1.regist_date,\n" +
            "        t2.app_start_date,\n" +
            "        t2.device_channel,\n" +
            "        t2.device_type\n" +
            "),\n" +
            "D as ( -- 获取注册日后x天的用户启动情况\n" +
            "    select\n" +
            "        t1.regist_date,\n" +
            "        t1.app_start_date,\n" +
            "        t1.after_days,\n" +
            "        t1.device_channel,\n" +
            "        t1.device_type,\n" +
            "        t1.total_users,\n" +
            "        coalesce(t2.app_start_channel_users,0) app_start_date_users\n" +
            "    from\n" +
            "        B4 t1\n" +
            "    left join\n" +
            "        C t2\n" +
            "    on\n" +
            "        t1.regist_date = t2.regist_date\n" +
            "    and\n" +
            "        t1.app_start_date = t2.app_start_date\n" +
            "    and\n" +
            "        t1.device_channel = t2.device_channel\n" +
            "    and\n" +
            "        t1.device_type = t2.device_type\n" +
            "),\n" +
            "E as ( -- 计算存留比例\n" +
            "    select\n" +
            "        t1.regist_date,\n" +
            "        t1.app_start_date,\n" +
            "        t1.after_days,\n" +
            "        t1.device_channel,\n" +
            "        t1.device_type,\n" +
            "        t1.total_users,\n" +
            "        t1.app_start_date_users,\n" +
            "        case \n" +
            "            when app_start_date_users != 0 then concat(cast(cast(app_start_date_users*100.0/total_users as decimal(5,2)) as string),'%')\n" +
            "            else case\n" +
            "                    when app_start_date <= date_add(current_date,-1) then '0%'\n" +
            "                    else '-' \n" +
            "                end\n" +
            "            end as retention_rate  \n" +
            "    from\n" +
            "        D t1\n" +
            ")\n" +
            "INSERT overwrite table bi_ucar.rpt_user_ctr_retention partition(dt)\n" +
            "select \n" +
            "    t1.regist_date,\n" +
            "    t1.app_start_date,\n" +
            "    t1.after_days,\n" +
            "    t1.device_channel,\n" +
            "    t1.total_users,\n" +
            "    t1.app_start_date_users,\n" +
            "    t1.retention_rate,\n" +
            "    null, -- id\n" +
            "    t1.device_type,\n" +
            "    regist_date as dt\n" +
            "from \n" +
            "    E t1";
    public static final String SQL_10 = "with A as ( -- t_vrms_repair_sheet\n" +
            "    select\n" +
            "        t1.repair_sheet_no `维修单号`,\n" +
            "        case t1.repair_mode \n" +
            "            when 1 then '进厂维修'\n" +
            "            when 2 then '上门维保'\n" +
            "            when 3 then '只定损'\n" +
            "            when 4 then '补单'\n" +
            "        end `维修方式`, \n" +
            "        t1.origin_km `进厂里程`,\n" +
            "        t1.leave_repair_km `离厂里程`,\n" +
            "        t1.id,\n" +
            "        t1.update_date `工单完成时间`,\n" +
            "        t1.repair_vehicle_id,\n" +
            "        t1.vehicle_id,\n" +
            "        t1.repair_factory_id,\n" +
            "        t1.dept_id,\n" +
            "        t1.dept_name `维修单所属门店`,\n" +
            "        t3.name `维修单所属城市`\n" +
            "    from\n" +
            "        uc.t_vrms_repair_sheet t1\n" +
            "    left join\n" +
            "        uc.t_department t2\n" +
            "    on\n" +
            "        t1.dept_id = t2.id\n" +
            "    left join\n" +
            "        uc.t_b_city t3\n" +
            "    on\n" +
            "        t2.local_city = t3.id\n" +
            "    where\n" +
            "        t1.status = 7 -- 已结束\n" +
            "    and\n" +
            "        t1.is_deleted = 0 -- 未删除\n" +
            "    and\n" +
            "    t1.update_date >= '$sDate'\n" +
            "    and\n" +
            "    t1.update_date < '$eDate'\n" +
            "),\n" +
            "B as (SELECT \n" +
            "      repair_sheet_id,\n" +
            "      SUM(payment1) AS payment1,\n" +
            "      SUM(payment2) AS payment2,\n" +
            "      SUM(payment3) AS payment3 \n" +
            "    FROM(SELECT \n" +
            "      repair_sheet_id,\n" +
            "      SUM(payment1) + SUM(payment2)  AS payment1,\n" +
            "      0 AS payment2,\n" +
            "      0 AS payment3 \n" +
            "    FROM\n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        IF(\n" +
            "          r.care_outside = 1,\n" +
            "          SUM(\n" +
            "            IF(r.out_money IS NULL, 0, r.out_money)\n" +
            "          ),\n" +
            "          SUM(\n" +
            "            IF(\n" +
            "              r.work_time_fee IS NULL,\n" +
            "              0,\n" +
            "              r.work_time_fee\n" +
            "            ) + IF(trc.fee IS NULL, 0, trc.fee)\n" +
            "          )\n" +
            "        ) AS payment1,\n" +
            "        0 AS payment2\n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_pro_re r \n" +
            "        LEFT JOIN \n" +
            "          (SELECT \n" +
            "            repair_project_id,\n" +
            "            SUM(rc.component_fee) AS fee \n" +
            "          FROM\n" +
            "            uc.t_vrms_repair_sheet_component_re rc \n" +
            "          WHERE rc.is_deleted = 0 \n" +
            "          GROUP BY repair_project_id) trc \n" +
            "          ON r.id = trc.repair_project_id \n" +
            "      WHERE payment_type = 2 \n" +
            "        AND r.is_deleted = 0 \n" +
            "        AND r.is_reduce = 0 \n" +
            "        AND (\n" +
            "          r.repair_conclusion IS NULL \n" +
            "          OR r.repair_conclusion = 1\n" +
            "        ) \n" +
            "      GROUP BY r.repair_sheet_id,\n" +
            "        r.care_outside \n" +
            "      UNION\n" +
            "      SELECT \n" +
            "        repair_sheet_id,\n" +
            "        0 AS payment1,\n" +
            "        IF(\n" +
            "          r.care_outside = 1,\n" +
            "          SUM(\n" +
            "            IF(r.out_money IS NULL, 0, r.out_money)\n" +
            "          ),\n" +
            "          SUM(\n" +
            "            IF(\n" +
            "              r.additional_project_fee IS NULL,\n" +
            "              0,\n" +
            "              r.additional_project_fee\n" +
            "            )\n" +
            "          )\n" +
            "        ) AS payment2 \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_additional_project_re r \n" +
            "      WHERE payment_type = 2 \n" +
            "        AND r.is_deleted = 0 \n" +
            "        AND r.is_reduce = 0 \n" +
            "        AND (\n" +
            "          r.repair_conclusion IS NULL \n" +
            "          OR r.repair_conclusion = 1\n" +
            "        ) \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        care_outside) AS repair_sheet_payment1 \n" +
            "    GROUP BY repair_sheet_id \n" +
            "    UNION\n" +
            "    SELECT \n" +
            "      repair_sheet_id,\n" +
            "      0 AS payment1,\n" +
            "      SUM(payment1) + SUM(payment2)  AS payment2,\n" +
            "      0 AS payment3 \n" +
            "    FROM\n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        IF(\n" +
            "          r.care_outside = 1,\n" +
            "          SUM(\n" +
            "            IF(r.out_money IS NULL, 0, r.out_money)\n" +
            "          ),\n" +
            "          SUM(\n" +
            "            IF(\n" +
            "              r.work_time_fee IS NULL,\n" +
            "              0,\n" +
            "              r.work_time_fee\n" +
            "            ) + IF(trc.fee IS NULL, 0, trc.fee)\n" +
            "          )\n" +
            "        ) AS payment1,\n" +
            "        0 AS payment2\n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_pro_re r \n" +
            "        LEFT JOIN \n" +
            "          (SELECT \n" +
            "            repair_project_id,\n" +
            "            SUM(rc.component_fee) AS fee \n" +
            "          FROM\n" +
            "            uc.t_vrms_repair_sheet_component_re rc \n" +
            "          WHERE rc.is_deleted = 0 \n" +
            "          GROUP BY repair_project_id) trc \n" +
            "          ON r.id = trc.repair_project_id \n" +
            "      WHERE payment_type = 1 \n" +
            "        AND r.is_deleted = 0 \n" +
            "        AND r.is_reduce = 0 \n" +
            "        AND (\n" +
            "          r.repair_conclusion IS NULL \n" +
            "          OR r.repair_conclusion = 1\n" +
            "        ) \n" +
            "      GROUP BY r.repair_sheet_id,\n" +
            "        r.care_outside \n" +
            "      UNION\n" +
            "      SELECT \n" +
            "        repair_sheet_id,\n" +
            "        0 AS payment1,\n" +
            "        IF(\n" +
            "          r.care_outside = 1,\n" +
            "          SUM(\n" +
            "            IF(r.out_money IS NULL, 0, r.out_money)\n" +
            "          ),\n" +
            "          SUM(\n" +
            "            IF(\n" +
            "              r.additional_project_fee IS NULL,\n" +
            "              0,\n" +
            "              r.additional_project_fee\n" +
            "            )\n" +
            "          )\n" +
            "        ) AS payment2 \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_additional_project_re r \n" +
            "      WHERE payment_type = 1 \n" +
            "        AND r.is_deleted = 0 \n" +
            "        AND r.is_reduce = 0 \n" +
            "        AND (\n" +
            "          r.repair_conclusion IS NULL \n" +
            "          OR r.repair_conclusion = 1\n" +
            "        ) \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        r.care_outside) AS repair_sheet_payment2 \n" +
            "    GROUP BY repair_sheet_id \n" +
            "    UNION\n" +
            "    SELECT \n" +
            "      repair_sheet_id,\n" +
            "      0 AS payment1,\n" +
            "      0 AS payment2,\n" +
            "      SUM(payment1) + SUM(payment2)  AS payment3 \n" +
            "    FROM\n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        IF(\n" +
            "          r.care_outside = 1,\n" +
            "          SUM(\n" +
            "            IF(r.out_money IS NULL, 0, r.out_money)\n" +
            "          ),\n" +
            "          SUM(\n" +
            "            IF(\n" +
            "              r.work_time_fee IS NULL,\n" +
            "              0,\n" +
            "              r.work_time_fee\n" +
            "            ) + IF(trc.fee IS NULL, 0, trc.fee)\n" +
            "          )\n" +
            "        ) AS payment1,\n" +
            "        0 AS payment2\n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_pro_re r \n" +
            "        LEFT JOIN \n" +
            "          (SELECT \n" +
            "            repair_project_id,\n" +
            "            SUM(rc.component_fee) AS fee \n" +
            "          FROM\n" +
            "            uc.t_vrms_repair_sheet_component_re rc \n" +
            "          WHERE rc.is_deleted = 0 \n" +
            "          GROUP BY repair_project_id) trc \n" +
            "          ON r.id = trc.repair_project_id \n" +
            "      WHERE payment_type = 3 \n" +
            "        AND r.is_deleted = 0 \n" +
            "        AND r.is_reduce = 0 \n" +
            "        AND (\n" +
            "          r.repair_conclusion IS NULL \n" +
            "          OR r.repair_conclusion = 1\n" +
            "        ) \n" +
            "      GROUP BY r.repair_sheet_id,\n" +
            "        r.care_outside \n" +
            "      UNION\n" +
            "      SELECT \n" +
            "        repair_sheet_id,\n" +
            "        0 AS payment1,\n" +
            "        IF(\n" +
            "          r.care_outside = 1,\n" +
            "          SUM(\n" +
            "            IF(r.out_money IS NULL, 0, r.out_money)\n" +
            "          ),\n" +
            "          SUM(\n" +
            "            IF(\n" +
            "              r.additional_project_fee IS NULL,\n" +
            "              0,\n" +
            "              r.additional_project_fee\n" +
            "            )\n" +
            "          )\n" +
            "        ) AS payment2 \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_additional_project_re r \n" +
            "      WHERE payment_type = 3 \n" +
            "        AND r.is_deleted = 0 \n" +
            "        AND r.is_reduce = 0 \n" +
            "        AND (\n" +
            "          r.repair_conclusion IS NULL \n" +
            "          OR r.repair_conclusion = 1\n" +
            "        ) \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        r.care_outside) AS repair_sheet_payment3 \n" +
            "    GROUP BY repair_sheet_id) AS repair_sheet_payment \n" +
            "    GROUP BY repair_sheet_id),\n" +
            "C as ( -- t_vrms_repair_sheet_zuche_info \n" +
            "    select\n" +
            "        t1.zuche_repair_no `租车维保单号`,\n" +
            "        t1.return_date `还车时间`,\n" +
            "        t1.apply_city_name `工单申请城市`,\n" +
            "        t1.apply_dept_name `工单申请门店`,\n" +
            "        t1.park_dept_name `停放租车门店`,\n" +
            "        t1.park_city_name `停放城市`,\n" +
            "        t1.costs_attach_city_name `成本归属城市`,\n" +
            "        t1.costs_attach_dept_name `成本归属门店`,\n" +
            "        case t1.send_repair_type\n" +
            "            when 1 then '自主送取'\n" +
            "            when 2 then '上门取送'\n" +
            "        end `送修方式`,\n" +
            "        t1.claim_nos `理赔单号`,\n" +
            "        t1.repair_sheet_id,\n" +
            "        t1.repair_project_type_id\n" +
            "    from\n" +
            "        uc.t_vrms_repair_sheet_zuche_info t1\n" +
            "\n" +
            "),\n" +
            "D as ( -- t_rm_bill\n" +
            "    select \n" +
            "        t1.id,\n" +
            "        t1.bill_no,\n" +
            "        case t1.ordercar \n" +
            "            when 0 then '短租' \n" +
            "            when 1 then '长租' \n" +
            "            when 3 then '融资租赁' \n" +
            "            when 4 then '试驾' \n" +
            "            when 5 then '公务车' \n" +
            "            when 7 then '专车' \n" +
            "            when 8 then '优驾' \n" +
            "        end `车辆用途`,\n" +
            "        t3.model_name `车型`\n" +
            "    from \n" +
            "        zuche.t_rm_bill t1\n" +
            "    left join\n" +
            "        zuche.vehicle t2\n" +
            "    on\n" +
            "        t1.vehicle_id = t2.xid\n" +
            "    left join\n" +
            "        zuche.t_v_model t3\n" +
            "    on\n" +
            "        t2.model_id = t3.id\n" +
            "),\n" +
            "E as ( -- t_vrms_repair_vehicle\n" +
            "    select\n" +
            "        t1.id,\n" +
            "        t1.vehicle_no,\n" +
            "        t1.model_id,\n" +
            "        case t1.vehicle_nature\n" +
            "            when 1 then '神州租车'\n" +
            "            when 2 then '神州专车'\n" +
            "            when 3 then '神州闪贷'\n" +
            "            when 4 then '神州畅行'\n" +
            "            when 5 then '买买车二手车'\n" +
            "            when 6 then '外部车辆'\n" +
            "        end `车辆性质` -- 2018年2月2日新增字段 车辆性质\n" +
            "    from\n" +
            "        uc.t_vrms_repair_vehicle t1\n" +
            "),\n" +
            "F as ( -- t_vrms_vehicle\n" +
            "    select\n" +
            "        t1.id,\n" +
            "        t1.vehicle_no,\n" +
            "        t1.model_id,\n" +
            "        case t1.vehicle_nature\n" +
            "            when 1 then '神州租车'\n" +
            "            when 2 then '神州专车'\n" +
            "            when 3 then '神州闪贷'\n" +
            "            when 4 then '神州畅行'\n" +
            "            when 5 then '买买车二手车'\n" +
            "            when 6 then '外部车辆'\n" +
            "        end `车辆性质` -- 2018年2月2日新增字段 车辆性质\n" +
            "    from\n" +
            "        uc.t_vrms_vehicle t1\n" +
            "),\n" +
            "-- G as ( -- t_v_model\n" +
            "--     select\n" +
            "--         t1.id,\n" +
            "--         t1.model_name `车型`\n" +
            "--     from\n" +
            "--         uc.t_v_model t1\n" +
            "-- ),\n" +
            "H as ( -- t_vrms_repair_factory\n" +
            "    select\n" +
            "        t1.id,\n" +
            "        t1.factory_name `维修厂`,\n" +
            "        t2.aptitude_name `维修厂资质`\n" +
            "    from\n" +
            "        uc.t_vrms_repair_factory t1\n" +
            "    left join\n" +
            "        uc.t_vrms_repair_factory_aptitude t2\n" +
            "    on\n" +
            "        t1.aptitude_id = t2.id\n" +
            "),\n" +
            "I as ( -- t_vrms_repair_his\n" +
            "    select distinct\n" +
            "        t1.id,\n" +
            "        t1.create_date,\n" +
            "        t1.creator_name,\n" +
            "        t1.dept_name,\n" +
            "        t1.type,\n" +
            "        t1.status,\n" +
            "        t1.repair_sheet_id\n" +
            "    from \n" +
            "        uc.t_vrms_repair_his t1\n" +
            "),\n" +
            "J as ( -- t_vrms_repair_project_type\n" +
            "    select\n" +
            "        t1.id,\n" +
            "        t1.type_name `租车工单类型`\n" +
            "    from\n" +
            "        uc.t_vrms_repair_project_type t1\n" +
            "),\n" +
            "K as ( -- t_vrms_repair_project && t_vrms_repair_sheet_pro_re\n" +
            "    select\n" +
            "        t1.repair_sheet_id,\n" +
            "        case when count(case when t2.update_mileage = 1 then 1 else null end) > 0 then '是' else '否' end `是否更新待保养里程`\n" +
            "    from\n" +
            "        uc.t_vrms_repair_sheet_pro_re t1\n" +
            "    left join\n" +
            "        uc.t_vrms_repair_project t2\n" +
            "    on\n" +
            "        t1.repair_project_id = t2.id\n" +
            "    group by \n" +
            "        t1.repair_sheet_id\n" +
            "),\n" +
            "L as ( -- 2018年2月2日新增字段 工单新建人\n" +
            "    select\n" +
            "        t1.repair_sheet_id,\n" +
            "        t1.creator_name `工单新建人`\n" +
            "    from\n" +
            "        I t1\n" +
            "    where\n" +
            "        t1.type=1\n" +
            "),\n" +
            "TIME_01_01 as ( -- 方案通过操作人-前置\n" +
            "    select\n" +
            "        t1.repair_sheet_id,\n" +
            "        max(t1.id) as id\n" +
            "    from\n" +
            "        I t1\n" +
            "    where\n" +
            "        t1.type = 4\n" +
            "    group by \n" +
            "        t1.repair_sheet_id\n" +
            "),\n" +
            "TIME_01 as ( -- 方案通过操作人\n" +
            "    select \n" +
            "        t1.repair_sheet_id,\n" +
            "        t2.creator_name `方案通过操作人`,\n" +
            "        t2.dept_name `方案通过操作人部门`\n" +
            "    from\n" +
            "        TIME_01_01 t1\n" +
            "    inner join\n" +
            "        I t2\n" +
            "    on\n" +
            "        t1.id = t2.id\n" +
            "),\n" +
            "TIME_02_01 as ( -- 首次提报方案时间,获取首次提报方案的时间和历史维修单记录的id,为下一步获取 首次提报方案操作人 和 首次提报方案操作部门 做准备\n" +
            "    select\n" +
            "        t1.repair_sheet_id,\n" +
            "        min(t1.id) as id\n" +
            "    from\n" +
            "        I t1\n" +
            "    where\n" +
            "        t1.type = 14\n" +
            "    group by \n" +
            "        t1.repair_sheet_id\n" +
            "),\n" +
            "TIME_02 as ( -- 首次提报方案时间\n" +
            "    select \n" +
            "        t1.repair_sheet_id,\n" +
            "        t2.creator_name `首次提报方案操作人`,\n" +
            "        t2.dept_name `首次提报方案操作部门`\n" +
            "    from\n" +
            "        TIME_02_01 t1\n" +
            "    inner join\n" +
            "        I t2\n" +
            "    on\n" +
            "        t1.id = t2.id\n" +
            "),\n" +
            "-- TIME_04 as ( -- 维修完毕操作时间\n" +
            "--     select\n" +
            "--         t1.repair_sheet_id,\n" +
            "--         min(t1.create_date) `维修完毕操作时间`\n" +
            "--     from\n" +
            "--         I t1\n" +
            "--     where\n" +
            "--         t1.type = 5\n" +
            "--     group by \n" +
            "--         t1.repair_sheet_id\n" +
            "-- ),\n" +
            "-- TIME_05 as ( -- 离场操作时间\n" +
            "--     select\n" +
            "--         t1.repair_sheet_id,\n" +
            "--         min(t1.create_date) `离场操作时间`\n" +
            "--     from\n" +
            "--         I t1\n" +
            "--     where\n" +
            "--         t1.type = 15\n" +
            "--     group by \n" +
            "--         t1.repair_sheet_id\n" +
            "-- ),\n" +
            "-- TIME_08 as ( -- 确认定损操作时间\n" +
            "--     select\n" +
            "--         t1.repair_sheet_id,\n" +
            "--         min(t1.create_date) `确认定损操作时间`\n" +
            "--     from\n" +
            "--         I t1\n" +
            "--     where\n" +
            "--         t1.type = 19\n" +
            "--     group by \n" +
            "--         t1.repair_sheet_id\n" +
            "-- ),\n" +
            "-- TIME_09 as ( -- 变更成维修单结束的时间\n" +
            "--     select\n" +
            "--         t1.repair_sheet_id,\n" +
            "--         min(t1.create_date) `变更成维修单结束的时间`\n" +
            "--     from\n" +
            "--         I t1\n" +
            "--     where\n" +
            "--         t1.status = 7\n" +
            "--     group by \n" +
            "--         t1.repair_sheet_id\n" +
            "-- ),\n" +
            "FEE_01 as ( -- 保养项目金额\n" +
            "SELECT \n" +
            "  SUM(fee) AS fee,\n" +
            "  repair_sheet_id \n" +
            "FROM\n" +
            "  (SELECT \n" +
            "    r.repair_sheet_id,\n" +
            "    CASE\n" +
            "      WHEN r.care_outside = 1 \n" +
            "      THEN SUM(r.out_money + coalesce(fee, 0)) \n" +
            "      ELSE SUM(work_time_fee + coalesce(fee, 0)) \n" +
            "    END AS fee \n" +
            "  FROM\n" +
            "    uc.t_vrms_repair_sheet_pro_re r \n" +
            "    LEFT JOIN uc.t_vrms_repair_project p \n" +
            "      ON p.id = r.repair_project_id \n" +
            "    LEFT JOIN uc.t_vrms_repair_project_type tp \n" +
            "      ON tp.id = p.project_type_id \n" +
            "    LEFT JOIN \n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        repair_project_id,\n" +
            "        SUM(coalesce(rc.component_fee, 0)) AS fee \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_component_re rc \n" +
            "      WHERE rc.is_deleted = 0 \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        repair_project_id) trc \n" +
            "      ON r.id = trc.repair_project_id \n" +
            "  WHERE r.is_deleted = 0 \n" +
            "    AND r.is_reduce = 0 \n" +
            "    AND payment_type != 2\n" +
            "    AND tp.id = 4\n" +
            "  GROUP BY r.repair_sheet_id,\n" +
            "    r.care_outside) ttt\n" +
            "GROUP BY repair_sheet_id \n" +
            "),\n" +
            "FEE_02 as ( -- 故障维修项目金额\n" +
            "SELECT \n" +
            "  SUM(fee) AS fee,\n" +
            "  repair_sheet_id \n" +
            "FROM\n" +
            "  (SELECT \n" +
            "    r.repair_sheet_id,\n" +
            "    CASE\n" +
            "      WHEN r.care_outside = 1 \n" +
            "      THEN SUM(r.out_money + coalesce(fee, 0)) \n" +
            "      ELSE SUM(work_time_fee + coalesce(fee, 0)) \n" +
            "    END AS fee \n" +
            "  FROM\n" +
            "    uc.t_vrms_repair_sheet_pro_re r \n" +
            "    LEFT JOIN uc.t_vrms_repair_project p \n" +
            "      ON p.id = r.repair_project_id \n" +
            "    LEFT JOIN uc.t_vrms_repair_project_type tp \n" +
            "      ON tp.id = p.project_type_id \n" +
            "    LEFT JOIN \n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        repair_project_id,\n" +
            "        SUM(coalesce(rc.component_fee, 0)) AS fee \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_component_re rc \n" +
            "      WHERE rc.is_deleted = 0 \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        repair_project_id) trc \n" +
            "      ON r.id = trc.repair_project_id \n" +
            "  WHERE r.is_deleted = 0 \n" +
            "    AND r.is_reduce = 0 \n" +
            "    AND payment_type != 2\n" +
            "    AND tp.id = 5 \n" +
            "  GROUP BY r.repair_sheet_id,\n" +
            "    r.care_outside) ttt\n" +
            "GROUP BY repair_sheet_id \n" +
            "),\n" +
            "FEE_03 as ( -- 事故项目金额\n" +
            "SELECT \n" +
            "  SUM(fee) AS fee,\n" +
            "  repair_sheet_id \n" +
            "FROM\n" +
            "  (SELECT \n" +
            "    r.repair_sheet_id,\n" +
            "    CASE\n" +
            "      WHEN r.care_outside = 1 \n" +
            "      THEN SUM(r.out_money + coalesce(fee, 0)) \n" +
            "      ELSE SUM(work_time_fee + coalesce(fee, 0)) \n" +
            "    END AS fee \n" +
            "  FROM\n" +
            "    uc.t_vrms_repair_sheet_pro_re r \n" +
            "    LEFT JOIN uc.t_vrms_repair_project p \n" +
            "      ON p.id = r.repair_project_id \n" +
            "    LEFT JOIN uc.t_vrms_repair_project_type tp \n" +
            "      ON tp.id = p.project_type_id \n" +
            "    LEFT JOIN \n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        repair_project_id,\n" +
            "        SUM(coalesce(rc.component_fee, 0)) AS fee \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_component_re rc \n" +
            "      WHERE rc.is_deleted = 0 \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        repair_project_id) trc \n" +
            "      ON r.id = trc.repair_project_id \n" +
            "  WHERE r.is_deleted = 0 \n" +
            "    AND r.is_reduce = 0 \n" +
            "    AND \n" +
            "    CASE\n" +
            "      WHEN payment_type != 2 \n" +
            "      THEN tp.id = 7 \n" +
            "      ELSE 1 = 1 \n" +
            "    END \n" +
            "  GROUP BY r.repair_sheet_id,\n" +
            "    r.care_outside) ttt\n" +
            "GROUP BY repair_sheet_id \n" +
            "),\n" +
            "FEE_04 as ( -- 美容项目金额\n" +
            "SELECT \n" +
            "  SUM(fee) AS fee,\n" +
            "  repair_sheet_id \n" +
            "FROM\n" +
            "  (SELECT \n" +
            "    r.repair_sheet_id,\n" +
            "    CASE\n" +
            "      WHEN r.care_outside = 1 \n" +
            "      THEN SUM(r.out_money + coalesce(fee, 0)) \n" +
            "      ELSE SUM(work_time_fee + coalesce(fee, 0)) \n" +
            "    END AS fee \n" +
            "  FROM\n" +
            "    uc.t_vrms_repair_sheet_pro_re r \n" +
            "    LEFT JOIN uc.t_vrms_repair_project p \n" +
            "      ON p.id = r.repair_project_id \n" +
            "    LEFT JOIN uc.t_vrms_repair_project_type tp \n" +
            "      ON tp.id = p.project_type_id \n" +
            "    LEFT JOIN \n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        repair_project_id,\n" +
            "        SUM(coalesce(rc.component_fee, 0)) AS fee \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_component_re rc \n" +
            "      WHERE rc.is_deleted = 0 \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        repair_project_id) trc \n" +
            "      ON r.id = trc.repair_project_id \n" +
            "  WHERE r.is_deleted = 0 \n" +
            "    AND r.is_reduce = 0 \n" +
            "    AND payment_type != 2\n" +
            "    AND tp.id = 1 \n" +
            "  GROUP BY r.repair_sheet_id,\n" +
            "    r.care_outside) ttt\n" +
            "GROUP BY repair_sheet_id \n" +
            "),\n" +
            "FEE_05 as ( -- 外观维修项目金额\n" +
            "SELECT \n" +
            "  SUM(fee) AS fee,\n" +
            "  repair_sheet_id \n" +
            "FROM\n" +
            "  (SELECT \n" +
            "    r.repair_sheet_id,\n" +
            "    CASE\n" +
            "      WHEN r.care_outside = 1 \n" +
            "      THEN SUM(r.out_money + coalesce(fee, 0)) \n" +
            "      ELSE SUM(work_time_fee + coalesce(fee, 0)) \n" +
            "    END AS fee \n" +
            "  FROM\n" +
            "    uc.t_vrms_repair_sheet_pro_re r \n" +
            "    LEFT JOIN uc.t_vrms_repair_project p \n" +
            "      ON p.id = r.repair_project_id \n" +
            "    LEFT JOIN uc.t_vrms_repair_project_type tp \n" +
            "      ON tp.id = p.project_type_id \n" +
            "    LEFT JOIN \n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        repair_project_id,\n" +
            "        SUM(coalesce(rc.component_fee, 0)) AS fee \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_component_re rc \n" +
            "      WHERE rc.is_deleted = 0 \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        repair_project_id) trc \n" +
            "      ON r.id = trc.repair_project_id \n" +
            "  WHERE r.is_deleted = 0 \n" +
            "    AND r.is_reduce = 0 \n" +
            "    AND payment_type != 2\n" +
            "    AND tp.id = 3 \n" +
            "  GROUP BY r.repair_sheet_id,\n" +
            "    r.care_outside) ttt\n" +
            "GROUP BY repair_sheet_id \n" +
            "),\n" +
            "FEE_06 as ( -- 易损件维修项目金额\n" +
            "SELECT \n" +
            "  SUM(fee) AS fee,\n" +
            "  repair_sheet_id \n" +
            "FROM\n" +
            "  (SELECT \n" +
            "    r.repair_sheet_id,\n" +
            "    CASE\n" +
            "      WHEN r.care_outside = 1 \n" +
            "      THEN SUM(r.out_money + coalesce(fee, 0)) \n" +
            "      ELSE SUM(work_time_fee + coalesce(fee, 0)) \n" +
            "    END AS fee \n" +
            "  FROM\n" +
            "    uc.t_vrms_repair_sheet_pro_re r \n" +
            "    LEFT JOIN uc.t_vrms_repair_project p \n" +
            "      ON p.id = r.repair_project_id \n" +
            "    LEFT JOIN uc.t_vrms_repair_project_type tp \n" +
            "      ON tp.id = p.project_type_id \n" +
            "    LEFT JOIN \n" +
            "      (SELECT \n" +
            "        repair_sheet_id,\n" +
            "        repair_project_id,\n" +
            "        SUM(coalesce(rc.component_fee, 0)) AS fee \n" +
            "      FROM\n" +
            "        uc.t_vrms_repair_sheet_component_re rc \n" +
            "      WHERE rc.is_deleted = 0 \n" +
            "      GROUP BY repair_sheet_id,\n" +
            "        repair_project_id) trc \n" +
            "      ON r.id = trc.repair_project_id \n" +
            "  WHERE r.is_deleted = 0 \n" +
            "    AND r.is_reduce = 0 \n" +
            "    AND payment_type != 2\n" +
            "    AND tp.id = 2 \n" +
            "  GROUP BY r.repair_sheet_id,\n" +
            "    r.care_outside) ttt\n" +
            "GROUP BY repair_sheet_id \n" +
            "),\n" +
            "FEE_07 as ( -- 附加项目金额-保险 附加项目金额-客户 附加项目金额-公司\n" +
            "    SELECT\n" +
            "        t1.repair_sheet_id,\n" +
            "        sum(case when t1.payment_type = 1 then (t1.out_money+t1.additional_project_fee) else 0 end) `附加项目金额-客户`,\n" +
            "        sum(case when t1.payment_type = 2 then (t1.out_money+t1.additional_project_fee) else 0 end) `附加项目金额-保险`,\n" +
            "        sum(case when t1.payment_type = 3 then (t1.out_money+t1.additional_project_fee) else 0 end) `附加项目金额-公司`\n" +
            "    FROM\n" +
            "        uc.t_vrms_repair_sheet_additional_project_re t1\n" +
            "    where\n" +
            "        t1.is_deleted = 0\n" +
            "    and\n" +
            "        t1.is_reduce = 0\n" +
            "    group by\n" +
            "        t1.repair_sheet_id\n" +
            "),\n" +
            "Result as (\n" +
            "    select\n" +
            "        A.`维修单号`,\n" +
            "        A.`维修方式`, \n" +
            "        A.`进厂里程`,\n" +
            "        A.`离厂里程`,\n" +
            "        B.payment1 `理赔款付费金额` ,\n" +
            "        B.payment2 `合同付费金额` ,\n" +
            "        B.payment3 `公司付费金额` ,\n" +
            "        B.payment1 + B.payment2 + B.payment3 `实际费用` ,\n" +
            "        FEE_01.fee `保养项目金额` ,\n" +
            "        FEE_02.fee `故障维修项目金额` ,\n" +
            "        FEE_03.fee `事故项目金额` ,\n" +
            "        FEE_04.fee `美容项目金额` ,\n" +
            "        FEE_05.fee `外观维修项目金额` ,\n" +
            "        FEE_06.fee `易损件维修项目金额` ,\n" +
            "        C.`租车维保单号`,\n" +
            "        C.`还车时间`,\n" +
            "        C.`工单申请城市`,\n" +
            "        C.`工单申请门店`,\n" +
            "        C.`停放租车门店`,\n" +
            "        C.`停放城市`,\n" +
            "        C.`成本归属城市`,\n" +
            "        C.`成本归属门店`,\n" +
            "        D.`车辆用途`,\n" +
            "        coalesce(E.vehicle_no,F.vehicle_no) `车牌号`,\n" +
            "        D.`车型`,\n" +
            "        H.`维修厂`,\n" +
            "        A.`工单完成时间`,\n" +
            "        J.`租车工单类型`,\n" +
            "        TIME_02.`首次提报方案操作人`,\n" +
            "        TIME_02.`首次提报方案操作部门`,\n" +
            "        TIME_01.`方案通过操作人`,\n" +
            "        TIME_01.`方案通过操作人部门`,\n" +
            "        C.`理赔单号`,\n" +
            "        K.`是否更新待保养里程`,\n" +
            "        A.`维修单所属门店`,\n" +
            "        A.`维修单所属城市`,\n" +
            "        FEE_07.`附加项目金额-客户`,\n" +
            "        FEE_07.`附加项目金额-保险`,\n" +
            "        FEE_07.`附加项目金额-公司`,\n" +
            "        H.`维修厂资质`,\n" +
            "        case when A.repair_vehicle_id is not null then E.`车辆性质` else F.`车辆性质` end `车辆性质`, -- 优先使用t_vrms_repair_vehicle表\n" +
            "        L.`工单新建人`\n" +
            "    from\n" +
            "        A \n" +
            "    left join\n" +
            "        B\n" +
            "    on\n" +
            "        A.id = B.repair_sheet_id\n" +
            "    left join\n" +
            "        C\n" +
            "    on\n" +
            "        A.id = C.repair_sheet_id\n" +
            "    left join\n" +
            "        D\n" +
            "    on\n" +
            "        C.`租车维保单号` = D.bill_no\n" +
            "    left join\n" +
            "        E\n" +
            "    on\n" +
            "        A.repair_vehicle_id = E.id\n" +
            "    left join\n" +
            "        F\n" +
            "    on\n" +
            "        A.vehicle_id = F.id\n" +
            "    -- left join\n" +
            "    --     G\n" +
            "    -- on\n" +
            "    --     G.id = case when A.repair_vehicle_id is not null then E.model_id else F.model_id end -- 优先使用t_vrms_repair_vehicle表的model_id\n" +
            "    left join\n" +
            "        H\n" +
            "    on\n" +
            "        A.repair_factory_id = H.id\n" +
            "    left join\n" +
            "        TIME_01\n" +
            "    on\n" +
            "        TIME_01.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        TIME_02\n" +
            "    on\n" +
            "        TIME_02.repair_sheet_id = A.id\n" +
            "    -- left join\n" +
            "    --  TIME_04\n" +
            "    -- on\n" +
            "    --  TIME_04.repair_sheet_id = A.id\n" +
            "    -- left join\n" +
            "    --  TIME_05\n" +
            "    -- on\n" +
            "    --  TIME_05.repair_sheet_id = A.id\n" +
            "    -- left join\n" +
            "    --  TIME_08\n" +
            "    -- on\n" +
            "    --  TIME_08.repair_sheet_id = A.id\n" +
            "    -- left join\n" +
            "    --  TIME_09\n" +
            "    -- on\n" +
            "    --  TIME_09.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        J\n" +
            "    on\n" +
            "        J.id = C.repair_project_type_id\n" +
            "    left join\n" +
            "        K\n" +
            "    on\n" +
            "        K.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_01\n" +
            "    on\n" +
            "        Fee_01.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_02\n" +
            "    on\n" +
            "        Fee_02.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_03\n" +
            "    on\n" +
            "        Fee_03.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_04\n" +
            "    on\n" +
            "        Fee_04.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_05\n" +
            "    on\n" +
            "        Fee_05.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_06\n" +
            "    on\n" +
            "        Fee_06.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        FEE_07\n" +
            "    on\n" +
            "        Fee_07.repair_sheet_id = A.id\n" +
            "    left join\n" +
            "        L\n" +
            "    on\n" +
            "        L.repair_sheet_id = A.id\n" +
            "\n" +
            ")\n" +
            "insert into table bi_mmc.rpt_vrms_fee_statistics_all\n" +
            "select \n" +
            "    t1.`维修厂`,\n" +
            "    t1.`维修单号`,\n" +
            "    t1.`租车维保单号`,\n" +
            "    t1.`车牌号`,\n" +
            "    t1.`车型`,\n" +
            "    t1.`还车时间`,\n" +
            "    t1.`工单申请城市`,\n" +
            "    t1.`工单申请门店`,\n" +
            "    t1.`停放租车门店`,\n" +
            "    t1.`停放城市`,\n" +
            "    t1.`维修方式`,\n" +
            "    t1.`理赔单号`,\n" +
            "    t1.`车辆用途`,\n" +
            "    t1.`进厂里程`,\n" +
            "    t1.`离厂里程`,\n" +
            "    t1.`理赔款付费金额`,\n" +
            "    t1.`理赔款付费金额`, -- t1.`理赔款维保支出额度`, 相同口径\n" +
            "    t1.`合同付费金额`,\n" +
            "    t1.`公司付费金额`,\n" +
            "    t1.`实际费用`,\n" +
            "    t1.`保养项目金额`,\n" +
            "    t1.`故障维修项目金额`,\n" +
            "    t1.`事故项目金额`,\n" +
            "    t1.`美容项目金额`,\n" +
            "    t1.`外观维修项目金额`,\n" +
            "    t1.`易损件维修项目金额`,\n" +
            "    t1.`是否更新待保养里程`,\n" +
            "    t1.`首次提报方案操作人`,\n" +
            "    t1.`首次提报方案操作部门`,\n" +
            "    t1.`方案通过操作人`,\n" +
            "    t1.`方案通过操作人部门`,\n" +
            "    t1.`工单完成时间`,\n" +
            "    t1.`成本归属城市`,\n" +
            "    t1.`成本归属门店`,\n" +
            "    t1.`租车工单类型`,\n" +
            "    null, -- id\n" +
            "    t1.`维修单所属门店`,\n" +
            "    t1.`维修单所属城市`,\n" +
            "    t1.`附加项目金额-保险`,\n" +
            "    t1.`附加项目金额-客户`,\n" +
            "    t1.`附加项目金额-公司`,\n" +
            "    t1.`维修厂资质`, -- 2018年2月2日新增字段\n" +
            "    t1.`车辆性质`, -- 2018年2月2日新增字段\n" +
            "    t1.`工单新建人` -- 2018年2月2日新增字段\n" +
            "from \n" +
            "    Result t1\n";
}
