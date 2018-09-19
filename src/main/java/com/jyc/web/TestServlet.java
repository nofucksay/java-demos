package com.jyc.web;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerEX;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Created by jyc on 2018/7/23.
 */
public class TestServlet extends HttpServlet {
    /**
     * Called by the server (via the <code>service</code> method)
     * to allow a servlet to handle a POST request.
     * <p>
     * The HTTP POST method allows the client to send
     * data of unlimited length to the Web server a single time
     * and is useful when posting information such as
     * credit card numbers.
     * <p>
     * <p>When overriding this method, read the request data,
     * write the response headers, get the response's writer or output
     * stream object, and finally, write the response data. It's best
     * to include content type and encoding. When using a
     * <code>PrintWriter</code> object to return the response, set the
     * content type before accessing the <code>PrintWriter</code> object.
     * <p>
     * <p>The servlet container must write the headers before committing the
     * response, because in HTTP the headers must be sent before the
     * response body.
     * <p>
     * <p>Where possible, set the Content-Length header (with the
     * {@link ServletResponse#setContentLength} method),
     * to allow the servlet container to use a persistent connection
     * to return its response to the client, improving performance.
     * The content length is automatically set if the entire response fits
     * inside the response buffer.
     * <p>
     * <p>When using HTTP 1.1 chunked encoding (which means that the response
     * has a Transfer-Encoding header), do not set the Content-Length header.
     * <p>
     * <p>This method does not need to be either safe or idempotent.
     * Operations requested through POST can have side effects for
     * which the user can be held accountable, for example,
     * updating stored data or buying items online.
     * <p>
     * <p>If the HTTP POST request is incorrectly formatted,
     * <code>doPost</code> returns an HTTP "Bad Request" message.
     *
     * @param req  an {@link HttpServletRequest} object that
     *             contains the request the client has made
     *             of the servlet
     * @param resp an {@link HttpServletResponse} object that
     *             contains the response the servlet sends
     *             to the client
     * @throws IOException      if an input or output error is
     *                          detected when the servlet handles
     *                          the request
     * @throws ServletException if the request for the POST
     *                          could not be handled
     * @see ServletOutputStream
     * @see ServletResponse#setContentType
     */
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String sql = req.getParameter("sql");
        sql = sql.replaceAll("@", "");

//        HiveConf conf = new HiveConf();
        try {
//            SemanticAnalyzerEX.getBloodRelationShipFromSQL(sql);
            String result = SemanticAnalyzerEX.printResultInfo(SemanticAnalyzerEX.getBloodRelationShipFromSQL(sql), true);
            ServletOutputStream op=resp.getOutputStream();
            op.write(result.getBytes());
            op.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static void main(String[] args) {
        try {

            String sql = "with vehicle_limited_detail as (    \n" +
                    "select \n" +
                    " dt\n" +
                    ",vehicle_id\n" +
                    ",vehicleno\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",city_id\n" +
                    ",limit_line_start_time\n" +
                    ",limit_line_end_time \n" +
                    ",time_split_tag\n" +
                    ",daynumofweek \n" +
                    ",tailNum\n" +
                    ",limit_line_weekday\n" +
                    ",limit_line_tailnum\n" +
                    ",cast(substring(limit_line_start_time ,0,2)as int) as  limitLineStartIntervalNum\n" +
                    ",cast(substring(limit_line_end_time ,0,2) as int) as   limitLineEndIntervalNum\n" +
                    ",substring(cast(limit_line_tailnum as string) , 0, 1) as                  limitLineCode1\n" +
                    ",substring(cast(limit_line_tailnum as string), 3 ,1) as                   limitLineCode2 \n" +
                    ",time_name_start\n" +
                    ",time_name_end\n" +
                    "from (\n" +
                    "select \n" +
                    " vh.dt\n" +
                    ",vh.vehicle_id\n" +
                    ",vh.vehicleno\n" +
                    ",vh.department_id \n" +
                    ",vh.nowdepartment_id\n" +
                    ",vh.park_city_id\n" +
                    ",vh.vehicleno_city_id\n" +
                    ",vh.tailNum\n" +
                    ",vh.daynumofweek\n" +
                    ",vl.limit_line_weekday \n" +
                    ",vl.limit_line_start_time\n" +
                    ",vl.limit_line_end_time\n" +
                    ",vl.city_id\n" +
                    ",vl.limit_line_tailnum\n" +
                    ",vh.time_split_tag\n" +
                    "from ( \n" +
                    "select \n" +
                    " dt\n" +
                    ",id vehicle_id\n" +
                    ",vehicleno\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",dd.daynumofweek   as daynumofweek\n" +
                    ",reverse(vehicleno)vehicleno_r \n" +
                    ",SUBSTRING(reverse(vehicleno), 0, 1)   as tailNum\n" +
                    ",time_split_tag\n" +
                    "from \n" +
                    "bi_zuche.bas_vehicle_sync_by_hour \n" +
                    "LEFT JOIN  bi_dm.dim_date dd   --查询日期对应的星期？  \n" +
                    "on dt =  dd.fulldate\n" +
                    "where dt >= '@sDate@' and dt <='@eDate@'    \n" +
                    "and  dd.fulldate >= '@sDate@' and dd.fulldate <='@eDate@'\n" +
                    "and self_status_1st = '200000' and is_shared = 1  and ordercar = 0) vh\n" +
                    "LEFT JOIN \n" +
                    "(\n" +
                    "select \n" +
                    "id , \n" +
                    "city_id ,\n" +
                    "limit_line_type ,\n" +
                    "limit_line_start_time ,\n" +
                    "limit_line_end_time ,\n" +
                    "limit_line_weekday -1 as limit_line_weekday ,  --存放数据需要-1对应dim_date表 \n" +
                    "limit_line_tailnum,\n" +
                    "dt\n" +
                    "from bi_zuche.bas_vehicle_limit_by_city_his  --限行表\n" +
                    ") vl\n" +
                    "on vh.vehicleno_city_id = vl.city_id and vh.daynumofweek = vl.limit_line_weekday\n" +
                    "and vh.dt = vl.dt ) veh_limit\n" +
                    "left join bi_zuche.bas_time_section_dict bd   -- 关联每个车辆时段的开始、结束时间\n" +
                    "on veh_limit.time_split_tag  =  bd.time_hour_id\n" +
                    "\n" +
                    ") , \n" +
                    "\n" +
                    "vehicle_limited_sum as (\n" +
                    "\n" +
                    "select \n" +
                    " dt\n" +
                    ",vehicle_type\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",sum(a) a \n" +
                    ",sum(b) b\n" +
                    ",sum(c) c\n" +
                    ",sum(d) d\n" +
                    ",sum(e) e\n" +
                    ",sum(f) f\n" +
                    ",sum(g) g\n" +
                    ",sum(h) h\n" +
                    ",sum(i) i\n" +
                    ",sum(j) j\n" +
                    ",sum(k) k\n" +
                    ",sum(l) l\n" +
                    ",sum(m) m\n" +
                    ",sum(n) n\n" +
                    ",sum(o) o\n" +
                    ",sum(p) p\n" +
                    ",sum(q) q\n" +
                    ",sum(r) r\n" +
                    ",sum(s) s\n" +
                    ",sum(t) t\n" +
                    ",sum(u) u\n" +
                    ",sum(v) v\n" +
                    ",sum(w) w\n" +
                    ",sum(x) x\n" +
                    "from (\n" +
                    "select\n" +
                    "  dt \n" +
                    " ,vehicle_id\n" +
                    " ,'6' as vehicle_type\n" +
                    " ,vehicleno\n" +
                    " ,tailnum\n" +
                    " ,limit_line_weekday\n" +
                    " ,limit_line_tailnum\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",limitLineStartIntervalNum\n" +
                    ",limitLineEndIntervalNum\n" +
                    ",limitLineCode1\n" +
                    ",limitLineCode2\n" +
                    ",max(a) a \n" +
                    ",max(b) b\n" +
                    ",max(c) c\n" +
                    ",max(d) d\n" +
                    ",max(e) e\n" +
                    ",max(f) f\n" +
                    ",max(g) g\n" +
                    ",max(h) h\n" +
                    ",max(i) i\n" +
                    ",max(j) j\n" +
                    ",max(k) k\n" +
                    ",max(l) l\n" +
                    ",max(m) m\n" +
                    ",max(n) n\n" +
                    ",max(o) o\n" +
                    ",max(p) p\n" +
                    ",max(q) q\n" +
                    ",max(r) r\n" +
                    ",max(s) s\n" +
                    ",max(t) t\n" +
                    ",max(u) u\n" +
                    ",max(v) v\n" +
                    ",max(w) w\n" +
                    ",max(x) x\n" +
                    "\n" +
                    "from (\n" +
                    "select \n" +
                    "dt \n" +
                    " ,vehicle_id\n" +
                    " ,vehicleno\n" +
                    " ,tailnum\n" +
                    " ,limit_line_weekday\n" +
                    " ,limit_line_tailnum\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",city_id\n" +
                    ",limitLineStartIntervalNum\n" +
                    ",limitLineEndIntervalNum\n" +
                    ",limitLineCode1\n" +
                    ",limitLineCode2\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 0 >= limitLineStartIntervalNum    and 0 <= limitLineEndIntervalNum  \n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END  a\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 1 >= (limitLineStartIntervalNum  )  and 1 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END b\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 2 >=  (limitLineStartIntervalNum )  and 2 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END c\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 3 >=  (limitLineStartIntervalNum  )  and 3 <= (limitLineEndIntervalNum ) \n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END d\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "case when 4 >=  (limitLineStartIntervalNum )  and 4 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END e\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "case when 5 >=  (limitLineStartIntervalNum  )  and 5 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END f\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "case when 6 >=  (limitLineStartIntervalNum )  and 6 <= (limitLineEndIntervalNum  ) \n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END g\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "case when 7 >=  (limitLineStartIntervalNum )  and 7 <= (limitLineEndIntervalNum  )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END h\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "case when 8 >=  (limitLineStartIntervalNum  )  and 8 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END i\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 9 >=  (limitLineStartIntervalNum  )  and 9 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END j\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "case when 10 >=  (limitLineStartIntervalNum )  and 10 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END k\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 11 >=  (limitLineStartIntervalNum  )  and 11 <= (limitLineEndIntervalNum ) \n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END end l\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "case when 12 >=  (limitLineStartIntervalNum  )  and 12 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END m\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "case when 13 >=  (limitLineStartIntervalNum  )  and 13 <= (limitLineEndIntervalNum  )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END n\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 14 >=  (limitLineStartIntervalNum )  and 14 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END o\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 15 >=  (limitLineStartIntervalNum )  and 15 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END p\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 16 >=  (limitLineStartIntervalNum  )  and 16 <= (limitLineEndIntervalNum  )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END q\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 17 >=  (limitLineStartIntervalNum  )  and 17 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END r\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 18 >=  (limitLineStartIntervalNum  )  and 18 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END s\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 19 >=  (limitLineStartIntervalNum  )  and 19 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END t\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 20 >=  (limitLineStartIntervalNum )  and 20 <= (limitLineEndIntervalNum ) \n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END u\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 21 >=  (limitLineStartIntervalNum  )  and 21 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END v\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 22 >=  (limitLineStartIntervalNum  )  and 22 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END w\n" +
                    "\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when 23 >=  (limitLineStartIntervalNum  )  and 23 <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN time_split_tag >= limitLineStartIntervalNum  AND time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END END x\n" +
                    "\n" +
                    "from vehicle_limited_detail vd\n" +
                    "\n" +
                    "where vd.dt >= '@sDate@'  and vd.dt <= '@eDate@'\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    " ) lim\n" +
                    "group by\n" +
                    "dt \n" +
                    " ,vehicle_id\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",limitLineStartIntervalNum\n" +
                    ",limitLineEndIntervalNum\n" +
                    ",limitLineCode1\n" +
                    ",limitLineCode2\n" +
                    ",city_id\n" +
                    ",vehicleno\n" +
                    ",tailnum\n" +
                    ",limit_line_weekday\n" +
                    ",limit_line_tailnum\n" +
                    "   ) limit_veh_sum\n" +
                    "group by \n" +
                    " dt\n" +
                    ",vehicle_type\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ") ,\n" +
                    "\n" +
                    "\n" +
                    "shared_occupy as(   ---分时运营任务占车\n" +
                    "\n" +
                    "select \n" +
                    "vehicle_id\n" +
                    ",dt\n" +
                    ",time_split_tag\n" +
                    ",sum(is_occupy)is_occupy\n" +
                    "from  (\n" +
                    "SELECT \n" +
                    "vehicle_id\n" +
                    ",start_time\n" +
                    ",s_time\n" +
                    ",end_time\n" +
                    ",e_time\n" +
                    ",create_time\n" +
                    ",ct_time\n" +
                    ",modify_time\n" +
                    ",md_time\n" +
                    ",dt\n" +
                    ",time_split_tag\n" +
                    ",cast(substring(s_time  , 12 ,2)  as int)\n" +
                    ",cast(substring(e_time  , 12 ,2) as int ) \n" +
                    ",cast(substring(ct_time , 12 ,2)  as int)\n" +
                    ",cast(substring(md_time  , 12 ,2) as int) \n" +
                    ",CASE WHEN cast(substring(s_time , 12 ,2)  as int) >= cast(substring(ct_time , 12 ,2)  as int)\n" +
                    "      AND (cast(substring(e_time , 12 ,2) as int ) <= cast(substring(md_time, 12 ,2) as int) ) \n" +
                    "       or ( cast(substring(e_time  , 12 ,2) as int ) - 1 = cast(substring(md_time  , 12 ,2) as int)  )\n" +
                    "      or e_time is null  \n" +
                    "THEN 1 ELSE 0 END is_occupy     -- 是否占车 \n" +
                    "FROM (\n" +
                    "select \n" +
                    "vh.id vehicle_id\n" +
                    ",unix_timestamp(CONCAT(vh.dt , td.time_name_start)) start_time\n" +
                    ",unix_timestamp(CONCAT(vh.dt , td.time_name_end)) end_time\n" +
                    ",unix_timestamp(vt.create_time ) create_time  --分时任务占车单创建时间\n" +
                    ",unix_timestamp(vt.modify_time ) modify_time --分时任务占车单结束时间\n" +
                    ",vt.create_time ct_time\n" +
                    ",vt.modify_time md_time\n" +
                    ",CONCAT(vh.dt , td.time_name_start)  s_time --车辆时段的开始时间\n" +
                    ",CONCAT(vh.dt , td.time_name_end)  e_time  --车辆时段的结束时间\n" +
                    ",vh.dt\n" +
                    ",vh.time_split_tag\n" +
                    "from \n" +
                    "bi_zuche.bas_vehicle_sync_by_hour vh\n" +
                    "LEFT JOIN\n" +
                    "(\n" +
                    "SELECT \n" +
                    "start_date\n" +
                    ",create_time\n" +
                    ",modify_time\n" +
                    ",vehicle_id\n" +
                    "FROM (\n" +
                    "SELECT  SUBSTRING(create_time  , 1 ,10) start_date ,create_time , modify_time , vehicle_id FROM \n" +
                    "bi_zuche.bas_sco_operation_task ) task WHERE start_date >= '@sDate@'  AND  start_date <= '@eDate@'\n" +
                    ") vt -- 分时车辆占车\n" +
                    "on vh.id = vt.vehicle_id\n" +
                    "AND vh.dt  = vt.start_date\n" +
                    "LEFT JOIN \n" +
                    "bi_zuche.bas_time_section_dict td \n" +
                    "on vh.time_split_tag = td.time_hour_id\n" +
                    "WHERE vh.dt>= '@sDate@'  and vh.dt <= '@eDate@' ) TT \n" +
                    ")share_occu\n" +
                    "group by \n" +
                    "vehicle_id\n" +
                    ",dt\n" +
                    ",time_split_tag\n" +
                    "),\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "ordercollect_his as (   ---48小时后无有效占车\n" +
                    "select \n" +
                    " min(start_time)start_time_after_48  --取每辆车的占车单的最小时间\n" +
                    ",vehicle_id\n" +
                    ",dt\n" +
                    "from \n" +
                    "bi_zuche.ordercollect_history\n" +
                    "where start_time  >= cast(concat('@sDate@' ,' 00:00:00')  as timestamp)  and start_time < cast(concat(date_add('@sDate@' , 3), ' 00:00:00') as timestamp)\n" +
                    "and vehicle_id is not null and state = 1  \n" +
                    "group by vehicle_id\n" +
                    ",dt\n" +
                    "),\n" +
                    "\n" +
                    "\n" +
                    "not_limited_occupy_effect as(   \n" +
                    "\n" +
                    "select \n" +
                    " dt\n" +
                    ",vehicle_id\n" +
                    ",vehicleno\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",city_id\n" +
                    ",time_split_tag\n" +
                    ",daynumofweek \n" +
                    ",tailNum\n" +
                    ",limit_line_weekday\n" +
                    ",limit_line_tailnum\n" +
                    ",limitLineCode1\n" +
                    ",limitLineCode2\n" +
                    ",is_limited\n" +
                    ",is_occupy\n" +
                    ",start_time_after_48\n" +
                    ",case when start_time_after_48 is null then 0 else \n" +
                    "\n" +
                    " case when abs(unix_timestamp(cast(concat(dt , time_name_start) as timestamp)) - unix_timestamp(start_time_after_48))   < 48 then 1 else 0 end\n" +
                    "\n" +
                    " end is_effect\n" +
                    ",abs(unix_timestamp(cast(concat(dt , time_name_start) as timestamp)) - unix_timestamp(start_time_after_48)) is_48hour \n" +
                    "from (\n" +
                    "\n" +
                    "select \n" +
                    " vd.dt\n" +
                    ",vd.vehicle_id\n" +
                    ",vehicleno\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",city_id\n" +
                    ",limit_line_start_time\n" +
                    ",limit_line_end_time \n" +
                    ",vd.time_split_tag\n" +
                    ",daynumofweek \n" +
                    ",tailNum\n" +
                    ",limit_line_weekday\n" +
                    ",limit_line_tailnum\n" +
                    "--,limitLineStartIntervalNum\n" +
                    "--,limitLineEndIntervalNum\n" +
                    ",limitLineCode1\n" +
                    ",limitLineCode2\n" +
                    ",time_name_start\n" +
                    ",time_name_end\n" +
                    ",CASE WHEN  limitLineStartIntervalNum is null OR limitLineEndIntervalNum is null THEN 0 ELSE\n" +
                    "\n" +
                    "case when vd.time_split_tag >=  (limitLineStartIntervalNum  )  and vd.time_split_tag <= (limitLineEndIntervalNum )\n" +
                    " \n" +
                    "     THEN  CASE WHEN  tailNum = limitLineCode1 OR  tailNum = limitLineCode2\n" +
                    "\n" +
                    "            THEN CASE WHEN vd.time_split_tag >= limitLineStartIntervalNum  AND vd.time_split_tag <= limitLineEndIntervalNum\n" +
                    "                 THEN 1 ELSE 0 END\n" +
                    "           ELSE 0 END \n" +
                    "ELSE 0 END end is_limited\n" +
                    ",oh.start_time_after_48\n" +
                    ",so.is_occupy\n" +
                    "from\n" +
                    "\n" +
                    "vehicle_limited_detail vd\n" +
                    "left join \n" +
                    "(select * from ordercollect_his) oh\n" +
                    "on vd.dt=oh.dt and vd.vehicle_id = oh.vehicle_id\n" +
                    "left join \n" +
                    "(select * from shared_occupy ) so \n" +
                    "on vd.dt = so.dt and vd.vehicle_id = so.vehicle_id and vd.time_split_tag = so.time_split_tag ) l_o_e\n" +
                    "\n" +
                    "),\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "vehicle_type_occupy as (\n" +
                    "select \n" +
                    " fulldate \n" +
                    ",vehicle_type\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",sum(a) a \n" +
                    ",sum(b) b\n" +
                    ",sum(c) c\n" +
                    ",sum(d) d\n" +
                    ",sum(e) e\n" +
                    ",sum(f) f\n" +
                    ",sum(g) g\n" +
                    ",sum(h) h\n" +
                    ",sum(i) i\n" +
                    ",sum(j) j\n" +
                    ",sum(k) k\n" +
                    ",sum(l) l\n" +
                    ",sum(m) m\n" +
                    ",sum(n) n\n" +
                    ",sum(o) o\n" +
                    ",sum(p) p\n" +
                    ",sum(q) q\n" +
                    ",sum(r) r\n" +
                    ",sum(s) s\n" +
                    ",sum(t) t\n" +
                    ",sum(u) u\n" +
                    ",sum(v) v\n" +
                    ",sum(w) w\n" +
                    ",sum(x) x\n" +
                    "from (\n" +
                    "select \n" +
                    " fulldate\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",vehicle_id\n" +
                    ",vehicle_type \n" +
                    ",max(a) a \n" +
                    ",max(b) b\n" +
                    ",max(c) c\n" +
                    ",max(d) d\n" +
                    ",max(e) e\n" +
                    ",max(f) f\n" +
                    ",max(g) g\n" +
                    ",max(h) h\n" +
                    ",max(i) i\n" +
                    ",max(j) j\n" +
                    ",max(k) k\n" +
                    ",max(l) l\n" +
                    ",max(m) m\n" +
                    ",max(n) n\n" +
                    ",max(o) o\n" +
                    ",max(p) p\n" +
                    ",max(q) q\n" +
                    ",max(r) r\n" +
                    ",max(s) s\n" +
                    ",max(t) t\n" +
                    ",max(u) u\n" +
                    ",max(v) v\n" +
                    ",max(w) w\n" +
                    ",max(x) x\n" +
                    "from \n" +
                    "(\n" +
                    "select \n" +
                    " fulldate\n" +
                    " ,vehicle_id\n" +
                    " ,vehicle_type\n" +
                    " ,start_time\n" +
                    " ,end_time\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (0+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 0  THEN  1 ELSE 0 END  \n" +
                    "           \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 0 AND end_time_tag > 0)\n" +
                    "                    OR (start_time_tag < 0 AND end_time_tag  < 0) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "\n" +
                    "     ELSE 0  END  END   AS a   --0-1\n" +
                    "     \n" +
                    " ,CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (1+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 1  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 1 AND end_time_tag > 1)\n" +
                    "                    OR (start_time_tag < 1 AND end_time_tag  < 1) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END   \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS b   --1-2\n" +
                    "     \n" +
                    "     \n" +
                    "     \n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (2+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 2  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 2 AND end_time_tag > 2)\n" +
                    "                    OR (start_time_tag < 2 AND end_time_tag  < 2) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END   \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS c  --2-3\n" +
                    "\n" +
                    " ,CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (3+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 3  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 3 AND end_time_tag > 3)\n" +
                    "                    OR (start_time_tag < 3 AND end_time_tag  < 3) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END   \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS d  --3-4\n" +
                    "\n" +
                    "\n" +
                    " ,CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (4+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 4  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 4 AND end_time_tag > 4)\n" +
                    "                    OR (start_time_tag < 4 AND end_time_tag  < 4) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END   \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS e  --4-5\n" +
                    "\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (5+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 5  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 5 AND end_time_tag > 5)\n" +
                    "                    OR (start_time_tag < 5 AND end_time_tag  < 5) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END   \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS f  -- --5-6\n" +
                    "\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (6+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 6  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 6 AND end_time_tag > 6)\n" +
                    "                    OR (start_time_tag < 6 AND end_time_tag  < 6) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END     --6-7\n" +
                    "        ELSE 0  END  END   AS g\n" +
                    "\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (7+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 7  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 7 AND end_time_tag > 7)\n" +
                    "                    OR (start_time_tag < 7 AND end_time_tag  < 7) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS h  --7-8\n" +
                    "\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (8+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 8  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 8 AND end_time_tag > 8)\n" +
                    "                    OR (start_time_tag < 8 AND end_time_tag  < 8) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS i  --8-9\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (9+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 9  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 9 AND end_time_tag > 9)\n" +
                    "                    OR (start_time_tag < 9 AND end_time_tag  < 9) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS j   --9-10\n" +
                    "\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (10+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 10  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 10 AND end_time_tag > 10)\n" +
                    "                    OR (start_time_tag < 10 AND end_time_tag  < 10) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS k   --10-11\n" +
                    "\n" +
                    "\n" +
                    "  ,CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (11+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 11  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 11 AND end_time_tag > 11)\n" +
                    "                    OR (start_time_tag < 11 AND end_time_tag  < 11) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS l   --11-12\n" +
                    "  \n" +
                    "  ,CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (12+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 12  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 12 AND end_time_tag > 12)\n" +
                    "                    OR (start_time_tag < 12 AND end_time_tag  < 12) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "    ELSE 0  END  END   AS m   --12-13\n" +
                    "\n" +
                    " ,CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (12+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 13  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 13 AND end_time_tag > 13)\n" +
                    "                    OR (start_time_tag < 13 AND end_time_tag  < 13) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS n   --13-14\n" +
                    "\n" +
                    " ,CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (13+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 14  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 14 AND end_time_tag > 14)\n" +
                    "                    OR (start_time_tag < 14 AND end_time_tag  < 14) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS o  --14-15\n" +
                    "\n" +
                    "\n" +
                    " ,CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (14+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 15  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 15 AND end_time_tag > 15)\n" +
                    "                    OR (start_time_tag < 15 AND end_time_tag  < 15) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS p  --15-16\n" +
                    "\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (15+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 16  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 16 AND end_time_tag > 16)\n" +
                    "                    OR (start_time_tag < 16 AND end_time_tag  < 16) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS q  --16-17\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (16+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 17  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 17 AND end_time_tag > 17)\n" +
                    "                    OR (start_time_tag < 17 AND end_time_tag  < 17) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS r  --17-18\n" +
                    "\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (17+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 18  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 18 AND end_time_tag > 18)\n" +
                    "                    OR (start_time_tag < 18 AND end_time_tag  < 18) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS s  --18-19\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (18+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 19  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 19 AND end_time_tag > 19)\n" +
                    "                    OR (start_time_tag < 19 AND end_time_tag  < 19) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS t  --19-20\n" +
                    "\n" +
                    "\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (19+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 20  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 20 AND end_time_tag > 20)\n" +
                    "                    OR (start_time_tag < 20 AND end_time_tag  < 20) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS u  --20-21\n" +
                    "\n" +
                    "  ,CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (20+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 21  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 21 AND end_time_tag > 21)\n" +
                    "                    OR (start_time_tag < 21 AND end_time_tag  < 21) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS v  --21-22\n" +
                    "\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (21+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 22  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "       WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "          THEN CASE when (start_time_tag > 22 AND end_time_tag > 22)\n" +
                    "                    OR (start_time_tag < 22 AND end_time_tag  < 22) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS w  --22-23\n" +
                    "\n" +
                    ",CASE WHEN vs.end_time IS NULL THEN 1 ELSE \n" +
                    "   CASE WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       >= date_tommo then 1\n" +
                    "      \n" +
                    "       \n" +
                    "     WHEN   unix_start_time >= date_today\n" +
                    "            AND unix_start_time  < date_tommo\n" +
                    "            AND unix_end_time >= date_tommo  \n" +
                    "       \n" +
                    "            THEN   CASE WHEN (start_time_tag + 1 ) <= (21+1)  THEN  1 ELSE 0 END    \n" +
                    "            \n" +
                    "       WHEN   unix_start_time < date_today AND unix_end_time \n" +
                    "       < date_tommo  \n" +
                    "          AND  unix_end_time >= date_today\n" +
                    "\n" +
                    "       \n" +
                    "           THEN    CASE WHEN (end_time_tag  ) >= 23  THEN  1 ELSE 0 END  \n" +
                    "               \n" +
                    "        WHEN   substring(vs.start_time , 0 ,10) = vs.fulldate AND substring(vs.end_time  , 0 ,10) = vs.fulldate\n" +
                    "\n" +
                    "          THEN CASE when (start_time_tag > 23 AND end_time_tag > 23)\n" +
                    "                    OR (start_time_tag < 23 AND end_time_tag  < 23) \n" +
                    "                    THEN 0\n" +
                    "           ELSE 1 END \n" +
                    "           \n" +
                    "     ELSE 0  END  END   AS x  --23-24\n" +
                    ",vs.self_status_1st  \n" +
                    ",vs.is_shared\n" +
                    ",ordercar\n" +
                    "from (\n" +
                    "select \n" +
                    " vehicle_id\n" +
                    ",fulldate\n" +
                    ",vehicle_type\n" +
                    ",start_time  -- 占车单开始时间\n" +
                    ",end_time     -- 占车单结束时间\n" +
                    ",unix_timestamp(v.start_time) unix_start_time\n" +
                    ",unix_timestamp(v.end_time) unix_end_time\n" +
                    ",unix_timestamp(date_add(to_date(CONCAT(v.fulldate , ' 00:00:00')),1) )  date_tommo\n" +
                    ",unix_timestamp(CONCAT(v.fulldate , ' 00:00:00'))  date_today\n" +
                    ",cast(substring(v.start_time , 12 , 2) as int)  as start_time_tag\n" +
                    ",cast(substring(v.end_time , 12 , 2) as int) as end_time_tag\n" +
                    ",vsh.self_status_1st  \n" +
                    ",vsh.is_shared \n" +
                    ",vsh.ordercar\n" +
                    ",vsh.department_id \n" +
                    ",vsh.nowdepartment_id\n" +
                    ",vsh.park_city_id\n" +
                    ",vsh.vehicleno_city_id\n" +
                    "FROM   \n" +
                    "bi_zuche.bas_vehicle_section_detail v\n" +
                    "inner JOIN bi_zuche.bas_vehicle_sync_by_hour vsh\n" +
                    "ON v.vehicle_id = vsh.id AND v.dt = vsh.dt \n" +
                    "WHERE v.dt>='@sDate@' AND v.dt <= '@eDate@'\n" +
                    "AND vsh.dt>='@sDate@' AND vsh.dt <='@eDate@'\n" +
                    ") vs where self_status_1st = '200000' and is_shared = 1  and ordercar = 0\n" +
                    ") part1\n" +
                    "group by \n" +
                    " fulldate\n" +
                    " ,vehicle_id\n" +
                    " ,vehicle_type\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id ) veh_type_sum group by \n" +
                    " fulldate\n" +
                    " ,vehicle_type\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    "\n" +
                    "),\n" +
                    "\n" +
                    "all_vehicle_sum as (\n" +
                    "select \n" +
                    " dt\n" +
                    ",vehicle_type\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",sum(a) a \n" +
                    ",sum(b) b\n" +
                    ",sum(c) c\n" +
                    ",sum(d) d\n" +
                    ",sum(e) e\n" +
                    ",sum(f) f\n" +
                    ",sum(g) g\n" +
                    ",sum(h) h\n" +
                    ",sum(i) i\n" +
                    ",sum(j) j\n" +
                    ",sum(k) k\n" +
                    ",sum(l) l\n" +
                    ",sum(m) m\n" +
                    ",sum(n) n\n" +
                    ",sum(o) o\n" +
                    ",sum(p) p\n" +
                    ",sum(q) q\n" +
                    ",sum(r) r\n" +
                    ",sum(s) s\n" +
                    ",sum(t) t\n" +
                    ",sum(u) u\n" +
                    ",sum(v) v\n" +
                    ",sum(w) w\n" +
                    ",sum(x) x\n" +
                    "from (\n" +
                    "select \n" +
                    "dt\n" +
                    ",'0' as vehicle_type\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",case time_split_tag when 0  then vel_num else 0  end a\n" +
                    ",case time_split_tag when 1  then vel_num else 0  end b\n" +
                    ",case time_split_tag when 2  then vel_num else 0  end c\n" +
                    ",case time_split_tag when 3  then vel_num else 0  end d\n" +
                    ",case time_split_tag when 4  then vel_num else 0  end e\n" +
                    ",case time_split_tag when 5  then vel_num else 0  end f\n" +
                    ",case time_split_tag when 6  then vel_num else 0  end g\n" +
                    ",case time_split_tag when 7  then vel_num else 0  end h\n" +
                    ",case time_split_tag when 8  then vel_num else 0  end i\n" +
                    ",case time_split_tag when 9  then vel_num else 0  end j\n" +
                    ",case time_split_tag when 10  then vel_num else 0  end k\n" +
                    ",case time_split_tag when 11  then vel_num else 0  end l\n" +
                    ",case time_split_tag when 12  then vel_num else 0  end m\n" +
                    ",case time_split_tag when 13  then vel_num else 0  end n\n" +
                    ",case time_split_tag when 14  then vel_num else 0  end o\n" +
                    ",case time_split_tag when 15  then vel_num else 0  end p\n" +
                    ",case time_split_tag when 16  then vel_num else 0  end q\n" +
                    ",case time_split_tag when 17  then vel_num else 0  end r\n" +
                    ",case time_split_tag when 18  then vel_num else 0  end s\n" +
                    ",case time_split_tag when 19  then vel_num else 0  end t\n" +
                    ",case time_split_tag when 20  then vel_num else 0  end u\n" +
                    ",case time_split_tag when 21  then vel_num else 0  end v\n" +
                    ",case time_split_tag when 22  then vel_num else 0  end w\n" +
                    ",case time_split_tag when 23  then vel_num else 0  end x\n" +
                    "from (\n" +
                    "\n" +
                    "select \n" +
                    "dt\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",time_split_tag\n" +
                    ",count(id) vel_num\n" +
                    "FROM bi_zuche.bas_vehicle_sync_by_hour\n" +
                    "where self_status_1st = '200000' and is_shared = 1  and ordercar = 0\n" +
                    "and  dt >= '@sDate@' and dt <='@eDate@' \n" +
                    "group by \n" +
                    "dt\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",time_split_tag )  all_vel ) all_vel_sum group by \n" +
                    " dt\n" +
                    ",vehicle_type\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id ),\n" +
                    "\n" +
                    "total_all as (\n" +
                    "\n" +
                    "--全部占车单统计\n" +
                    "select * from vehicle_type_occupy   \n" +
                    "\n" +
                    "union all\n" +
                    "\n" +
                    "--可用车辆统计\n" +
                    "select \n" +
                    " dt\n" +
                    ",vehicle_type\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",sum(a) a \n" +
                    ",sum(b) b\n" +
                    ",sum(c) c\n" +
                    ",sum(d) d\n" +
                    ",sum(e) e\n" +
                    ",sum(f) f\n" +
                    ",sum(g) g\n" +
                    ",sum(h) h\n" +
                    ",sum(i) i\n" +
                    ",sum(j) j\n" +
                    ",sum(k) k\n" +
                    ",sum(l) l\n" +
                    ",sum(m) m\n" +
                    ",sum(n) n\n" +
                    ",sum(o) o\n" +
                    ",sum(p) p\n" +
                    ",sum(q) q\n" +
                    ",sum(r) r\n" +
                    ",sum(s) s\n" +
                    ",sum(t) t\n" +
                    ",sum(u) u\n" +
                    ",sum(v) v\n" +
                    ",sum(w) w\n" +
                    ",sum(x) x\n" +
                    "\n" +
                    "from (\n" +
                    "select \n" +
                    "\n" +
                    "dt\n" +
                    ",department_id \n" +
                    ",'7' as vehicle_type\n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",city_id\n" +
                    ",vehicle_id\n" +
                    ",case time_split_tag when 0  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as a  \n" +
                    "\n" +
                    ",case time_split_tag when 1  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as b  \n" +
                    "\n" +
                    ",case time_split_tag when 2  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as c  \n" +
                    "\n" +
                    ",case time_split_tag when 3  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as d  \n" +
                    "\n" +
                    ",case time_split_tag when 4  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as e  \n" +
                    "\n" +
                    ",case time_split_tag when 5  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as f  \n" +
                    "\n" +
                    ",case time_split_tag when 6  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as g  \n" +
                    "\n" +
                    ",case time_split_tag when 7  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as h  \n" +
                    "\n" +
                    ",case time_split_tag when 8  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as i  \n" +
                    "\n" +
                    ",case time_split_tag when 9  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as j  \n" +
                    "\n" +
                    ",case time_split_tag when 10  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as k  \n" +
                    "\n" +
                    ",case time_split_tag when 11  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as l  \n" +
                    "\n" +
                    ",case time_split_tag when 12  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as m  \n" +
                    "\n" +
                    ",case time_split_tag when 13  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as n  \n" +
                    "\n" +
                    "\n" +
                    ",case time_split_tag when 14  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as o  \n" +
                    "\n" +
                    "\n" +
                    ",case time_split_tag when 15  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as p  \n" +
                    "\n" +
                    "\n" +
                    ",case time_split_tag when 16  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as q  \n" +
                    "\n" +
                    "\n" +
                    ",case time_split_tag when 17  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as r  \n" +
                    "\n" +
                    "\n" +
                    ",case time_split_tag when 18  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as s  \n" +
                    "\n" +
                    "\n" +
                    ",case time_split_tag when 19  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as t  \n" +
                    "\n" +
                    "\n" +
                    ",case time_split_tag when 20  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as u  \n" +
                    "\n" +
                    "\n" +
                    ",case time_split_tag when 21  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as v  \n" +
                    "\n" +
                    "\n" +
                    ",case time_split_tag when 22  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as w  \n" +
                    "\n" +
                    "\n" +
                    ",case time_split_tag when 23  then\n" +
                    "\n" +
                    "case when is_occupy >0 or is_limited >0 or is_effect > 0 then  0 else 1 end end as x  \n" +
                    "from (\n" +
                    "select \n" +
                    " dt\n" +
                    ",vehicle_id\n" +
                    ",vehicleno\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",city_id\n" +
                    "\n" +
                    ",time_split_tag\n" +
                    ",daynumofweek \n" +
                    ",tailNum\n" +
                    ",limit_line_weekday\n" +
                    ",limit_line_tailnum\n" +
                    "\n" +
                    ",limitLineCode1\n" +
                    ",limitLineCode2\n" +
                    ",sum(is_occupy) is_occupy \n" +
                    ",sum(is_limited) is_limited\n" +
                    ",sum(is_effect)is_effect\n" +
                    "from not_limited_occupy_effect \n" +
                    "\n" +
                    "group by \n" +
                    "dt\n" +
                    ",vehicle_id\n" +
                    ",vehicleno\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",city_id\n" +
                    "\n" +
                    ",time_split_tag\n" +
                    ",daynumofweek \n" +
                    ",tailNum\n" +
                    ",limit_line_weekday\n" +
                    ",limit_line_tailnum\n" +
                    "\n" +
                    ",limitLineCode1\n" +
                    ",limitLineCode2 ) total_veh ) cal_total group by\n" +
                    "dt\n" +
                    ",department_id \n" +
                    ",nowdepartment_id\n" +
                    ",park_city_id\n" +
                    ",vehicleno_city_id\n" +
                    ",vehicle_type\n" +
                    "\n" +
                    "union all\n" +
                    "\n" +
                    "\n" +
                    "select * from vehicle_limited_sum \n" +
                    "\n" +
                    "union all\n" +
                    "\n" +
                    "\n" +
                    "select * from all_vehicle_sum\n" +
                    "\n" +
                    ") \n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "-- fe.status = 1          --状态为:有效\n" +
                    "-- AND fe.scope_type in (1,2) --适用范围:0 :'自助' , 1:'分时',2:'全支持'  \n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "SELECT \n" +
                    "fulldate\n" +
                    ",department_name\n" +
                    ",CASE vehicle_type WHEN '0' THEN '分时车辆/辆'\n" +
                    "                   WHEN '1' THEN '已停运/辆'\n" +
                    "                   WHEN '2' THEN '短租已租赁/辆'\n" +
                    "                   WHEN '3' THEN '分时已租赁/辆'\n" +
                    "                   WHEN '4' THEN '短租已排车/辆'\n" +
                    "                   WHEN '5' THEN '运营占车/辆'\n" +
                    "                   WHEN '6' THEN '限行/辆'\n" +
                    "                   WHEN '7' THEN '可用车辆合计'\n" +
                    "                   else null END v_type\n" +
                    ",sum(a) a \n" +
                    ",sum(b) b\n" +
                    ",sum(c) c\n" +
                    ",sum(d) d\n" +
                    ",sum(e) e\n" +
                    ",sum(f) f\n" +
                    ",sum(g) g\n" +
                    ",sum(h) h\n" +
                    ",sum(i) i\n" +
                    ",sum(j) j\n" +
                    ",sum(k) k\n" +
                    ",sum(l) l\n" +
                    ",sum(m) m\n" +
                    ",sum(n) n\n" +
                    ",sum(o) o\n" +
                    ",sum(p) p\n" +
                    ",sum(q) q\n" +
                    ",sum(r) r\n" +
                    ",sum(s) s\n" +
                    ",sum(t) t\n" +
                    ",sum(u) u\n" +
                    ",sum(v) v\n" +
                    ",sum(w) w\n" +
                    ",sum(x) x\n" +
                    "FROM (\n" +
                    "select \n" +
                    " fulldate\n" +
                    ",vehicle_type\n" +
                    ",CASE @type@   WHEN -1 THEN park_city_id\n" +
                    "      WHEN -2 THEN nowdepartment_id\n" +
                    "      WHEN -3 THEN department_id \n" +
                    " ELSE NULL END department_id \n" +
                    ",CASE @type@   WHEN -1 THEN city_name\n" +
                    "      WHEN -2 THEN dept_now_name\n" +
                    "      WHEN -3 THEN dept_name \n" +
                    " ELSE NULL END department_name \n" +
                    ",is_scar --是否支付分时,0不支持,1支持\n" +
                    ",city_flag  --是否开业(1:已开业,2:未开业,3:待开业,4:待停业,5:已停业)\n" +
                    ",dept_flag  --  0未开业,1已开业,2已停业,3待开业,4待停业\n" +
                    ",dept_now_flag\n" +
                    ",dept_srms_scope_type\n" +
                    ",dept_srms_status\n" +
                    ",dept_srms_now_scope_type\n" +
                    ",dept_srms_now_status\n" +
                    ",a \n" +
                    ",b\n" +
                    ",c\n" +
                    ",d\n" +
                    ",e\n" +
                    ",f\n" +
                    ",g\n" +
                    ",h\n" +
                    ",i\n" +
                    ",j\n" +
                    ",k\n" +
                    ",l\n" +
                    ",m\n" +
                    ",n\n" +
                    ",o\n" +
                    ",p\n" +
                    ",q\n" +
                    ",r\n" +
                    ",s\n" +
                    ",t\n" +
                    ",u\n" +
                    ",v\n" +
                    ",w\n" +
                    ",x\n" +
                    "from  (\n" +
                    "select \n" +
                    " fulldate\n" +
                    ",vehicle_type\n" +
                    ",ta.department_id \n" +
                    ",dept.department_name  as dept_name\n" +
                    ",nowdepartment_id\n" +
                    ",dept_now.department_name dept_now_name\n" +
                    ",park_city_id\n" +
                    ",city.name as city_name\n" +
                    ",city.is_scar --是否支付分时,0不支持,1支持\n" +
                    ",city.flag  city_flag--是否开业(1:已开业,2:未开业,3:待开业,4:待停业,5:已停业)\n" +
                    ",dept.flag  dept_flag--  0未开业,1已开业,2已停业,3待开业,4待停业\n" +
                    ",dept_now.flag dept_now_flag\n" +
                    ",dept_srms.scope_type dept_srms_scope_type\n" +
                    ",dept_srms.status  dept_srms_status \n" +
                    ",dept_srms_now.scope_type dept_srms_now_scope_type\n" +
                    ",dept_srms_now.status  dept_srms_now_status \n" +
                    ",a \n" +
                    ",b\n" +
                    ",c\n" +
                    ",d\n" +
                    ",e\n" +
                    ",f\n" +
                    ",g\n" +
                    ",h\n" +
                    ",i\n" +
                    ",j\n" +
                    ",k\n" +
                    ",l\n" +
                    ",m\n" +
                    ",n\n" +
                    ",o\n" +
                    ",p\n" +
                    ",q\n" +
                    ",r\n" +
                    ",s\n" +
                    ",t\n" +
                    ",u\n" +
                    ",v\n" +
                    ",w\n" +
                    ",x\n" +
                    "from total_all  ta\n" +
                    "LEFT JOIN bi_zuche.bas_city city\n" +
                    "on ta.park_city_id = city.id\n" +
                    "LEFT JOIN bi_zuche.bas_department_info dept\n" +
                    "ON ta.department_id = dept.id\n" +
                    "LEFT JOIN bi_zuche.bas_department_info dept_now\n" +
                    "ON ta.nowdepartment_id = dept_now.id\n" +
                    "LEFT JOIN (select * from zuche.t_srms_department_fence where scope_type in (1,2)) dept_srms\n" +
                    "ON ta.department_id = dept_srms.dept_id \n" +
                    "LEFT JOIN ( select * from zuche.t_srms_department_fence where scope_type in (1,2)) dept_srms_now\n" +
                    "ON ta.nowdepartment_id = dept_srms_now.dept_id ) all_veh \n" +
                    "where 1=1\n" +
                    "AND case  @type@    when  -1  THEN is_scar = 1 and city_flag in (1,4)\n" +
                    "                   when -2  THEN  dept_now_flag in (1,4) and dept_srms_now_scope_type in (1,2) AND dept_srms_status = 1\n" +
                    "                   when -3  THEN dept_flag IN (1,4) and dept_srms_scope_type in (1,2) and dept_srms_now_status = 1\n" +
                    "                  else 1=1 end   \n" +
                    ")sum_veh \n" +
                    "group by \n" +
                    "CASE vehicle_type WHEN  '0' THEN '分时车辆/辆'\n" +
                    "                   WHEN '1' THEN '已停运/辆'\n" +
                    "                   WHEN '2' THEN '短租已租赁/辆'\n" +
                    "                   WHEN '3' THEN '分时已租赁/辆'\n" +
                    "                   WHEN '4' THEN '短租已排车/辆'\n" +
                    "                   WHEN '5' THEN '运营占车/辆'\n" +
                    "                   WHEN '6' THEN '限行/辆'\n" +
                    "                   WHEN '7' THEN '可用车辆合计'\n" +
                    "                   else null END \n" +
                    ",fulldate\n" +
                    ",department_name\n" +
                    "order by department_name\n" +
                    " \n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n";
            sql = sql.replaceAll("@", "");


            SemanticAnalyzerEX.Result result = SemanticAnalyzerEX.getBloodRelationShipFromSQL(sql);
            SemanticAnalyzerEX.printResultInfo(result, true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
