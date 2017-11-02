package com.jyc.presto.jdbc;

import java.sql.*;

/**
 * Created by jyc on 2017/11/2.
 */
public class PrestoJdbcTest {


    public static void main(String[] args) throws SQLException {
        //连接字符串中的hive是catalog名字，sys是schema名字，ddd是用户名，这个用户名根据实际业务自己设定，用来标示执行sql的用户，但是不会通过该用户名进行身份认证，但是必须要写。密码直接指定为null
        // String connection = (PrestoConnection) DriverManager.getConnection("jdbc:presto://Coordinator IP地址:Coordinator端口号/hive/sys","ddd",null);

        String test_url = "jdbc:presto://10.104.102.184:8888/hive1/default";
        String prod_url = "jdbc:presto://10.204.11.166:8080/hive2/default";
        Connection connection = DriverManager.getConnection(prod_url, "yuchen.jia",null);
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from hive1.default.t_scd_order limit 10");
        while (rs.next()) {
            System.out.println(String.format("%s\t%s\t%s",rs.getString(1),rs.getString(2),rs.getString(3)));
        }
        rs.close();
        connection.close();
    }

}
