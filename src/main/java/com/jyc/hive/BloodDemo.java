package com.jyc.hive;


import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.*;

import java.io.IOException;

/**
 * Created by jyc on 2018/7/13.
 */
public class BloodDemo {

    public static void main(String[] args) {
        try {
//            HiveConf conf = new HiveConf();
//            SemanticAnalyzerEX.getBloodRelationShipFromSQL(SQLConstans.SQL_01);
            test01();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void test01() throws ParseException, IOException {
        HiveConf conf = new HiveConf();
        conf.set("_hive.local.session.path", "/tmp/local");
        conf.set("_hive.hdfs.session.path", "/tmp/hdfs");
        Context ctx = new Context(conf);
        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(SQLConstans.SQL_10, ctx);
        String s  = tree.toStringTree();
        System.out.println(s);
    }
}
