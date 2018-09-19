package com.jyc.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author jyc
 * @date 2018/5/21
 */
public class HdfsDemo {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.104.90.45:9000");
//        conf.set("fs.defaultFS", "hdfs://114.244.214.223:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/jyc/测试/test");
//        Path path = new Path("/hadoop-env.sh");
        System.out.println(fs.exists(path));

        // read file
        InputStream in = null;
        try {
            in = fs.open(path);
//            IOUtils.copyBytes(in, System.out, 4096, false);
            String content = org.apache.commons.io.IOUtils.toString(in, "utf-8");
            System.out.println(content);
        } finally {
            IOUtils.closeStream(in);
            fs.close();
        }



    }

}
