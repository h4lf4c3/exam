package com.hjj.common;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


public class hdfsCtrl {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://192.168.160.30:9000");

        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path("F://Code//国内疫情数据.csv"),new Path("/inputdata/"));
        System.out.print("上传完毕");

    }
}
