package com.myself.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/28
 * mr程序读取hbase中的数据并写入到hbase中的另一张表中
 */
public class DriverMain  {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //hbaseconfiguration集成了hadoop和hbase的config
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop128,hadoop129,hadoop130");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("family","baseInfor");
        conf.set("column","name");

        Job job = Job.getInstance(conf);
        job.setJarByClass(DriverMain.class);

        //与hadoop的mr对比，由此关联hbase的inputformat和outputformat
        TableMapReduceUtil.initTableMapperJob(
                "student",
                new Scan(),
                MrHbaseMapper.class,
                NullWritable.class,
                Put.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                "student2",
                MrHbaseReduce.class,
                job
        );

        job.waitForCompletion(true);
    }
}
